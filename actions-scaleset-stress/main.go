package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const userAgent = "ghes-traffic-simulator/actions-scaleset-stress"

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "actions-scaleset-stress:", safeError(err))
		os.Exit(1)
	}
}

func execute(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(os.Stdout)
		return nil
	}

	switch args[0] {
	case "guard":
		return commandGuard(args[1:])
	case "version":
		fmt.Println("actions-scaleset-stress dev")
		return nil
	case "check":
		return commandCheck(args[1:])
	case "prepare":
		return commandPrepare(args[1:])
	case "run":
		return commandRun(args[1:])
	case "cleanup":
		return commandCleanup(args[1:])
	default:
		return fmt.Errorf("unknown command %q (run help)", args[0])
	}
}

func printUsage(w *os.File) {
	fmt.Fprint(w, `actions-scaleset-stress: bounded GitHub Actions load against real GHES

Usage:
  actions-scaleset-stress check   --config <file>
  actions-scaleset-stress prepare --config <file>
  actions-scaleset-stress run     --config <file> --mode registrations|workflows [limits]
  actions-scaleset-stress cleanup --config <file> --run-id <id> [--apply]

The GHES token is read from the environment variable named by token_env in
the non-secret JSON config (default: GHES_ACTIONS_TOKEN). No command starts
load unless its hard duration and work limits are positive and bounded.
`)
}

type commonOptions struct {
	configPath string
	caCert     string
	insecure   bool
}

func addCommonFlags(fs *flag.FlagSet) *commonOptions {
	o := &commonOptions{}
	fs.StringVar(&o.configPath, "config", "actions-scaleset-stress.json", "non-secret target configuration")
	fs.StringVar(&o.caCert, "ca-cert", "", "PEM CA bundle for the GHES TLS certificate")
	fs.BoolVar(&o.insecure, "insecure", false, "disable TLS verification (lab use only)")
	return o
}

func commandCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	opts := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, token, err := loadConfig(opts.configPath)
	if err != nil {
		return err
	}
	target, err := newTarget(cfg, token, opts.caCert, opts.insecure)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.check(ctx); err != nil {
		return err
	}
	fmt.Printf("GHES reachable; authenticated as %s; target %s/%s: %s\n",
		target.identity, cfg.Owner, cfg.Repo, target.repoState)
	return nil
}

func commandPrepare(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ContinueOnError)
	opts := addCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, token, err := loadConfig(opts.configPath)
	if err != nil {
		return err
	}
	target, err := newTarget(cfg, token, opts.caCert, opts.insecure)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := target.prepare(ctx); err != nil {
		return err
	}
	fmt.Printf("Private test repository and workflow are ready: %s/%s (%s)\n",
		cfg.Owner, cfg.Repo, cfg.WorkflowFile)
	return nil
}

type runOptions struct {
	commonOptions
	mode               string
	runID              string
	duration           time.Duration
	total              int
	rate               float64
	maxInflight        int
	dwell              time.Duration
	maxRunners         int
	jobsPerRun         int
	jobDuration        time.Duration
	failPercent        int
	maxLogBytes        int
	maxOutstandingJobs int
	runnerMemoryMiB    int64
	runnerCPUs         float64
	drainTimeout       time.Duration
	artifactsDir       string
}

func commandRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	o := &runOptions{}
	common := addCommonFlags(fs)
	fs.StringVar(&o.mode, "mode", "", "required profile: registrations or workflows")
	fs.StringVar(&o.runID, "run-id", "", "run identifier (default: generated)")
	fs.DurationVar(&o.duration, "duration", time.Minute, "hard maximum run duration")
	fs.IntVar(&o.total, "total", 10, "maximum registrations or workflow dispatches")
	fs.Float64Var(&o.rate, "rate", 1, "maximum new registrations or dispatches per second")
	fs.IntVar(&o.maxInflight, "max-inflight", 5, "maximum concurrent registration operations")
	fs.DurationVar(&o.dwell, "dwell", 5*time.Second, "registered runner dwell before removal")
	fs.IntVar(&o.maxRunners, "max-runners", 4, "maximum concurrent official runner containers")
	fs.IntVar(&o.jobsPerRun, "jobs-per-run", 1, "matrix jobs in each dispatched workflow")
	fs.DurationVar(&o.jobDuration, "job-duration", time.Second, "bounded synthetic job duration")
	fs.IntVar(&o.failPercent, "fail-percent", 0, "deterministic intentional job failure percentage")
	fs.IntVar(&o.maxLogBytes, "max-log-bytes", 1024, "maximum synthetic log bytes per job")
	fs.IntVar(&o.maxOutstandingJobs, "max-outstanding-jobs", 20, "maximum generated queued plus running jobs")
	fs.Int64Var(&o.runnerMemoryMiB, "runner-memory-mib", 2048, "hard memory limit per runner container")
	fs.Float64Var(&o.runnerCPUs, "runner-cpus", 1, "CPU limit per runner container")
	fs.DurationVar(&o.drainTimeout, "drain-timeout", 30*time.Second, "maximum graceful workflow drain")
	fs.StringVar(&o.artifactsDir, "artifacts-dir", "tmp/actions-scaleset-stress", "private run state and report directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	o.commonOptions = *common
	if o.mode != "registrations" && o.mode != "workflows" {
		return errors.New("--mode must be registrations or workflows")
	}
	if err := o.validate(); err != nil {
		return err
	}
	cfg, token, err := loadConfig(o.configPath)
	if err != nil {
		return err
	}
	target, err := newTarget(cfg, token, o.caCert, o.insecure)
	if err != nil {
		return err
	}
	if o.mode == "workflows" && cfg.RunnerImage == "" {
		return errors.New("runner_image must be a pinned digest reference for workflow mode")
	}
	if o.mode == "workflows" && o.total > 100 {
		return errors.New("workflow mode is limited to 100 dispatches per run")
	}
	if o.runID == "" {
		o.runID, err = newRunID()
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()
	if err := target.check(ctx); err != nil {
		return err
	}
	if target.repoState != "private repository, Actions enabled, workflow verified" {
		return fmt.Errorf("run requires a prepared private repository with an active workflow; current target state: %s", target.repoState)
	}
	state, err := createRunState(o.artifactsDir, o.runID, o.mode, target)
	if err != nil {
		return err
	}
	defer state.Close()
	fmt.Printf("Run %s started (mode=%s, duration=%s, total=%d)\n", o.runID, o.mode, o.duration, o.total)
	var guard *guardProcess
	if o.mode == "workflows" {
		guard, err = startGuard(state)
		if err != nil {
			_ = state.record("guard_start_failed", map[string]any{"error": err.Error()})
			return err
		}
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan error, 1)
	go state.heartbeat(heartbeatCtx, heartbeatDone)
	workDone := make(chan error, 1)
	go func() { workDone <- runWorkload(ctx, target, state, *o) }()
	var workErr error
	select {
	case workErr = <-workDone:
	case heartbeatErr, ok := <-heartbeatDone:
		if ok && heartbeatErr != nil {
			cancel()
			workErr = errors.Join(heartbeatErr, <-workDone)
		} else {
			workErr = <-workDone
		}
	}
	stopHeartbeat()
	if heartbeatErr, ok := <-heartbeatDone; ok && heartbeatErr != nil {
		workErr = errors.Join(workErr, heartbeatErr)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	cleanupErr := cleanupRun(cleanupCtx, target, state)
	cleanupCancel()
	if err := state.update(func(data *runData) {
		if cleanupErr != nil {
			data.CleanupStatus = "incomplete"
		} else {
			data.CleanupStatus = "complete"
		}
	}); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	if guard != nil {
		if err := guard.stop(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if ctx.Err() != nil {
		_ = state.record("run_stopped", map[string]any{"reason": ctx.Err().Error()})
	}
	if workErr != nil {
		_ = state.update(func(data *runData) {
			data.Status = "failed"
			data.LastError = workErr.Error()
		})
	} else if cleanupErr != nil {
		_ = state.update(func(data *runData) {
			data.Status = "cleanup-incomplete"
			data.LastError = cleanupErr.Error()
		})
	} else {
		_ = state.update(func(data *runData) { data.Status = "complete" })
	}
	reportErr := state.writeReport()
	if workErr != nil || cleanupErr != nil || reportErr != nil {
		return errors.Join(workErr, cleanupErr, reportErr)
	}
	fmt.Printf("Run %s completed; report: %s\n", o.runID, state.reportPath())
	return nil
}

func (o runOptions) validate() error {
	switch {
	case o.duration <= 0 || o.duration > 24*time.Hour:
		return errors.New("--duration must be between 1ns and 24h")
	case o.total <= 0 || o.total > 100000:
		return errors.New("--total must be between 1 and 100000")
	case o.rate <= 0 || o.rate > 100:
		return errors.New("--rate must be > 0 and <= 100 operations per second")
	case o.maxInflight <= 0 || o.maxInflight > 100:
		return errors.New("--max-inflight must be between 1 and 100")
	case o.dwell < 0 || o.dwell > time.Hour:
		return errors.New("--dwell must be between 0 and 1h")
	case o.maxRunners <= 0 || o.maxRunners > 100:
		return errors.New("--max-runners must be between 1 and 100")
	case o.jobsPerRun <= 0 || o.jobsPerRun > 25:
		return errors.New("--jobs-per-run must be between 1 and 25")
	case o.jobDuration < 0 || o.jobDuration > 10*time.Minute:
		return errors.New("--job-duration must be between 0 and 10m")
	case o.failPercent < 0 || o.failPercent > 100:
		return errors.New("--fail-percent must be between 0 and 100")
	case o.maxLogBytes < 0 || o.maxLogBytes > 1024*1024:
		return errors.New("--max-log-bytes must be between 0 and 1048576")
	case o.maxOutstandingJobs <= 0 || o.maxOutstandingJobs > 2500:
		return errors.New("--max-outstanding-jobs must be between 1 and 2500")
	case o.mode == "workflows" && o.maxOutstandingJobs < o.jobsPerRun:
		return errors.New("--max-outstanding-jobs must fit at least one full workflow matrix")
	case o.mode == "workflows" && len(o.runID) > 59:
		return errors.New("workflow mode requires --run-id to be at most 59 characters so generated markers stay within the workflow limit")
	case o.runnerMemoryMiB < 256 || o.runnerMemoryMiB > 65536:
		return errors.New("--runner-memory-mib must be between 256 and 65536")
	case o.runnerCPUs <= 0 || o.runnerCPUs > 32:
		return errors.New("--runner-cpus must be > 0 and <= 32")
	case o.drainTimeout <= 0 || o.drainTimeout > 10*time.Minute:
		return errors.New("--drain-timeout must be between 1ns and 10m")
	case !safeRunID.MatchString(o.runID) && o.runID != "":
		return errors.New("--run-id may contain only letters, digits, dot, underscore and hyphen")
	}
	return nil
}

func commandCleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	opts := addCommonFlags(fs)
	runID := fs.String("run-id", "", "run identifier to reconcile")
	artifactsDir := fs.String("artifacts-dir", "tmp/actions-scaleset-stress", "run state directory")
	apply := fs.Bool("apply", false, "apply cleanup; default is preview only")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runID == "" || !safeRunID.MatchString(*runID) {
		return errors.New("cleanup requires a valid --run-id")
	}
	cfg, token, err := loadConfig(opts.configPath)
	if err != nil {
		return err
	}
	target, err := newTarget(cfg, token, opts.caCert, opts.insecure)
	if err != nil {
		return err
	}
	state, err := openRunState(*artifactsDir, *runID, target)
	if err != nil {
		return err
	}
	defer state.Close()
	if !*apply {
		return state.preview(os.Stdout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := cleanupRun(ctx, target, state); err != nil {
		return err
	}
	if err := state.update(func(data *runData) { data.CleanupStatus = "complete" }); err != nil {
		return err
	}
	if err := state.writeReport(); err != nil {
		return err
	}
	fmt.Printf("Cleanup completed for run %s\n", *runID)
	return nil
}

func commandGuard(args []string) error {
	fs := flag.NewFlagSet("guard", flag.ContinueOnError)
	root := fs.String("artifacts-dir", "tmp/actions-scaleset-stress", "run state directory")
	runID := fs.String("run-id", "", "owned run identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !safeRunID.MatchString(*runID) {
		return errors.New("invalid guard run ID")
	}
	return runGuard(filepath.Join(*root, *runID))
}

func safeGuardEnvironment() []string {
	allowed := map[string]bool{
		"PATH": true, "DOCKER_HOST": true, "DOCKER_TLS_VERIFY": true,
		"DOCKER_CERT_PATH": true, "DOCKER_CONTEXT": true, "DOCKER_API_VERSION": true,
	}
	var result []string
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if ok && allowed[key] {
			result = append(result, entry)
		}
	}
	if runtime.GOOS == "windows" {
		result = append(result, "SystemRoot="+os.Getenv("SystemRoot"))
	}
	return result
}

func startGuard(state *runState) (*guardProcess, error) {
	if err := state.writeHeartbeat(); err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate guard executable: %w", err)
	}
	command := exec.Command(executable, "guard", "--artifacts-dir", filepath.Dir(state.dir), "--run-id", state.data.RunID)
	command.Env = safeGuardEnvironment()
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	if runtime.GOOS != "windows" {
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start independent container guard: %w", err)
	}
	guard := &guardProcess{command: command, done: make(chan error, 1), dir: state.dir}
	go func() { guard.done <- command.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(state.dir, "guard-ready")); err == nil {
			return guard, nil
		}
		select {
		case err := <-guard.done:
			return nil, fmt.Errorf("container guard exited before readiness: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	_ = guard.stop()
	return nil, errors.New("container guard did not become ready within 10 seconds")
}
