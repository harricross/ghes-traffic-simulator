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
	case "probe-online":
		return commandProbeOnline(args[1:])
	default:
		return fmt.Errorf("unknown command %q (run help)", args[0])
	}
}

func printUsage(w *os.File) {
	fmt.Fprint(w, `actions-scaleset-stress: bounded GitHub Actions load against real GHES

Usage:
  actions-scaleset-stress check   --config <file>
  actions-scaleset-stress prepare --config <file>
  actions-scaleset-stress run     --config <file> --mode registrations|workflows|hold [--workload synthetic|ci] [limits]
  actions-scaleset-stress run     --config <file> --mode fleet --fleet <spec.json> [--workload synthetic|ci] [limits]
  actions-scaleset-stress cleanup --config <file> --run-id <id> [--apply]
  actions-scaleset-stress probe-online --config <file> [--runners 3] [--hold 90s]   (spike: protocol-only online sessions)

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
	fmt.Printf("Private test repository is ready: %s/%s (synthetic workflow %s, CI workflow %s, CI project %s/)\n",
		cfg.Owner, cfg.Repo, cfg.WorkflowFile, ciWorkflowFile, ciProjectDir)
	return nil
}

type runOptions struct {
	commonOptions
	mode               string
	workload           string
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
	standingRunners    int
	rampRate           float64
	rampTimeout        time.Duration
	hostGuard          bool
	minFreeMemMiB      int64
	minFreeDiskMiB     int64
	maxLoadPerCPU      float64
	workerReserveMiB   int64
	fleetPath          string
	fleet              fleetSpec
	openConcurrency    int
	dispatchRate       float64
}

func commandRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	o := &runOptions{}
	common := addCommonFlags(fs)
	fs.StringVar(&o.mode, "mode", "", "required profile: registrations, workflows, hold (bulk registered-only runners held for a long dwell) or fleet (runner groups mixing real and fake runners; needs --fleet)")
	fs.StringVar(&o.fleetPath, "fleet", "", "fleet mode: JSON spec of runner groups and their real, online and offline runner counts")
	fs.IntVar(&o.openConcurrency, "open-concurrency", 6, "fleet mode: concurrent session opens for fake-online runners")
	fs.Float64Var(&o.dispatchRate, "dispatch-rate", 0.5, "fleet mode: maximum workflow dispatches per second")
	fs.StringVar(&o.workload, "workload", workloadSynthetic, "workflows mode: synthetic (matrix sleep jobs) or ci (Lint, sharded unit tests, Build)")
	fs.StringVar(&o.runID, "run-id", "", "run identifier (default: generated)")
	fs.DurationVar(&o.duration, "duration", time.Minute, "hard maximum run duration")
	fs.IntVar(&o.total, "total", 10, "maximum registrations or workflow dispatches (hold mode: up to 100000)")
	fs.Float64Var(&o.rate, "rate", 1, "maximum new registrations or dispatches per second")
	fs.IntVar(&o.maxInflight, "max-inflight", 5, "maximum concurrent registration operations (registered runners held at once; hold mode: concurrent API calls)")
	fs.DurationVar(&o.dwell, "dwell", 5*time.Second, "registered runner dwell before removal (up to 1h; hold mode up to 24h)")
	fs.IntVar(&o.maxRunners, "max-runners", 4, "maximum concurrent official runner containers")
	fs.IntVar(&o.standingRunners, "standing-runners", 0, "workflows mode: idle runner containers to ramp up to and keep online (0 = on demand only)")
	fs.Float64Var(&o.rampRate, "ramp-rate", 5, "maximum runner containers started per second while growing the pool")
	fs.DurationVar(&o.rampTimeout, "ramp-timeout", 15*time.Minute, "maximum time to wait for the standing pool before dispatching")
	fs.BoolVar(&o.hostGuard, "host-guard", runtime.GOOS == "linux", "stop growing the runner pool when this host runs low on memory or disk (Linux only)")
	fs.Int64Var(&o.minFreeMemMiB, "min-free-mem-mib", 2048, "host guard: memory that must remain available")
	fs.Int64Var(&o.minFreeDiskMiB, "min-free-disk-mib", 10240, "host guard: disk space that must remain free under the artifacts directory")
	fs.Float64Var(&o.maxLoadPerCPU, "max-load-per-cpu", 3, "host guard: pause growth while the 1-minute load average exceeds this per CPU")
	fs.Int64Var(&o.workerReserveMiB, "worker-reserve-mib", 256, "host guard: memory reserved per busy runner for its job worker")
	fs.IntVar(&o.jobsPerRun, "jobs-per-run", 1, "matrix jobs (synthetic) or test shards (ci) in each dispatched workflow")
	fs.DurationVar(&o.jobDuration, "job-duration", time.Second, "bounded synthetic job or extra test-shard duration")
	fs.IntVar(&o.failPercent, "fail-percent", 0, "percentage of dispatched workflow runs that intentionally fail one job; the rest pass")
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
	if o.mode != "registrations" && o.mode != "workflows" && o.mode != modeHold && o.mode != modeFleet {
		return errors.New("--mode must be registrations, workflows, hold or fleet")
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
	var fleetPlan fleetBudget
	if o.mode == modeFleet {
		if fleetPlan, err = prepareFleetOptions(o, cfg); err != nil {
			return err
		}
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
	if !target.ready {
		return fmt.Errorf("run requires a prepared private repository with active workflows; current target state: %s", target.repoState)
	}
	state, err := createRunState(o.artifactsDir, o.runID, o.mode, target)
	if err != nil {
		return err
	}
	defer state.Close()
	if o.mode == "workflows" || o.mode == modeFleet {
		if err := state.update(func(data *runData) { data.Workload = o.workload }); err != nil {
			return err
		}
	}
	fmt.Printf("Run %s started (mode=%s, duration=%s, total=%d)\n", o.runID, o.mode, o.duration, o.total)
	var guard *guardProcess
	if o.mode == "workflows" || (o.mode == modeFleet && fleetPlan.Real > 0) {
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
	go func() {
		if o.mode == modeFleet {
			workDone <- runFleet(ctx, target, state, *o, fleetPlan)
			return
		}
		workDone <- runWorkload(ctx, target, state, *o)
	}()
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
	cleanupTimeout := 10 * time.Minute
	if o.mode == modeFleet {
		cleanupTimeout = fleetCleanupTimeout(fleetPlan.Real + fleetPlan.Fake)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cleanupTimeout)
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
	printScaleSummary(os.Stdout, o.mode, state.snapshot().Scale)
	if o.mode == "workflows" || (o.mode == modeFleet && fleetPlan.Real > 0) {
		summary := state.snapshot()
		fmt.Printf("Workflow runs (%s): %d dispatched, %d passed, %d failed intentionally as requested, %d failed verification, %d unresolved\n",
			o.workload, summary.Attempts, summary.Passed, summary.IntentionalFailures, summary.Failed, summary.Unresolved)
		if workErr == nil && (summary.Failed > 0 || summary.Unresolved > 0) {
			workErr = fmt.Errorf("%d workflow runs failed verification and %d were unresolved; see verification_error in %s",
				summary.Failed, summary.Unresolved, filepath.Join(state.dir, "journal.jsonl"))
		}
	}
	if workErr != nil {
		_ = state.update(func(data *runData) {
			data.Status = "failed"
			data.LastError = safeError(workErr)
		})
	} else if cleanupErr != nil {
		_ = state.update(func(data *runData) {
			data.Status = "cleanup-incomplete"
			data.LastError = safeError(cleanupErr)
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
	case o.maxInflight <= 0 || o.maxInflight > 1000:
		return errors.New("--max-inflight must be between 1 and 1000")
	case o.dwell < 0 || (o.mode != modeHold && o.mode != modeFleet && o.dwell > time.Hour) || o.dwell > 24*time.Hour:
		return errors.New("--dwell must be between 0 and 1h (hold and fleet modes: 24h)")
	case (o.mode == modeHold || o.mode == modeFleet) && o.dwell >= o.duration:
		return errors.New("--duration must exceed --dwell in hold and fleet modes so registration and removal have time to finish")
	case o.maxRunners <= 0 || o.maxRunners > 1000:
		return errors.New("--max-runners must be between 1 and 1000")
	case o.standingRunners < 0 || o.standingRunners > 1000:
		return errors.New("--standing-runners must be between 0 and 1000")
	case o.standingRunners > 0 && o.mode != "workflows":
		return errors.New("--standing-runners applies only to --mode workflows")
	case o.standingRunners > o.maxRunners:
		return errors.New("--standing-runners cannot exceed --max-runners")
	case o.rampRate <= 0 || o.rampRate > 50:
		return errors.New("--ramp-rate must be > 0 and <= 50 runners per second")
	case o.rampTimeout <= 0 || o.rampTimeout > 2*time.Hour:
		return errors.New("--ramp-timeout must be between 1ns and 2h")
	case o.minFreeMemMiB < 0 || o.minFreeDiskMiB < 0 || o.workerReserveMiB < 0:
		return errors.New("host guard memory and disk limits cannot be negative")
	case o.maxLoadPerCPU < 0:
		return errors.New("--max-load-per-cpu cannot be negative")
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
	case o.workload != workloadSynthetic && o.workload != workloadCI:
		return errors.New("--workload must be synthetic or ci")
	case o.mode != "workflows" && o.mode != modeFleet && o.workload != workloadSynthetic:
		return errors.New("--workload applies only to --mode workflows or fleet")
	case (o.mode == "workflows" || o.mode == modeFleet) && o.maxOutstandingJobs < jobsPerDispatch(o.workload, o.jobsPerRun):
		return fmt.Errorf("--max-outstanding-jobs must fit at least one full workflow dispatch (%d jobs)", jobsPerDispatch(o.workload, o.jobsPerRun))
	case (o.mode == "workflows" || o.mode == modeFleet) && len(o.runID) > 59:
		return errors.New("workflow and fleet modes require --run-id to be at most 59 characters so generated markers stay within the workflow limit")
	case (o.mode == modeFleet) != (o.fleetPath != ""):
		return errors.New("--fleet is required with --mode fleet and applies only to it")
	case o.mode == modeFleet && (o.openConcurrency <= 0 || o.openConcurrency > 64):
		return errors.New("--open-concurrency must be between 1 and 64")
	case o.mode == modeFleet && (o.dispatchRate <= 0 || o.dispatchRate > 10):
		return errors.New("--dispatch-rate must be > 0 and <= 10 dispatches per second")
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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
