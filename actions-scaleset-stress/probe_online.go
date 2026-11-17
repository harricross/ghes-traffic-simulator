package main

// Spike for the "online but idle" runner idea: can a small protocol client
// built from a JIT config make a registered runner appear online in GHES
// without starting the real runner? The probe uses a handful of runners on a
// throwaway scale set and removes everything it creates. It prints protocol
// metadata only; credentials, tokens and RSA parameters are never logged.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/actions/scaleset"
)

const (
	probeMaxRunners = 10000
	probeMaxHold    = 30 * time.Minute
	probeSmallRun   = 20
)

func redactedURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<unparseable>"
	}
	return u.Scheme + "://" + u.Host + "/<redacted-path>"
}

func commandProbeOnline(args []string) error {
	fs := flag.NewFlagSet("probe-online", flag.ContinueOnError)
	opts := addCommonFlags(fs)
	runners := fs.Int("runners", 3, "number of JIT runners to bring online")
	hold := fs.Duration("hold", 90*time.Second, "how long to keep the sessions open")
	concurrency := fs.Int("open-concurrency", 10, "concurrent JIT generations, session opens and cleanup calls")
	version := fs.String("runner-version", "2.338.0", "runner version reported in the session")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runners < 1 || *runners > probeMaxRunners {
		return fmt.Errorf("--runners must be between 1 and %d", probeMaxRunners)
	}
	if *concurrency < 1 || *concurrency > 50 {
		return errors.New("--open-concurrency must be between 1 and 50")
	}
	if *hold < 10*time.Second || *hold > probeMaxHold {
		return fmt.Errorf("--hold must be between 10s and %s", probeMaxHold)
	}
	cfg, token, err := loadConfig(opts.configPath)
	if err != nil {
		return err
	}
	t, err := newTarget(cfg, token, opts.caCert, opts.insecure)
	if err != nil {
		return err
	}
	client := &probeHTTP{client: &http.Client{Transport: t.transport, Timeout: 90 * time.Second}}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stamp := time.Now().UTC().Format("0102-150405")
	setName := "probe-online-" + stamp
	created, err := t.scaleSet.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          setName,
		RunnerGroupID: 1,
		Labels:        []scaleset.Label{{Name: setName, Type: "System"}},
	})
	if err != nil {
		return fmt.Errorf("create probe scale set: %w", err)
	}
	fmt.Printf("created probe scale set %s (id %d)\n", setName, created.ID)
	t.scaleSet.SetSystemInfo(scaleset.SystemInfo{System: "ghes-traffic-simulator", Version: "dev", ScaleSetID: created.ID, Subsystem: "actions-load"})

	probes := make([]*probeRunner, *runners)
	cleanup := func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		var sessionErrs, removeErrs atomic.Int64
		_ = runLimited(cctx, len(probes), *concurrency, func(ctx context.Context, i int) error {
			if r := probes[i]; r != nil {
				if err := r.closeSession(ctx); err != nil {
					sessionErrs.Add(1)
				}
			}
			return nil
		})
		removed := 0
		var removedMu sync.Mutex
		_ = runLimited(cctx, len(probes), *concurrency, func(ctx context.Context, i int) error {
			if r := probes[i]; r != nil && r.id != 0 {
				if err := t.scaleSet.RemoveRunner(ctx, int64(r.id)); err != nil && !isRunnerNotFound(err) {
					removeErrs.Add(1)
					return nil
				}
				removedMu.Lock()
				removed++
				removedMu.Unlock()
			}
			return nil
		})
		if sessionErrs.Load() > 0 || removeErrs.Load() > 0 {
			fmt.Printf("cleanup: %d session delete errors, %d runner remove errors\n", sessionErrs.Load(), removeErrs.Load())
		}
		if err := t.scaleSet.DeleteRunnerScaleSet(cctx, created.ID); err != nil {
			fmt.Printf("cleanup: delete scale set %d: %s (removed %d runners)\n", created.ID, safeError(err), removed)
		} else {
			fmt.Printf("cleanup: removed %d runners and scale set %d\n", removed, created.ID)
		}
	}
	defer cleanup()

	stats := func(label string) {
		ss, err := t.scaleSet.GetRunnerScaleSetByID(context.Background(), created.ID)
		if err != nil || ss == nil || ss.Statistics == nil {
			fmt.Printf("stats[%s]: unavailable (%v)\n", label, err)
			return
		}
		s := ss.Statistics
		fmt.Printf("stats[%s]: registered=%d idle=%d busy=%d\n", label, s.TotalRegisteredRunners, s.TotalIdleRunners, s.TotalBusyRunners)
	}

	hostname, _ := os.Hostname()
	genStart := time.Now()
	err = runLimited(ctx, *runners, *concurrency, func(ctx context.Context, i int) error {
		name := fmt.Sprintf("probe-%s-%d", stamp, i+1)
		cfgResp, err := t.scaleSet.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: name, WorkFolder: "_work"}, created.ID)
		if err != nil {
			return fmt.Errorf("generate JIT config %d: %w", i+1, err)
		}
		r := &probeRunner{
			name: name, id: cfgResp.Runner.ID, http: client, ver: *version,
			owner: fmt.Sprintf("%s (PID: %d)", hostname, os.Getpid()),
		}
		probes[i] = r
		jit, err := decodeProbeJIT(cfgResp.EncodedJITConfig)
		if err != nil {
			return fmt.Errorf("decode JIT config %d: %w", i+1, err)
		}
		r.jit = jit
		r.setStage("jit_ok")
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("generated %d JIT configs in %s\n", *runners, time.Since(genStart).Round(time.Second))
	first := probes[0].jit
	fmt.Printf("jit files: %s\n", strings.Join(first.files, ", "))
	fmt.Printf("jit settings: use_v2_flow=%t use_runner_admin_flow=%t ephemeral=%t hosted=%t pool_id=%d server_url=%s server_url_v2=%s\n",
		first.settings.UseV2Flow, first.settings.UseRunnerAdminFlow, first.settings.Ephemeral, first.settings.IsHostedServer,
		first.settings.PoolID, redactedURL(first.settings.ServerURL), redactedURL(first.settings.ServerURLV2))
	fmt.Printf("jit credentials: scheme=%s has_client_id=%t authorization_url=%s authorization_url_v2=%s oauth_endpoint_url=%s\n",
		first.creds.Scheme, first.creds.Data.ClientID != "", redactedURL(first.creds.Data.AuthorizationURL),
		redactedURL(first.creds.Data.AuthorizationURLV2), redactedURL(first.creds.Data.OAuthEndpointURL))
	stats("registered-only")

	runCtx, stopRun := context.WithCancel(ctx)
	var group sync.WaitGroup
	openSem := make(chan struct{}, *concurrency)
	openStart := time.Now()
	for _, r := range probes {
		group.Add(1)
		go func(r *probeRunner) {
			defer group.Done()
			select {
			case openSem <- struct{}{}:
			case <-runCtx.Done():
				return
			}
			err := r.openWithRetry(runCtx)
			<-openSem
			if err != nil {
				r.fail(err)
				return
			}
			r.poll(runCtx)
		}(r)
	}

	deadline := time.After(*hold)
	tickEvery := 10 * time.Second
	if *runners > probeSmallRun {
		tickEvery = 30 * time.Second
	}
	tick := time.NewTicker(tickEvery)
	defer tick.Stop()
	report := func() {
		if *runners > probeSmallRun {
			printProbeSummary(probes)
		} else {
			printProbeRunners(probes)
		}
	}
	reportedOpen := false
hold:
	for {
		select {
		case <-ctx.Done():
			break hold
		case <-deadline:
			break hold
		case <-tick.C:
			if !reportedOpen && probeStageCount(probes, "session_open")+probeStageCount(probes, "error") == len(probes) {
				reportedOpen = true
				fmt.Printf("all sessions attempted %s after the first open\n", time.Since(openStart).Round(time.Second))
			}
			stats("holding")
			report()
		}
	}
	stats("holding-final")
	report()
	stopRun()
	group.Wait()
	_ = runLimited(context.Background(), len(probes), *concurrency, func(ctx context.Context, i int) error {
		if err := probes[i].closeSession(ctx); err != nil {
			fmt.Printf("close session %s: %s\n", probes[i].name, safeError(err))
		}
		return nil
	})
	time.Sleep(10 * time.Second)
	stats("sessions-closed")
	return nil
}

func probeStageCount(probes []*probeRunner, stage string) int {
	n := 0
	for _, r := range probes {
		if s, _ := r.stage.Load().(string); s == stage {
			n++
		}
	}
	return n
}

func printProbeSummary(probes []*probeRunner) {
	stages := map[string]int{}
	errs := map[string]int{}
	var polls, empty, messages int64
	var sessionMS []int64
	for _, r := range probes {
		stage, _ := r.stage.Load().(string)
		stages[stage]++
		polls += r.polls.Load()
		empty += r.emptyPoll.Load()
		messages += r.messages.Load()
		if ms := r.sessionMS.Load(); ms > 0 {
			sessionMS = append(sessionMS, ms)
		}
		if e, _ := r.lastErr.Load().(string); e != "" {
			errs[e]++
		}
	}
	var stageParts []string
	for s, n := range stages {
		stageParts = append(stageParts, fmt.Sprintf("%s=%d", s, n))
	}
	sort.Strings(stageParts)
	fmt.Printf("  stages: %s | polls=%d empty=%d messages=%d", strings.Join(stageParts, " "), polls, empty, messages)
	if len(sessionMS) > 0 {
		sort.Slice(sessionMS, func(i, j int) bool { return sessionMS[i] < sessionMS[j] })
		fmt.Printf(" | session_ms p50=%d p95=%d max=%d", sessionMS[len(sessionMS)/2], sessionMS[len(sessionMS)*95/100], sessionMS[len(sessionMS)-1])
	}
	fmt.Println()
	type errCount struct {
		msg string
		n   int
	}
	var ordered []errCount
	for m, n := range errs {
		ordered = append(ordered, errCount{m, n})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].n > ordered[j].n })
	for i, e := range ordered {
		if i == 5 {
			break
		}
		fmt.Printf("  error x%d: %s\n", e.n, snippet([]byte(e.msg)))
	}
}

func printProbeRunners(probes []*probeRunner) {
	for _, r := range probes {
		stage, _ := r.stage.Load().(string)
		lastErr, _ := r.lastErr.Load().(string)
		var types []string
		r.msgTypes.Range(func(k, v any) bool {
			types = append(types, fmt.Sprintf("%v=%d", k, v.(*atomic.Int64).Load()))
			return true
		})
		sort.Strings(types)
		fmt.Printf("  %s stage=%s token_ms=%d session_ms=%d polls=%d empty=%d messages=%d %s", r.name, stage,
			r.tokenMS.Load(), r.sessionMS.Load(), r.polls.Load(), r.emptyPoll.Load(), r.messages.Load(), strings.Join(types, ","))
		if lastErr != "" {
			fmt.Printf(" last_error=%q", lastErr)
		}
		fmt.Println()
	}
}
