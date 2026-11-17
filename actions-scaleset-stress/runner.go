package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

const (
	provisionConcurrency        = 8
	teardownConcurrency         = 16
	monitorInterval             = 10 * time.Second
	statusInterval              = 30 * time.Second
	hostPauseDelay              = 2 * time.Second
	maxConsecutiveStartFailures = 3
	// exitGrace gives a finished runner's job-completed message time to
	// arrive before its exit is treated as unexpected.
	exitGrace = 20 * time.Second
)

type managedRunner struct {
	name        string
	runnerID    int
	containerID string
	jobStarted  bool
}

type runnerPool struct {
	mu          sync.Mutex
	runSeq      int
	target      *target
	state       *runState
	docker      *dockerclient.Client
	session     listener.Client
	scaleSetID  int
	maxRunners  int
	memoryBytes int64
	nanoCPUs    int64
	entries     map[string]managedRunner

	// standing idle runners are kept warm on top of assigned jobs.
	standing int
	assigned int
	starting int
	// ceiling, when positive, caps the live runner count after provisioning
	// hit a host or GHES limit.
	ceiling  int
	failures int
	stats    scaleset.RunnerScaleSetStatistic
	limits   *hostLimits
	diskPath string
	rampRate float64
	wake     chan struct{}
	fatal    chan error

	// group, when set, is the runner group this pool serves; runner and
	// container names include it.
	group string
	// set, when set, aggregates host-wide counts across every pool on this
	// host; a nil set counts this pool alone.
	set *poolSet

	standingTarget  int
	started         int
	provisionErrors int
	containerExits  int
	peakLive        int
	peakBusy        int
	peakActiveJobs  int
	ceilingReason   string
	rampFinishedAt  time.Time
}

// poolSummary reports one pool's provisioning results.
type poolSummary struct {
	Group           string    `json:"group,omitempty"`
	ScaleSetID      int       `json:"scale_set_id"`
	StandingTarget  int       `json:"standing_target"`
	MaxRunners      int       `json:"max_runners"`
	Started         int       `json:"started"`
	Live            int       `json:"live"`
	PeakLive        int       `json:"peak_live"`
	PeakBusy        int       `json:"peak_busy"`
	PeakActiveJobs  int       `json:"peak_active_jobs"`
	ProvisionErrors int       `json:"provision_errors"`
	ContainerExits  int       `json:"container_exits"`
	CeilingRunners  int       `json:"ceiling_runners,omitempty"`
	CeilingReason   string    `json:"ceiling_reason,omitempty"`
	RampFinishedAt  time.Time `json:"ramp_finished_at,omitzero"`
}

// poolSet groups the runner pools that share this host so that peaks, ramp
// ceilings and the host guard count every pool's runners. Its pools share one
// fatal channel; the first error wins.
type poolSet struct {
	mu    sync.Mutex
	pools []*runnerPool
	fatal chan error
}

func newPoolSet() *poolSet {
	return &poolSet{fatal: make(chan error, 1)}
}

func (s *poolSet) add(p *runnerPool) {
	s.mu.Lock()
	s.pools = append(s.pools, p)
	s.mu.Unlock()
}

func newDockerClient(ctx context.Context, imageRef string) (*dockerclient.Client, error) {
	client, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create Docker client: %w", err)
	}
	if _, err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to Docker daemon: %w", err)
	}
	if _, _, err := client.ImageInspectWithRaw(ctx, imageRef); err != nil {
		if !errdefs.IsNotFound(err) {
			_ = client.Close()
			return nil, fmt.Errorf("inspect pinned runner image: %w", err)
		}
		pull, err := client.ImagePull(ctx, imageRef, image.PullOptions{})
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("pull pinned runner image: %w", err)
		}
		if _, err := io.Copy(io.Discard, pull); err != nil {
			_ = pull.Close()
			_ = client.Close()
			return nil, fmt.Errorf("read runner image pull result: %w", err)
		}
		if err := pull.Close(); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("close runner image pull: %w", err)
		}
	}
	return client, nil
}

// newRunnerPool creates a pool for one scale set. maxRunners caps its live
// runners; zero uses --max-runners. A non-empty group switches to fleet runner
// and container names.
func newRunnerPool(t *target, state *runState, docker *dockerclient.Client, set *poolSet, scaleSetID int, group string, standing, maxRunners int, options runOptions) *runnerPool {
	if maxRunners <= 0 {
		maxRunners = options.maxRunners
	}
	pool := &runnerPool{
		target:         t,
		state:          state,
		docker:         docker,
		scaleSetID:     scaleSetID,
		maxRunners:     maxRunners,
		memoryBytes:    options.runnerMemoryMiB * 1024 * 1024,
		nanoCPUs:       int64(math.Round(options.runnerCPUs * 1e9)),
		entries:        make(map[string]managedRunner),
		standing:       standing,
		standingTarget: standing,
		diskPath:       state.dir,
		rampRate:       options.rampRate,
		wake:           make(chan struct{}, 1),
		fatal:          make(chan error, 1),
		group:          group,
		set:            set,
	}
	if set != nil {
		pool.fatal = set.fatal
		set.add(pool)
	}
	if options.hostGuard {
		pool.limits = &hostLimits{
			minFreeMemBytes:    uint64(options.minFreeMemMiB) << 20,
			minFreeDiskBytes:   uint64(options.minFreeDiskMiB) << 20,
			maxLoadPerCPU:      options.maxLoadPerCPU,
			workerReserveBytes: uint64(options.workerReserveMiB) << 20,
			maxBusyRunners:     options.maxOutstandingJobs,
		}
	}
	return pool
}

func (p *runnerPool) Scale(ctx context.Context, message *scaleset.RunnerScaleSetMessage) error {
	if message == nil {
		return nil
	}
	if len(message.JobAvailableMessages) > 0 {
		requestIDs := make([]int64, 0, len(message.JobAvailableMessages))
		for _, job := range message.JobAvailableMessages {
			requestIDs = append(requestIDs, job.RunnerRequestID)
		}
		if p.session == nil {
			return errors.New("scale-set message session is unavailable")
		}
		if _, err := p.session.AcquireJobs(ctx, requestIDs); err != nil {
			return fmt.Errorf("acquire available jobs: %w", err)
		}
	}
	for _, job := range message.JobStartedMessages {
		p.markJobStarted(job.RunnerName)
		if err := p.state.record("job_started", map[string]any{
			"runner_name":     job.RunnerName,
			"workflow_run_id": job.WorkflowRunID,
		}); err != nil {
			return err
		}
	}
	for _, job := range message.JobCompletedMessages {
		if err := p.completeRunner(ctx, job.RunnerName); err != nil {
			return err
		}
		if err := p.state.record("job_completed", map[string]any{
			"runner_name":     job.RunnerName,
			"workflow_run_id": job.WorkflowRunID,
			"result":          job.Result,
		}); err != nil {
			return err
		}
	}
	if message.Statistics == nil {
		return nil
	}
	p.mu.Lock()
	p.assigned = message.Statistics.TotalAssignedJobs
	p.stats = *message.Statistics
	p.peakBusy = max(p.peakBusy, message.Statistics.TotalBusyRunners)
	p.mu.Unlock()
	p.nudge()
	return p.notePeaks()
}

func (p *runnerPool) nudge() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *runnerPool) markJobStarted(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[name]; ok {
		entry.jobStarted = true
		p.entries[name] = entry
		p.peakActiveJobs = max(p.peakActiveJobs, p.activeJobsLocked())
	}
}

// siblings returns every pool sharing this host, including p.
func (p *runnerPool) siblings() []*runnerPool {
	if p.set == nil {
		return []*runnerPool{p}
	}
	p.set.mu.Lock()
	defer p.set.mu.Unlock()
	return append([]*runnerPool(nil), p.set.pools...)
}

// hostRunners sums live and starting runners across the pools on this host,
// excluding skip. It must not be called with p.mu held.
func (p *runnerPool) hostRunners(skip *runnerPool) (live, starting int) {
	for _, pool := range p.siblings() {
		if pool == skip {
			continue
		}
		pool.mu.Lock()
		live += len(pool.entries)
		starting += pool.starting
		pool.mu.Unlock()
	}
	return live, starting
}

// hostStats sums the latest scale-set statistics of every pool on this host.
func (p *runnerPool) hostStats() scaleset.RunnerScaleSetStatistic {
	var total scaleset.RunnerScaleSetStatistic
	for _, pool := range p.siblings() {
		pool.mu.Lock()
		stats := pool.stats
		pool.mu.Unlock()
		total.TotalAvailableJobs += stats.TotalAvailableJobs
		total.TotalAcquiredJobs += stats.TotalAcquiredJobs
		total.TotalAssignedJobs += stats.TotalAssignedJobs
		total.TotalRunningJobs += stats.TotalRunningJobs
		total.TotalRegisteredRunners += stats.TotalRegisteredRunners
		total.TotalBusyRunners += stats.TotalBusyRunners
		total.TotalIdleRunners += stats.TotalIdleRunners
	}
	return total
}

func (p *runnerPool) notePeaks() error {
	live, _ := p.hostRunners(nil)
	stats := p.hostStats()
	return p.state.update(func(data *runData) {
		scale := &data.Scale
		scale.PeakContainers = max(scale.PeakContainers, live)
		scale.PeakRegistered = max(scale.PeakRegistered, stats.TotalRegisteredRunners)
		scale.PeakIdle = max(scale.PeakIdle, stats.TotalIdleRunners)
		scale.PeakBusy = max(scale.PeakBusy, stats.TotalBusyRunners)
		scale.PeakAssignedJobs = max(scale.PeakAssignedJobs, stats.TotalAssignedJobs)
		scale.PeakRunningJobs = max(scale.PeakRunningJobs, stats.TotalRunningJobs)
	})
}

func (p *runnerPool) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// activeJobs counts runners that have started a job and not yet completed it.
func (p *runnerPool) activeJobs() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeJobsLocked()
}

func (p *runnerPool) activeJobsLocked() int {
	active := 0
	for _, entry := range p.entries {
		if entry.jobStarted {
			active++
		}
	}
	return active
}

// fields adds the pool's group and scale set to fleet journal events.
func (p *runnerPool) fields(fields map[string]any) map[string]any {
	if p.group != "" {
		fields["group"] = p.group
		fields["scale_set_id"] = p.scaleSetID
	}
	return fields
}

func (p *runnerPool) summary() poolSummary {
	p.mu.Lock()
	defer p.mu.Unlock()
	return poolSummary{
		Group:           p.group,
		ScaleSetID:      p.scaleSetID,
		StandingTarget:  p.standingTarget,
		MaxRunners:      p.maxRunners,
		Started:         p.started,
		Live:            len(p.entries),
		PeakLive:        p.peakLive,
		PeakBusy:        p.peakBusy,
		PeakActiveJobs:  p.peakActiveJobs,
		ProvisionErrors: p.provisionErrors,
		ContainerExits:  p.containerExits,
		CeilingRunners:  p.ceiling,
		CeilingReason:   p.ceilingReason,
		RampFinishedAt:  p.rampFinishedAt,
	}
}

func (p *runnerPool) setStanding(n int) {
	p.mu.Lock()
	p.standing = n
	p.mu.Unlock()
	p.nudge()
}

func (p *runnerPool) desiredLocked() int {
	desired := min(p.maxRunners, p.standing+p.assigned)
	if p.ceiling > 0 {
		desired = min(desired, p.ceiling)
	}
	return desired
}

// rampComplete reports that every standing runner the host and GHES allow is
// running.
func (p *runnerPool) rampComplete() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starting == 0 && len(p.entries) >= p.desiredLocked()
}

// markRampFinished records when this pool's standing ramp ended.
func (p *runnerPool) markRampFinished() {
	p.mu.Lock()
	if p.rampFinishedAt.IsZero() {
		p.rampFinishedAt = time.Now().UTC()
	}
	p.mu.Unlock()
}

// rampTimedOut ends a ramp that did not complete in time.
func (p *runnerPool) rampTimedOut() {
	_ = p.state.record("ramp_timeout", p.fields(map[string]any{"containers": p.count()}))
	p.markRampFinished()
}

// provision converges the live runner count toward the desired count at the
// configured ramp rate. Host or GHES limits lower the ceiling instead of
// failing the run.
func (p *runnerPool) provision(ctx context.Context) {
	interval := time.Duration(float64(time.Second) / p.rampRate)
	slots := make(chan struct{}, provisionConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	next := time.Now()
	for {
		if wait := time.Until(next); wait > 0 {
			if !sleepContext(ctx, wait) {
				return
			}
		}
		if !p.reserve() {
			select {
			case <-ctx.Done():
				return
			case <-p.wake:
			case <-time.After(time.Second):
			}
			continue
		}
		verdict, reason := p.checkHost()
		switch verdict {
		case hostCeiling:
			p.release()
			p.lowerCeiling(reason)
			if !sleepContext(ctx, hostPauseDelay) {
				return
			}
			continue
		case hostPause:
			p.release()
			if !sleepContext(ctx, hostPauseDelay) {
				return
			}
			continue
		}
		next = time.Now().Add(interval)
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			p.release()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			err := p.startRunner(ctx)
			p.finishStart(ctx, err)
		}()
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// reserve claims a provisioning slot when the pool is below its target.
func (p *runnerPool) reserve() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.entries)+p.starting >= p.desiredLocked() {
		return false
	}
	p.starting++
	return true
}

func (p *runnerPool) release() {
	p.mu.Lock()
	p.starting--
	p.mu.Unlock()
}

func (p *runnerPool) checkHost() (int, string) {
	if p.limits == nil {
		return hostOK, ""
	}
	sample, err := readHostSample(p.diskPath)
	if err != nil {
		return hostCeiling, err.Error()
	}
	live, starting := p.hostRunners(nil)
	return p.limits.evaluate(sample, live+starting-1)
}

// lowerCeiling caps the pool at its current size. A pool with no runners
// cannot lower its ceiling, so that is fatal.
func (p *runnerPool) lowerCeiling(reason string) {
	p.mu.Lock()
	live := len(p.entries) + p.starting
	if live == 0 {
		p.mu.Unlock()
		p.fail(fmt.Errorf("cannot provision any runner: %s", reason))
		return
	}
	changed := p.ceiling == 0 || live < p.ceiling
	if changed {
		p.ceiling = live
		p.ceilingReason = reason
	}
	p.mu.Unlock()
	if !changed {
		return
	}
	otherLive, otherStarting := p.hostRunners(p)
	hostReason := reason
	if p.group == "" {
		fmt.Printf("Ramp ceiling reached at %d runners: %s\n", live, reason)
	} else {
		fmt.Printf("Group %s ramp ceiling reached at %d runners: %s\n", p.group, live, reason)
		hostReason = "group " + p.group + ": " + reason
	}
	_ = p.state.update(func(data *runData) {
		data.Scale.CeilingRunners = live + otherLive + otherStarting
		data.Scale.CeilingReason = hostReason
	})
	_ = p.state.record("ramp_ceiling", p.fields(map[string]any{"runners": live, "reason": reason}))
}

func (p *runnerPool) fail(err error) {
	if p.group != "" {
		err = fmt.Errorf("group %s: %w", p.group, err)
	}
	select {
	case p.fatal <- err:
	default:
	}
}

func (p *runnerPool) finishStart(ctx context.Context, err error) {
	p.mu.Lock()
	p.starting--
	if err == nil {
		p.failures = 0
		p.mu.Unlock()
		return
	}
	if ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.failures++
	p.provisionErrors++
	consecutive := p.failures
	live := len(p.entries) + p.starting
	p.mu.Unlock()
	_ = p.state.update(func(data *runData) { data.Scale.ProvisionErrors++ })
	_ = p.state.record("runner_provision_failed", p.fields(map[string]any{"error": safeError(err), "consecutive": consecutive, "live": live}))
	if consecutive < maxConsecutiveStartFailures {
		return
	}
	if live == 0 {
		p.fail(fmt.Errorf("runner provisioning failed %d times in a row: %w", consecutive, err))
		return
	}
	p.mu.Lock()
	p.failures = 0
	p.mu.Unlock()
	p.lowerCeiling("provisioning failed: " + safeError(err))
}

func (p *runnerPool) runnerNameFor(index int) string {
	if p.group == "" {
		return runnerName(p.state.data.RunID, 100000+index)
	}
	return fleetRunnerName(p.state.data.RunID, p.group, fleetKindReal, index)
}

func (p *runnerPool) containerNameFor(index int) string {
	prefix := "ghes-load-" + shortHash(p.state.data.RunID) + "-"
	if p.group != "" {
		prefix += p.group + "-"
	}
	return prefix + fmt.Sprintf("%06d", index)
}

func (p *runnerPool) startRunner(ctx context.Context) error {
	p.mu.Lock()
	p.runSeq++
	index := p.runSeq
	p.mu.Unlock()
	name := p.runnerNameFor(index)
	if err := p.state.record("workflow_runner_intent", map[string]any{"runner_name": name}); err != nil {
		return err
	}
	jit, err := p.target.scaleSet.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       name,
		WorkFolder: "_work",
	}, p.scaleSetID)
	if err != nil {
		observed, lookupErr := p.target.scaleSet.GetRunnerByName(context.WithoutCancel(ctx), name)
		if lookupErr == nil && observed != nil {
			if stateErr := p.state.update(func(data *runData) {
				data.Runners = append(data.Runners, runnerRecord{ID: observed.ID, Name: name, ScaleSetID: p.scaleSetID, CreatedAt: time.Now().UTC()})
			}); stateErr != nil {
				return errors.Join(fmt.Errorf("generate runner config: %w", err), stateErr)
			}
			if cleanupErr := removeRunner(context.Background(), p.target, p.state, observed.ID, name); cleanupErr != nil {
				_ = p.state.update(func(data *runData) { data.Unresolved++ })
				return errors.Join(fmt.Errorf("generate runner config: %w", err), cleanupErr)
			}
			return fmt.Errorf("generate runner config: %w", err)
		}
		if lookupErr != nil {
			stateErr := p.state.update(func(data *runData) {
				data.Runners = append(data.Runners, runnerRecord{Name: name, ScaleSetID: p.scaleSetID, CreatedAt: time.Now().UTC()})
				data.Unresolved++
			})
			return errors.Join(fmt.Errorf("generate runner config failed and reconciliation is unresolved: %w", err), stateErr)
		}
		return fmt.Errorf("generate runner config: %w", err)
	}
	if jit == nil || jit.Runner == nil || jit.Runner.ID <= 0 || jit.EncodedJITConfig == "" {
		return fmt.Errorf("runner config for %s did not include a runner identity and JIT config", name)
	}
	if err := p.state.update(func(data *runData) {
		data.Runners = append(data.Runners, runnerRecord{ID: jit.Runner.ID, Name: name, ScaleSetID: p.scaleSetID, CreatedAt: time.Now().UTC()})
	}); err != nil {
		_ = removeRunner(context.Background(), p.target, p.state, jit.Runner.ID, name)
		return err
	}
	containerName := p.containerNameFor(index)
	config := &container.Config{
		Image: p.target.cfg.RunnerImage,
		User:  "runner",
		Cmd:   []string{"/home/runner/run.sh"},
		Env:   []string{"ACTIONS_RUNNER_INPUT_JITCONFIG=" + jit.EncodedJITConfig},
		Labels: map[string]string{
			runContainerLabel:                          p.state.data.RunID,
			"com.github.ghes-traffic-simulator.runner": name,
		},
	}
	hostConfig := &container.HostConfig{
		Memory:        p.memoryBytes,
		NanoCPUs:      p.nanoCPUs,
		RestartPolicy: container.RestartPolicy{Name: "no"},
	}
	created, err := p.docker.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
	if err != nil {
		_ = removeRunner(context.Background(), p.target, p.state, jit.Runner.ID, name)
		return fmt.Errorf("create runner container: %w", err)
	}
	if err := p.docker.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = p.docker.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
		_ = removeRunner(context.Background(), p.target, p.state, jit.Runner.ID, name)
		return fmt.Errorf("start runner container: %w", err)
	}
	entry := managedRunner{name: name, runnerID: jit.Runner.ID, containerID: created.ID}
	p.mu.Lock()
	p.entries[name] = entry
	p.started++
	p.peakLive = max(p.peakLive, len(p.entries))
	p.mu.Unlock()
	live, _ := p.hostRunners(nil)
	if err := p.state.update(func(data *runData) {
		data.ContainerIDs = append(data.ContainerIDs, created.ID)
		data.Scale.PeakContainers = max(data.Scale.PeakContainers, live)
	}); err != nil {
		_ = p.docker.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
		return err
	}
	return p.state.record("workflow_runner_started", map[string]any{
		"runner_id":    jit.Runner.ID,
		"runner_name":  name,
		"container_id": created.ID,
	})
}

func (p *runnerPool) completeRunner(ctx context.Context, name string) error {
	p.mu.Lock()
	entry, ok := p.entries[name]
	if ok {
		delete(p.entries, name)
	}
	p.mu.Unlock()
	p.nudge()
	if !ok {
		// The runner may have been retired as an exited idle container first.
		return p.state.record("job_completed_untracked_runner", map[string]any{"runner_name": name})
	}
	// Teardown failures are journaled, not returned: a Scale error would stop
	// the listener, and cleanup retries any container or runner left behind.
	if err := p.teardownRunner(ctx, entry); err != nil {
		return p.state.record("runner_teardown_failed", map[string]any{"runner_name": name, "error": safeError(err)})
	}
	return nil
}

func (p *runnerPool) teardownRunner(ctx context.Context, entry managedRunner) error {
	name := entry.name
	err := p.docker.ContainerRemove(ctx, entry.containerID, container.RemoveOptions{Force: true})
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove completed runner container: %w", err)
	}
	observed, err := p.target.scaleSet.GetRunnerByName(ctx, name)
	if err != nil {
		return fmt.Errorf("verify ephemeral runner removal: %w", err)
	}
	if observed != nil {
		return removeRunner(ctx, p.target, p.state, observed.ID, name)
	}
	return p.state.update(func(data *runData) {
		for i := range data.Runners {
			if data.Runners[i].Name == name {
				data.Runners[i].Removed = true
			}
		}
	})
}

// monitor reconciles runner containers with the pool and reports status.
func (p *runnerPool) monitor(ctx context.Context) {
	reconcile := time.NewTicker(monitorInterval)
	defer reconcile.Stop()
	status := time.NewTicker(statusInterval)
	defer status.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcile.C:
			p.reconcile(ctx)
		case <-status.C:
			p.reportStatus()
		}
	}
}

// reconcile retires idle runners whose container exited before running a
// job. Runners that started a job are completed by the listener instead.
func (p *runnerPool) reconcile(ctx context.Context) {
	items, err := p.docker.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", runContainerLabel+"="+p.state.data.RunID)),
	})
	if err != nil {
		return
	}
	running := make(map[string]bool, len(items))
	for _, item := range items {
		running[item.ID] = item.State == "running"
	}
	p.mu.Lock()
	var candidates []managedRunner
	for _, entry := range p.entries {
		if !entry.jobStarted && !running[entry.containerID] {
			candidates = append(candidates, entry)
		}
	}
	p.mu.Unlock()
	for _, entry := range candidates {
		p.retireExited(ctx, entry)
	}
}

func (p *runnerPool) retireExited(ctx context.Context, entry managedRunner) {
	// The container list in reconcile can predate a just-started runner, so
	// retire only on positive proof that the container has stopped.
	inspected, err := p.docker.ContainerInspect(ctx, entry.containerID)
	exitCode, oomKilled := -1, false
	switch {
	case err == nil && inspected.State != nil:
		if inspected.State.Running || inspected.State.Restarting || inspected.State.Paused {
			return
		}
		exitCode, oomKilled = inspected.State.ExitCode, inspected.State.OOMKilled
		finished, parseErr := time.Parse(time.RFC3339Nano, inspected.State.FinishedAt)
		if parseErr != nil || finished.IsZero() || finished.Year() <= 1 || time.Since(finished) < exitGrace {
			return
		}
	case errdefs.IsNotFound(err):
	default:
		return
	}
	p.mu.Lock()
	current, ok := p.entries[entry.name]
	if !ok || current.jobStarted {
		p.mu.Unlock()
		return
	}
	delete(p.entries, entry.name)
	p.containerExits++
	p.mu.Unlock()
	_ = p.docker.ContainerRemove(context.WithoutCancel(ctx), entry.containerID, container.RemoveOptions{Force: true})
	removeErr := removeRunner(context.WithoutCancel(ctx), p.target, p.state, entry.runnerID, entry.name)
	_ = p.state.update(func(data *runData) { data.Scale.ContainerExits++ })
	_ = p.state.record("runner_container_exited", p.fields(map[string]any{
		"runner_name": entry.name, "exit_code": exitCode, "oom_killed": oomKilled, "remove_error": errString(removeErr),
	}))
	if exitCode != 0 || oomKilled {
		p.mu.Lock()
		live := len(p.entries) + p.starting
		p.mu.Unlock()
		if live > 0 {
			p.lowerCeiling(fmt.Sprintf("runner container exited unexpectedly (exit code %d, OOM killed: %t)", exitCode, oomKilled))
		}
	}
	p.nudge()
}

func (p *runnerPool) reportStatus() {
	p.mu.Lock()
	containers, desired, ceiling, stats, active := len(p.entries), p.desiredLocked(), p.ceiling, p.stats, 0
	for _, entry := range p.entries {
		if entry.jobStarted {
			active++
		}
	}
	p.mu.Unlock()
	line := fmt.Sprintf("containers=%d desired=%d ceiling=%d active_jobs=%d | ghes registered=%d idle=%d busy=%d assigned_jobs=%d running_jobs=%d",
		containers, desired, ceiling, active, stats.TotalRegisteredRunners, stats.TotalIdleRunners, stats.TotalBusyRunners, stats.TotalAssignedJobs, stats.TotalRunningJobs)
	if p.group != "" {
		line = "group=" + p.group + " " + line
	}
	fields := p.fields(map[string]any{
		"containers": containers, "desired": desired, "ceiling": ceiling,
		"registered": stats.TotalRegisteredRunners, "idle": stats.TotalIdleRunners, "busy": stats.TotalBusyRunners,
		"assigned_jobs": stats.TotalAssignedJobs, "running_jobs": stats.TotalRunningJobs,
	})
	if sample, err := readHostSample(p.diskPath); err == nil {
		line += fmt.Sprintf(" | host mem_available=%dMiB load1=%.1f disk_free=%dMiB", sample.memAvailableBytes>>20, sample.load1, sample.diskFreeBytes>>20)
		fields["mem_available_mib"], fields["load1"], fields["disk_free_mib"] = sample.memAvailableBytes>>20, sample.load1, sample.diskFreeBytes>>20
	}
	fmt.Printf("%s %s\n", time.Now().UTC().Format("15:04:05"), line)
	_ = p.state.record("pool_status", fields)
}

func (p *runnerPool) shutdown(ctx context.Context) error {
	p.mu.Lock()
	entries := make([]managedRunner, 0, len(p.entries))
	for _, entry := range p.entries {
		entries = append(entries, entry)
	}
	p.entries = make(map[string]managedRunner)
	p.mu.Unlock()
	var (
		failuresMu sync.Mutex
		failures   []error
		wg         sync.WaitGroup
	)
	fail := func(err error) {
		failuresMu.Lock()
		failures = append(failures, err)
		failuresMu.Unlock()
	}
	slots := make(chan struct{}, teardownConcurrency)
	for _, entry := range entries {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if err := p.docker.ContainerStop(ctx, entry.containerID, container.StopOptions{Timeout: new(int)}); err != nil && !errdefs.IsNotFound(err) {
				fail(fmt.Errorf("stop runner %s: %w", entry.name, err))
				return
			}
			if err := p.docker.ContainerRemove(ctx, entry.containerID, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
				fail(fmt.Errorf("remove runner %s: %w", entry.name, err))
				return
			}
			if err := removeRunner(ctx, p.target, p.state, entry.runnerID, entry.name); err != nil {
				fail(err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(failures...)
}

var _ listener.Scaler = (*runnerPool)(nil)
