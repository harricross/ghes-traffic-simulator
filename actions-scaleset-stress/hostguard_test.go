package main

import (
	"strings"
	"testing"
)

func TestParseMemAvailableAndLoadAvg(t *testing.T) {
	meminfo := "MemTotal:       16384000 kB\nMemFree:          500000 kB\nMemAvailable:    8192000 kB\n"
	got, err := parseMemAvailable(strings.NewReader(meminfo))
	if err != nil || got != 8192000*1024 {
		t.Fatalf("parseMemAvailable = %d, %v", got, err)
	}
	if _, err := parseMemAvailable(strings.NewReader("MemTotal: 1 kB\n")); err == nil {
		t.Fatal("missing MemAvailable must fail")
	}
	load, err := parseLoadAvg("1.25 0.80 0.50 2/300 12345\n")
	if err != nil || load != 1.25 {
		t.Fatalf("parseLoadAvg = %v, %v", load, err)
	}
	if _, err := parseLoadAvg(""); err == nil {
		t.Fatal("empty load average must fail")
	}
}

func TestHostLimitsEvaluate(t *testing.T) {
	limits := hostLimits{
		minFreeMemBytes:    2 << 30,
		minFreeDiskBytes:   10 << 30,
		maxLoadPerCPU:      3,
		workerReserveBytes: 256 << 20,
		maxBusyRunners:     8,
	}
	healthy := hostSample{memAvailableBytes: 12 << 30, diskFreeBytes: 50 << 30, load1: 2, cpus: 4}
	cases := []struct {
		name   string
		sample func(hostSample) hostSample
		live   int
		want   int
	}{
		{"healthy", func(s hostSample) hostSample { return s }, 10, hostOK},
		{"memory below floor", func(s hostSample) hostSample { s.memAvailableBytes = 2 << 30; return s }, 0, hostCeiling},
		{"worker reserve counted for busy runners", func(s hostSample) hostSample { s.memAvailableBytes = 3 << 30; return s }, 100, hostCeiling},
		{"worker reserve capped at max busy", func(s hostSample) hostSample { s.memAvailableBytes = 5 << 30; return s }, 1000, hostOK},
		{"disk below floor", func(s hostSample) hostSample { s.diskFreeBytes = 1 << 30; return s }, 0, hostCeiling},
		{"load pauses", func(s hostSample) hostSample { s.load1 = 13; return s }, 0, hostPause},
		{"memory outranks load", func(s hostSample) hostSample { s.load1 = 13; s.memAvailableBytes = 1 << 30; return s }, 0, hostCeiling},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := limits.evaluate(tc.sample(healthy), tc.live)
			if got != tc.want {
				t.Fatalf("evaluate = %d (%s), want %d", got, reason, tc.want)
			}
			if got != hostOK && reason == "" {
				t.Fatal("a non-OK verdict must carry a reason")
			}
		})
	}
}

func newTestPool(t *testing.T, maxRunners, standing int) *runnerPool {
	t.Helper()
	state := &runState{dir: t.TempDir()}
	state.data.RunID = "test-run"
	return &runnerPool{
		state:      state,
		maxRunners: maxRunners,
		standing:   standing,
		entries:    make(map[string]managedRunner),
		wake:       make(chan struct{}, 1),
		fatal:      make(chan error, 1),
	}
}

func TestRunnerPoolDesiredAndCeiling(t *testing.T) {
	pool := newTestPool(t, 50, 40)
	pool.assigned = 5
	if got := pool.desiredLocked(); got != 45 {
		t.Fatalf("desired = %d, want standing plus assigned = 45", got)
	}
	pool.assigned = 30
	if got := pool.desiredLocked(); got != 50 {
		t.Fatalf("desired = %d, want max-runners cap 50", got)
	}
	for i := 0; i < 12; i++ {
		pool.entries[string(rune('a'+i))] = managedRunner{}
	}
	pool.lowerCeiling("test limit")
	if pool.ceiling != 12 || pool.desiredLocked() != 12 {
		t.Fatalf("ceiling = %d desired = %d, want both 12", pool.ceiling, pool.desiredLocked())
	}
	delete(pool.entries, "a")
	pool.lowerCeiling("later limit")
	if pool.ceiling != 11 {
		t.Fatalf("ceiling = %d, want it to lower to 11", pool.ceiling)
	}
	pool.entries["z"] = managedRunner{}
	pool.entries["y"] = managedRunner{}
	pool.lowerCeiling("higher count")
	if pool.ceiling != 11 {
		t.Fatalf("ceiling = %d, want it never to rise", pool.ceiling)
	}
}

func TestRunnerPoolEmptyCeilingIsFatal(t *testing.T) {
	pool := newTestPool(t, 10, 5)
	pool.lowerCeiling("nothing starts")
	select {
	case err := <-pool.fatal:
		if !strings.Contains(err.Error(), "nothing starts") {
			t.Fatalf("unexpected fatal error: %v", err)
		}
	default:
		t.Fatal("a pool that cannot start any runner must report a fatal error")
	}
}

func TestRunnerPoolReserveAndRampComplete(t *testing.T) {
	pool := newTestPool(t, 10, 2)
	if pool.rampComplete() {
		t.Fatal("ramp must not be complete with no runners")
	}
	if !pool.reserve() || !pool.reserve() {
		t.Fatal("pool below target must hand out reservations")
	}
	if pool.reserve() {
		t.Fatal("pool at target must not hand out another reservation")
	}
	pool.release()
	pool.release()
	pool.entries["a"] = managedRunner{}
	pool.entries["b"] = managedRunner{}
	if !pool.rampComplete() {
		t.Fatal("ramp must be complete once standing runners exist and none are starting")
	}
	pool.starting = 1
	if pool.rampComplete() {
		t.Fatal("ramp must wait for in-flight starts")
	}
}

func TestPrintScaleSummary(t *testing.T) {
	var out strings.Builder
	printScaleSummary(&out, "registrations", scaleResults{PeakHeld: 900, HeldVerified: 880, HeldMissing: 20})
	if !strings.Contains(out.String(), "registered-only") || !strings.Contains(out.String(), "peak 900") {
		t.Fatalf("registration summary = %q", out.String())
	}
	out.Reset()
	printScaleSummary(&out, "workflows", scaleResults{StandingTarget: 100, PeakContainers: 80, CeilingRunners: 80, CeilingReason: "memory"})
	if !strings.Contains(out.String(), "peak containers 80") || !strings.Contains(out.String(), "ceiling: 80 runners (memory)") {
		t.Fatalf("workflow summary = %q", out.String())
	}
}
