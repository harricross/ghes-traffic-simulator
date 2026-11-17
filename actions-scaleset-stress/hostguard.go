package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// hostLimits describe the resource floor that runner provisioning must not
// cross on the machine hosting the runner containers.
type hostLimits struct {
	minFreeMemBytes  uint64
	minFreeDiskBytes uint64
	maxLoadPerCPU    float64
	// workerReserveBytes is held back for each runner that may be busy at the
	// same time (at most maxBusyRunners) on top of its idle listener, so that
	// a fully loaded pool does not exhaust memory.
	workerReserveBytes uint64
	maxBusyRunners     int
}

type hostSample struct {
	memAvailableBytes uint64
	diskFreeBytes     uint64
	load1             float64
	cpus              int
}

const (
	hostOK = iota
	hostPause
	hostCeiling
)

// evaluate returns hostOK, hostPause (transient load) or hostCeiling (memory
// or disk exhaustion) for a prospective runner count, with the reason.
func (l hostLimits) evaluate(sample hostSample, liveRunners int) (int, string) {
	busy := min(liveRunners+1, l.maxBusyRunners)
	needed := l.minFreeMemBytes + l.workerReserveBytes*uint64(busy)
	if sample.memAvailableBytes < needed {
		return hostCeiling, fmt.Sprintf("available memory %d MiB is below the %d MiB floor plus worker reserve for %d concurrently busy runners",
			sample.memAvailableBytes>>20, needed>>20, busy)
	}
	if sample.diskFreeBytes < l.minFreeDiskBytes {
		return hostCeiling, fmt.Sprintf("free disk %d MiB is below the %d MiB floor", sample.diskFreeBytes>>20, l.minFreeDiskBytes>>20)
	}
	if l.maxLoadPerCPU > 0 && sample.cpus > 0 && sample.load1 > l.maxLoadPerCPU*float64(sample.cpus) {
		return hostPause, fmt.Sprintf("1-minute load %.1f exceeds %.1f per CPU on %d CPUs", sample.load1, l.maxLoadPerCPU, sample.cpus)
	}
	return hostOK, ""
}

func readHostSample(diskPath string) (hostSample, error) {
	var sample hostSample
	meminfo, err := os.Open("/proc/meminfo")
	if err != nil {
		return sample, fmt.Errorf("host resource guard requires Linux /proc: %w", err)
	}
	defer meminfo.Close()
	sample.memAvailableBytes, err = parseMemAvailable(meminfo)
	if err != nil {
		return sample, err
	}
	loadavg, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return sample, fmt.Errorf("read /proc/loadavg: %w", err)
	}
	sample.load1, err = parseLoadAvg(string(loadavg))
	if err != nil {
		return sample, err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(diskPath, &fs); err != nil {
		return sample, fmt.Errorf("statfs %s: %w", diskPath, err)
	}
	sample.diskFreeBytes = uint64(fs.Bavail) * uint64(fs.Bsize)
	sample.cpus = runtime.NumCPU()
	return sample, nil
}

func parseMemAvailable(r io.Reader) (uint64, error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			kib, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse MemAvailable: %w", err)
			}
			return kib * 1024, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("MemAvailable not found in /proc/meminfo")
}

func parseLoadAvg(text string) (float64, error) {
	fields := strings.Fields(text)
	if len(fields) < 1 {
		return 0, errors.New("empty /proc/loadavg")
	}
	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, fmt.Errorf("parse load average: %w", err)
	}
	return load, nil
}
