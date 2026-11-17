package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type runnerRecord struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Removed   bool      `json:"removed"`
}

type workflowRecord struct {
	ID               int64     `json:"id"`
	Marker           string    `json:"marker"`
	Workload         string    `json:"workload,omitempty"`
	Shards           int       `json:"shards,omitempty"`
	Jobs             int       `json:"jobs"`
	CreatedAt        time.Time `json:"created_at"`
	Status           string    `json:"status"`
	Conclusion       string    `json:"conclusion,omitempty"`
	ExpectedFailures int       `json:"expected_failures"`
	CompletedSeenAt  time.Time `json:"completed_seen_at,omitzero"`
}

func (r workflowRecord) workload() string {
	if r.Workload == "" {
		return workloadSynthetic
	}
	return r.Workload
}

func (r workflowRecord) shards() int {
	if r.Shards > 0 {
		return r.Shards
	}
	return r.Jobs
}

type runData struct {
	SchemaVersion       int              `json:"schema_version"`
	RunID               string           `json:"run_id"`
	Mode                string           `json:"mode"`
	Workload            string           `json:"workload,omitempty"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
	APIHost             string           `json:"api_host"`
	Owner               string           `json:"owner"`
	Repo                string           `json:"repo"`
	ScaleSetID          int              `json:"scale_set_id,omitempty"`
	Runners             []runnerRecord   `json:"runners,omitempty"`
	ContainerIDs        []string         `json:"container_ids,omitempty"`
	ScaleSetRemoved     bool             `json:"scale_set_removed,omitempty"`
	Workflows           []workflowRecord `json:"workflows,omitempty"`
	Attempts            int              `json:"attempts"`
	Accepted            int              `json:"accepted"`
	Completed           int              `json:"completed"`
	Passed              int              `json:"passed"`
	IntentionalFailures int              `json:"intentional_failures"`
	Failed              int              `json:"failed"`
	Unresolved          int              `json:"unresolved"`
	Status              string           `json:"status"`
	CleanupStatus       string           `json:"cleanup_status"`
	LastError           string           `json:"last_error,omitempty"`
}

type runState struct {
	mu   sync.Mutex
	dir  string
	data runData
	log  *os.File
}

func newRunID() (string, error) {
	random := make([]byte, 5)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate run ID: %w", err)
	}
	return "actions-" + time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random), nil
}

func createRunState(root, id, mode string, t *target) (*runState, error) {
	if !safeRunID.MatchString(id) {
		return nil, errors.New("invalid run ID")
	}
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create artifacts root: %w", err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect artifacts root: %w", err)
	}
	if rootInfo.Mode().Perm()&0077 != 0 {
		return nil, errors.New("artifacts root must not be accessible to group or other users")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, fmt.Errorf("create run directory: %w", err)
	}
	journal, err := os.OpenFile(filepath.Join(dir, "journal.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("create run journal: %w", err)
	}
	s := &runState{
		dir: dir,
		log: journal,
		data: runData{
			SchemaVersion: 1,
			RunID:         id,
			Mode:          mode,
			CreatedAt:     time.Now().UTC(),
			UpdatedAt:     time.Now().UTC(),
			APIHost:       t.api.base,
			Owner:         t.cfg.Owner,
			Repo:          t.cfg.Repo,
			Status:        "running",
			CleanupStatus: "pending",
		},
	}
	if err := s.record("run_started", map[string]any{"mode": mode}); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func openRunState(root, id string, t *target) (*runState, error) {
	dir := filepath.Join(root, id)
	stateBytes, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return nil, fmt.Errorf("read run state: %w", err)
	}
	var data runData
	if err := json.Unmarshal(stateBytes, &data); err != nil {
		return nil, fmt.Errorf("decode run state: %w", err)
	}
	if data.SchemaVersion != 1 || data.RunID != id {
		return nil, errors.New("run state version or identifier does not match")
	}
	if data.APIHost != t.api.base || data.Owner != t.cfg.Owner || data.Repo != t.cfg.Repo {
		return nil, errors.New("run state target does not match the configured GHES host and repository")
	}
	journal, err := os.OpenFile(filepath.Join(dir, "journal.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open run journal: %w", err)
	}
	return &runState{dir: dir, data: data, log: journal}, nil
}

func (s *runState) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		_ = s.log.Close()
		s.log = nil
	}
}

func (s *runState) record(event string, fields map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	line := struct {
		Time   time.Time      `json:"time"`
		Event  string         `json:"event"`
		Fields map[string]any `json:"fields,omitempty"`
	}{time.Now().UTC(), event, fields}
	if err := json.NewEncoder(s.log).Encode(line); err != nil {
		return fmt.Errorf("write journal event %q: %w", event, err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("sync journal event %q: %w", event, err)
	}
	s.data.UpdatedAt = time.Now().UTC()
	return s.saveLocked()
}

func (s *runState) update(fn func(*runData)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.data)
	s.data.UpdatedAt = time.Now().UTC()
	return s.saveLocked()
}

func (s *runState) saveLocked() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run state: %w", err)
	}
	tmp, err := os.CreateTemp(s.dir, ".state-*.json")
	if err != nil {
		return fmt.Errorf("create state snapshot: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect state snapshot: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write state snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync state snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close state snapshot: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, "state.json")); err != nil {
		return fmt.Errorf("replace state snapshot: %w", err)
	}
	return nil
}

func (s *runState) snapshot() runData {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.data
	result.Runners = append([]runnerRecord(nil), s.data.Runners...)
	result.ContainerIDs = append([]string(nil), s.data.ContainerIDs...)
	result.Workflows = append([]workflowRecord(nil), s.data.Workflows...)
	return result
}

func (s *runState) reportPath() string {
	return filepath.Join(s.dir, "report.json")
}

func (s *runState) writeReport() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return writeAtomic(filepath.Join(s.dir, "report.json"), data, 0600)
}

func (s *runState) writeHeartbeat() error {
	payload, err := json.Marshal(struct {
		RunID string    `json:"run_id"`
		Time  time.Time `json:"time"`
	}{s.data.RunID, time.Now().UTC()})
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(s.dir, "heartbeat.json"), payload, 0600)
}

func (s *runState) heartbeat(ctx context.Context, done chan<- error) {
	defer close(done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.writeHeartbeat(); err != nil {
			done <- fmt.Errorf("heartbeat write failed; container guard will stop the run: %w", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *runState) preview(w io.Writer) error {
	data := s.snapshot()
	fmt.Fprintf(w, "Run %s (%s) for %s/%s on %s\n", data.RunID, data.Mode, data.Owner, data.Repo, data.APIHost)
	fmt.Fprintf(w, "  runner scale set ID: %d\n  runner IDs: %v\n  container IDs: %v\n  workflow run IDs:", data.ScaleSetID, runnerIDs(data.Runners), data.ContainerIDs)
	for _, workflow := range data.Workflows {
		fmt.Fprintf(w, " %d", workflow.ID)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Preview only. Re-run with --apply to cancel/remove only these journaled resources.")
	return nil
}

func runnerIDs(runners []runnerRecord) []int {
	ids := make([]int, 0, len(runners))
	for _, runner := range runners {
		if !runner.Removed {
			ids = append(ids, runner.ID)
		}
	}
	return ids
}

func writeAtomic(name string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(name)
	tmp, err := os.CreateTemp(dir, ".write-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, name)
}
