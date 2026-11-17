package main

// Fake job execution. In a fleet group with fake_jobs set, every fake-online
// runner accepts one real job over the legacy runner protocol, holds it for
// --fake-job-duration while renewing its lock, reports a canned result
// computed from the job's matrix and inputs, and closes its session. GHES
// then deletes the ephemeral runner and a replacement is registered in its
// place. No container or runner process is started. Job tokens, message
// bodies and JIT configs stay in memory and are never logged.

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

const (
	fakeJobMessageType       = "PipelineAgentJobRequest"
	fakeJobBrokerMessageType = "RunnerJobRequest"
	fakeJobEventsAccept      = "application/json; api-version=2.0-preview.1"
	fakeJobRecordsAccept     = "application/json; api-version=5.1-preview.1"
	fakeJobEmptyLockToken    = "00000000-0000-0000-0000-000000000000"
	fakeJobAttempts          = 3
	fakeJobCallTimeout       = 30 * time.Second
	fakeJobRenewInterval     = 60 * time.Second
	fakeJobCancelGrace       = 10 * time.Second
	// Plans at this version or later finish the job request themselves
	// when the job completes; older plans need an explicit finish.
	fakeJobSelfFinishPlanVersion = 8

	fakeJobSucceeded = "succeeded"
	fakeJobFailed    = "failed"
	fakeJobCanceled  = "canceled"
)

// fakeJobResultCodes are the numeric TaskResult values, used only if the
// server rejects the string form.
var fakeJobResultCodes = map[string]int{fakeJobSucceeded: 0, fakeJobFailed: 2, fakeJobCanceled: 3}

// fakeJobConfig makes a group's fake-online runners execute jobs.
type fakeJobConfig struct {
	duration time.Duration
	// replace registers a runner to take the place of finishedRunner,
	// which GHES deletes once its job completes. It returns
	// errReplacementSkipped when the run is ending.
	replace func(ctx context.Context, finishedRunner string) error
}

var errReplacementSkipped = errors.New("replacement skipped: run ending")

// probeMessage is one message from the runner message queue.
type probeMessage struct {
	MessageID   flexInt `json:"messageId"`
	MessageType string  `json:"messageType"`
	IV          []byte  `json:"iv"`
	Body        string  `json:"body"`
}

type sessionEncryptionKey struct {
	Encrypted flexBool `json:"encrypted"`
	Value     []byte   `json:"value"`
}

// decodeSessionKey returns the AES key that encrypts this session's
// messages, or nil when the server sent none. Like the runner, it uses the
// server's useFipsEncryption reply to choose OAEP-SHA256 over OAEP-SHA1.
func decodeSessionKey(key *rsa.PrivateKey, k *sessionEncryptionKey, fips bool) ([]byte, error) {
	if k == nil || len(k.Value) == 0 {
		return nil, nil
	}
	if !bool(k.Encrypted) {
		return k.Value, nil
	}
	hash := sha1.New()
	if fips {
		hash = sha256.New()
	}
	plain, err := rsa.DecryptOAEP(hash, nil, key, k.Value, nil)
	if err != nil {
		return nil, errors.New("session encryption key could not be decrypted with the runner key")
	}
	return plain, nil
}

// decryptMessageBody returns the plaintext of an AES-CBC encrypted message
// body. A body is plaintext unless both the session key and IV are present.
func decryptMessageBody(key, iv []byte, body string) ([]byte, error) {
	if len(key) == 0 || len(iv) == 0 {
		return []byte(body), nil
	}
	ciphertext, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return nil, errors.New("encrypted message body is not base64")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("session key: %w", err)
	}
	size := block.BlockSize()
	if len(iv) != size {
		return nil, fmt.Errorf("message IV has %d bytes, expected %d", len(iv), size)
	}
	if len(ciphertext) == 0 || len(ciphertext)%size != 0 {
		return nil, fmt.Errorf("encrypted message body has %d bytes, not a whole number of blocks", len(ciphertext))
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plain, ciphertext)
	pad := int(plain[len(plain)-1])
	if pad == 0 || pad > size {
		return nil, errors.New("message body has invalid padding")
	}
	for _, b := range plain[len(plain)-pad:] {
		if int(b) != pad {
			return nil, errors.New("message body has invalid padding")
		}
	}
	return bytes.TrimPrefix(plain[:len(plain)-pad], []byte("\xef\xbb\xbf")), nil
}

// agentJobRequest is the part of a PipelineAgentJobRequest a fake runner
// needs to hold and complete the job.
type agentJobRequest struct {
	RequestID      flexInt `json:"requestId"`
	JobID          string  `json:"jobId"`
	JobName        string  `json:"jobName"`
	JobDisplayName string  `json:"jobDisplayName"`
	Plan           struct {
		ScopeIdentifier string  `json:"scopeIdentifier"`
		PlanType        string  `json:"planType"`
		PlanID          string  `json:"planId"`
		Version         flexInt `json:"version"`
	} `json:"plan"`
	Timeline struct {
		ID string `json:"id"`
	} `json:"timeline"`
	Resources struct {
		Endpoints []struct {
			Name          string `json:"name"`
			URL           string `json:"url"`
			Authorization struct {
				Parameters map[string]string `json:"parameters"`
			} `json:"authorization"`
		} `json:"endpoints"`
	} `json:"resources"`
	ContextData map[string]json.RawMessage `json:"contextData"`
}

func decodeJobMessage(key []byte, msg probeMessage) (*agentJobRequest, error) {
	plain, err := decryptMessageBody(key, msg.IV, msg.Body)
	if err != nil {
		return nil, err
	}
	var job agentJobRequest
	if err := json.Unmarshal(plain, &job); err != nil {
		return nil, fmt.Errorf("job message is not valid JSON: %v", jsonErrorShape(err))
	}
	switch {
	case job.RequestID <= 0:
		return nil, errors.New("job message has no requestId")
	case job.JobID == "":
		return nil, errors.New("job message has no jobId")
	case job.Plan.PlanID == "" || job.Plan.ScopeIdentifier == "" || job.Plan.PlanType == "":
		return nil, errors.New("job message has an incomplete plan reference")
	case job.Timeline.ID == "":
		return nil, errors.New("job message has no timeline")
	}
	return &job, nil
}

// planURL returns the URL of path under the job's plan on its
// SystemVssConnection endpoint, and the job's access token.
func (j *agentJobRequest) planURL(path string) (string, string, error) {
	connURL, token, err := j.systemConnection()
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("%s/%s/_apis/distributedtask/hubs/%s/plans/%s/%s",
		strings.TrimRight(connURL, "/"), url.PathEscape(j.Plan.ScopeIdentifier),
		url.PathEscape(j.Plan.PlanType), url.PathEscape(j.Plan.PlanID), path), token, nil
}

// systemConnection returns the URL and access token of the job's
// SystemVssConnection endpoint, which receives the job's events.
func (j *agentJobRequest) systemConnection() (string, string, error) {
	for _, endpoint := range j.Resources.Endpoints {
		if !strings.EqualFold(endpoint.Name, "SystemVssConnection") {
			continue
		}
		for name, value := range endpoint.Authorization.Parameters {
			if strings.EqualFold(name, "AccessToken") && value != "" && endpoint.URL != "" {
				return endpoint.URL, value, nil
			}
		}
		return "", "", errors.New("job SystemVssConnection endpoint has no URL or access token")
	}
	return "", "", errors.New("job message has no SystemVssConnection endpoint")
}

// pipelineContextData is one encoded PipelineContextData object. Plain JSON
// strings, booleans and numbers encode themselves; anything else is an
// object whose t field selects the type, defaulting to string.
type pipelineContextData struct {
	T *int              `json:"t"`
	S string            `json:"s"`
	B bool              `json:"b"`
	N float64           `json:"n"`
	A []json.RawMessage `json:"a"`
	D []struct {
		K string          `json:"k"`
		V json.RawMessage `json:"v"`
	} `json:"d"`
}

// decodeContextData decodes PipelineContextData into string, bool, float64,
// []any and map[string]any values.
func decodeContextData(raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if raw[0] != '{' {
		var value any
		err := json.Unmarshal(raw, &value)
		return value, err
	}
	var data pipelineContextData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	kind := 0
	if data.T != nil {
		kind = *data.T
	}
	switch kind {
	case 0:
		return data.S, nil
	case 1:
		items := make([]any, 0, len(data.A))
		for _, item := range data.A {
			value, err := decodeContextData(item)
			if err != nil {
				return nil, err
			}
			items = append(items, value)
		}
		return items, nil
	case 2, 5:
		entries := make(map[string]any, len(data.D))
		for _, entry := range data.D {
			value, err := decodeContextData(entry.V)
			if err != nil {
				return nil, err
			}
			entries[entry.K] = value
		}
		return entries, nil
	case 3:
		return data.B, nil
	case 4:
		return data.N, nil
	}
	return nil, fmt.Errorf("unknown context data type %d", kind)
}

// contextLookup follows path through nested dictionaries, matching keys
// case-insensitively like expression property access.
func contextLookup(value any, path ...string) (any, bool) {
	for _, key := range path {
		entries, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		found := false
		for name, entry := range entries {
			if strings.EqualFold(name, key) {
				value, found = entry, true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return value, true
}

func canonicalContextValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return fmt.Sprint(value)
}

// fakeJobFails reproduces the workloads' intentional failure step: a job
// fails when its matrix.shard is listed in the failure_shards input.
func fakeJobFails(contextData map[string]json.RawMessage) bool {
	contexts := map[string]any{}
	for name, raw := range contextData {
		for _, want := range []string{"matrix", "inputs", "github"} {
			if strings.EqualFold(name, want) {
				if value, err := decodeContextData(raw); err == nil {
					contexts[want] = value
				}
			}
		}
	}
	shard, ok := contextLookup(contexts, "matrix", "shard")
	if !ok {
		return false
	}
	failures, ok := contextLookup(contexts, "inputs", "failure_shards")
	if !ok {
		failures, ok = contextLookup(contexts, "github", "event", "inputs", "failure_shards")
	}
	if !ok {
		return false
	}
	var list []any
	switch v := failures.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &list); err != nil {
			return false
		}
	case []any:
		list = v
	default:
		return false
	}
	want := canonicalContextValue(shard)
	for _, item := range list {
		if canonicalContextValue(item) == want {
			return true
		}
	}
	return false
}

// poolURL resolves path under the runner's legacy agent pool API.
func (r *probeRunner) poolURL(path string) (string, error) {
	base := strings.TrimRight(r.jit.settings.ServerURL, "/") + "/"
	return resolveProbeURL(base, fmt.Sprintf("_apis/distributedtask/pools/%d/%s", r.jit.settings.PoolID, path))
}

// jobCall makes one job-protocol request with up to fakeJobAttempts tries,
// retrying transport errors, 429 and 5xx. bearer authorizes the call; empty
// uses the runner's OAuth token, which a 401 refreshes. Any other status is
// returned for the caller to judge.
func (r *probeRunner) jobCall(ctx context.Context, what, method, target, accept, bearer string, body any) (int, []byte, error) {
	var status int
	var data []byte
	var err error
	for attempt := 0; attempt < fakeJobAttempts; attempt++ {
		if attempt > 0 {
			sleepCtx(ctx, time.Duration(attempt)*time.Second)
		}
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		token := bearer
		if token == "" {
			token = r.token
		}
		callCtx, cancel := context.WithTimeout(ctx, fakeJobCallTimeout)
		status, data, err = r.http.do(callCtx, method, target, token, accept, body)
		cancel()
		switch {
		case err != nil:
			continue
		case status == http.StatusUnauthorized && bearer == "":
			if refreshErr := r.refreshToken(ctx); refreshErr != nil {
				err = refreshErr
			}
			continue
		case status == http.StatusTooManyRequests || status >= 500:
			continue
		}
		return status, data, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("%s: %w", what, err)
	}
	return status, data, nil
}

func httpStatusError(what string, status int, data []byte) error {
	return errors.New(redactErrorText(fmt.Sprintf("%s returned HTTP %d: %s", what, status, snippet(data))))
}

func (r *probeRunner) deleteMessage(ctx context.Context, messageID int64) error {
	target, err := r.poolURL("messages/" + strconv.FormatInt(messageID, 10))
	if err != nil {
		return err
	}
	target += "?" + url.Values{"sessionId": {r.sessionID}}.Encode()
	_, _, accept, err := r.urls()
	if err != nil {
		return err
	}
	status, data, err := r.jobCall(ctx, "delete job message", http.MethodDelete, target, accept, "", nil)
	switch {
	case err != nil:
		return err
	case status == http.StatusOK || status == http.StatusNoContent || status == http.StatusNotFound:
		return nil
	}
	return httpStatusError("delete job message", status, data)
}

func (r *probeRunner) jobRequestURL(job *agentJobRequest) (string, error) {
	target, err := r.poolURL("jobrequests/" + strconv.FormatInt(int64(job.RequestID), 10))
	if err != nil {
		return "", err
	}
	return target + "?" + url.Values{"lockToken": {fakeJobEmptyLockToken}}.Encode(), nil
}

// renewJob extends the job request's lock. gone reports a request the
// server no longer has, which means the job was cancelled.
func (r *probeRunner) renewJob(ctx context.Context, job *agentJobRequest) (gone bool, err error) {
	target, err := r.jobRequestURL(job)
	if err != nil {
		return false, err
	}
	_, _, accept, err := r.urls()
	if err != nil {
		return false, err
	}
	status, data, err := r.jobCall(ctx, "renew job request", http.MethodPatch, target, accept, "", map[string]any{"requestId": int64(job.RequestID)})
	switch {
	case err != nil:
		return false, err
	case status == http.StatusOK:
		return false, nil
	case status == http.StatusNotFound:
		return true, nil
	}
	return false, httpStatusError("renew job request", status, data)
}

// holdJob renews the job's lock until its duration passes. canceled reports
// a job that ctx or the server cancelled first.
func (r *probeRunner) holdJob(ctx context.Context, job *agentJobRequest) (canceled bool, err error) {
	deadline := time.Now().Add(r.jobs.duration)
	for {
		gone, err := r.renewJob(ctx, job)
		switch {
		case ctx.Err() != nil || gone:
			return true, nil
		case err != nil:
			return false, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		sleepCtx(ctx, min(remaining, fakeJobRenewInterval))
		if ctx.Err() != nil {
			return true, nil
		}
		if time.Until(deadline) <= 0 {
			return false, nil
		}
	}
}

// updateJobRecord updates the job's timeline record as the runner does.
// GHES takes the job's status, timing and conclusion from this record; a
// job completed without it is reported as skipped.
func (r *probeRunner) updateJobRecord(ctx context.Context, job *agentJobRequest, state, result string, at time.Time) error {
	target, token, err := job.planURL("timelines/" + url.PathEscape(job.Timeline.ID) + "/records")
	if err != nil {
		return err
	}
	record := map[string]any{
		"id": job.JobID, "recordType": "Job", "state": state,
		"workerName": r.name, "agentPlatform": probeOS,
	}
	if job.JobDisplayName != "" {
		record["name"] = job.JobDisplayName
	}
	if job.JobName != "" {
		record["refName"] = job.JobName
	}
	stamp := at.UTC().Format(time.RFC3339Nano)
	if result == "" {
		record["startTime"] = stamp
	} else {
		record["result"] = result
		record["finishTime"] = stamp
	}
	body := map[string]any{"count": 1, "value": []any{record}}
	status, data, err := r.jobCall(ctx, "update job timeline record", http.MethodPatch, target, fakeJobRecordsAccept, token, body)
	switch {
	case err != nil:
		return err
	case status != http.StatusOK:
		return httpStatusError("update job timeline record", status, data)
	}
	return nil
}

// completeJob records the job's result on its timeline, raises the
// JobCompleted plan event with the job's access token and, for plans that
// do not finish the request themselves, finishes the job request.
func (r *probeRunner) completeJob(ctx context.Context, job *agentJobRequest, result string) error {
	if err := r.updateJobRecord(ctx, job, "completed", result, time.Now()); err != nil {
		return err
	}
	target, token, err := job.planURL("events")
	if err != nil {
		return err
	}
	event := map[string]any{"name": "JobCompleted", "jobId": job.JobID, "requestId": int64(job.RequestID), "result": result}
	status, data, err := r.jobCall(ctx, "raise JobCompleted", http.MethodPost, target, fakeJobEventsAccept, token, event)
	if err == nil && status == http.StatusBadRequest {
		event["result"] = fakeJobResultCodes[result]
		status, data, err = r.jobCall(ctx, "raise JobCompleted", http.MethodPost, target, fakeJobEventsAccept, token, event)
	}
	switch {
	case err != nil:
		return err
	case status != http.StatusOK && status != http.StatusNoContent:
		return httpStatusError("raise JobCompleted", status, data)
	}
	if int64(job.Plan.Version) >= fakeJobSelfFinishPlanVersion {
		return nil
	}
	finishURL, err := r.jobRequestURL(job)
	if err != nil {
		return err
	}
	_, _, accept, err := r.urls()
	if err != nil {
		return err
	}
	finish := map[string]any{"requestId": int64(job.RequestID), "finishTime": time.Now().UTC().Format(time.RFC3339Nano), "result": result}
	status, data, err = r.jobCall(ctx, "finish job request", http.MethodPatch, finishURL, accept, "", finish)
	switch {
	case err != nil:
		return err
	case status != http.StatusOK:
		return httpStatusError("finish job request", status, data)
	}
	return nil
}

// runFakeJob accepts, holds and completes one job and returns its result.
// It closes the runner's session before completing the job, while the
// runner still exists: GHES deletes an ephemeral runner once its job
// completes, after which the session can no longer be deleted.
func (r *probeRunner) runFakeJob(ctx context.Context, msg probeMessage) (string, error) {
	sessionEnded := false
	endSession := func() {
		if !sessionEnded {
			sessionEnded = true
			r.endJobSession(ctx)
		}
	}
	defer endSession()

	job, decodeErr := decodeJobMessage(r.sessionKey, msg)
	if decodeErr == nil {
		_, _, decodeErr = job.systemConnection()
	}
	if err := r.deleteMessage(ctx, int64(msg.MessageID)); err != nil && ctx.Err() == nil {
		return "", errors.Join(decodeErr, err)
	}
	if decodeErr != nil {
		return "", fmt.Errorf("decode job message: %w", decodeErr)
	}
	result := fakeJobSucceeded
	if fakeJobFails(job.ContextData) {
		result = fakeJobFailed
	}
	// A cancelled ctx makes holdJob report the job as canceled.
	if err := r.updateJobRecord(ctx, job, "inProgress", "", time.Now()); err != nil && ctx.Err() == nil {
		return "", err
	}
	canceled, err := r.holdJob(ctx, job)
	if err != nil {
		return "", err
	}
	completeCtx := ctx
	if canceled {
		result = fakeJobCanceled
		var cancel context.CancelFunc
		completeCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), fakeJobCancelGrace)
		defer cancel()
	}
	endSession()
	if err := r.completeJob(completeCtx, job, result); err != nil {
		if canceled {
			// Completing a cancelled job is best-effort; GHES may already
			// have finished it.
			return result, nil
		}
		return "", err
	}
	return result, nil
}

// handleJob runs one job, which ends the session and, unless the job failed
// or the run is ending, registers a replacement runner. The ephemeral
// runner is then deleted by GHES, so poll must return afterwards.
func (r *probeRunner) handleJob(ctx context.Context, msg probeMessage) {
	c := r.counters
	c.jobsReceived.Add(1)
	r.setStage("job_running")
	result, err := r.runFakeJob(ctx, msg)
	switch {
	case err != nil:
		c.jobErrors.Add(1)
		r.jobFatal(fmt.Errorf("fake runner %s in group %s could not run a job: %s", r.name, c.group, safeError(err)))
	case result == fakeJobSucceeded:
		c.jobsSucceeded.Add(1)
	case result == fakeJobFailed:
		c.jobsFailed.Add(1)
	default:
		c.jobsCanceled.Add(1)
	}
	r.setStage("job_done")
	if err != nil || ctx.Err() != nil || r.jobs.replace == nil {
		return
	}
	if err := r.jobs.replace(ctx, r.name); err != nil {
		if ctx.Err() != nil || errors.Is(err, errReplacementSkipped) {
			return
		}
		c.replaceErrors.Add(1)
		r.jobFatal(fmt.Errorf("replace fake runner %s in group %s: %s", r.name, c.group, safeError(err)))
		return
	}
	c.replacements.Add(1)
}

// endJobSession closes the session of a runner that took a job.
func (r *probeRunner) endJobSession(ctx context.Context) {
	c := r.counters
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fakeJobCallTimeout)
	defer cancel()
	if err := r.closeSession(closeCtx); err != nil {
		// closeAll retries the session, which keeps its sessionID.
		c.closeErrors.Add(1)
		c.setError(safeError(err))
		return
	}
	c.open.Add(-1)
	c.closed.Add(1)
}

func (r *probeRunner) jobFatal(err error) {
	r.fail(err)
	r.counters.setError(err.Error())
	if r.fatal != nil {
		r.fatal(err)
	}
}

// fakeJobListener acquires the jobs offered to one group's fake scale set,
// so GHES assigns them to the group's fake-online runners.
type fakeJobListener struct {
	Group   string
	session *scaleset.MessageSessionClient
	stopRun context.CancelFunc
	done    chan error
	exited  chan struct{}
	stopped atomic.Bool
}

type fakeJobScaler struct {
	session *scaleset.MessageSessionClient
}

func (s fakeJobScaler) Scale(ctx context.Context, message *scaleset.RunnerScaleSetMessage) error {
	if message == nil || len(message.JobAvailableMessages) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(message.JobAvailableMessages))
	for _, job := range message.JobAvailableMessages {
		ids = append(ids, job.RunnerRequestID)
	}
	if _, err := s.session.AcquireJobs(ctx, ids); err != nil {
		return fmt.Errorf("acquire available jobs: %w", err)
	}
	return nil
}

// startFakeJobListener opens a scale-set message session for a group's fake
// scale set and acquires every job offered to it until stopped.
func startFakeJobListener(ctx context.Context, client *scaleset.Client, scaleSetID, maxRunners int, owner, group string) (*fakeJobListener, error) {
	session, err := client.MessageSessionClient(ctx, scaleSetID, owner)
	if err != nil {
		return nil, groupError(group, fmt.Errorf("create fake scale-set message session: %w", err))
	}
	jobListener, err := listener.New(session, listener.Config{ScaleSetID: scaleSetID, MaxRunners: max(maxRunners, 1)})
	if err != nil {
		return nil, errors.Join(groupError(group, fmt.Errorf("create fake scale-set listener: %w", err)), session.Close(context.Background()))
	}
	runCtx, stopRun := context.WithCancel(context.Background())
	l := &fakeJobListener{Group: group, session: session, stopRun: stopRun, done: make(chan error, 1), exited: make(chan struct{})}
	go func() {
		l.done <- jobListener.Run(runCtx, fakeJobScaler{session: session})
		close(l.exited)
	}()
	return l, nil
}

// stop ends the listener and closes its session. It is safe to call twice.
func (l *fakeJobListener) stop() error {
	if l == nil || !l.stopped.CompareAndSwap(false, true) {
		return nil
	}
	l.stopRun()
	var errs []error
	select {
	case err := <-l.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, groupError(l.Group, fmt.Errorf("fake scale-set listener stopped: %w", err)))
		}
	case <-time.After(10 * time.Second):
		errs = append(errs, groupError(l.Group, errors.New("fake scale-set listener did not stop within 10 seconds")))
	}
	if err := l.session.Close(context.Background()); err != nil {
		errs = append(errs, groupError(l.Group, fmt.Errorf("close fake scale-set session: %w", err)))
	}
	return errors.Join(errs...)
}
