package main

// Protocol-only runner sessions. A probeRunner turns a JIT config into an
// open runner session that GHES shows as idle, without starting the runner.
// It only long-polls: any message it receives is counted by type and never
// acted upon, so it can't take a job. JIT configs, tokens and RSA parameters
// stay in memory and are never logged or journaled.

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	probeOS   = "Linux"
	probeArch = "X64"

	sessionPollTimeout = 75 * time.Second
	sessionRetryDelay  = 5 * time.Second
	// sessionMessageBackoff paces polling after an unexpected message, which
	// the server may redeliver because a fake runner never handles it.
	sessionMessageBackoff = 5 * time.Second
)

type flexInt int64

func (v *flexInt) UnmarshalJSON(b []byte) error {
	n, err := strconv.ParseInt(strings.Trim(string(b), `"`), 10, 64)
	if err != nil {
		return err
	}
	*v = flexInt(n)
	return nil
}

type flexBool bool

func (v *flexBool) UnmarshalJSON(b []byte) error {
	parsed, err := strconv.ParseBool(strings.ToLower(strings.Trim(string(b), `"`)))
	if err != nil {
		return err
	}
	*v = flexBool(parsed)
	return nil
}

// The runner serializes some numeric and boolean settings as JSON strings.
type probeRunnerSettings struct {
	AgentID            flexInt  `json:"AgentId"`
	AgentName          string   `json:"AgentName"`
	PoolID             flexInt  `json:"PoolId"`
	ServerURL          string   `json:"ServerUrl"`
	ServerURLV2        string   `json:"ServerUrlV2"`
	UseV2Flow          flexBool `json:"UseV2Flow"`
	UseRunnerAdminFlow flexBool `json:"UseRunnerAdminFlow"`
	GitHubURL          string   `json:"GitHubUrl"`
	Ephemeral          flexBool `json:"Ephemeral"`
	DisableUpdate      flexBool `json:"DisableUpdate"`
	IsHostedServer     flexBool `json:"IsHostedServer"`
}

type probeCredentials struct {
	Scheme string `json:"Scheme"`
	Data   struct {
		ClientID           string `json:"clientId"`
		AuthorizationURL   string `json:"authorizationUrl"`
		AuthorizationURLV2 string `json:"authorizationUrlV2"`
		OAuthEndpointURL   string `json:"oauthEndpointUrl"`
	} `json:"Data"`
}

type probeRSAParams struct {
	Exponent []byte `json:"exponent"`
	Modulus  []byte `json:"modulus"`
	D        []byte `json:"d"`
	P        []byte `json:"p"`
	Q        []byte `json:"q"`
}

type probeJIT struct {
	settings probeRunnerSettings
	creds    probeCredentials
	key      *rsa.PrivateKey
	files    []string
}

func jsonErrorShape(err error) string {
	var typeErr *json.UnmarshalTypeError
	var syntaxErr *json.SyntaxError
	switch {
	case errors.As(err, &typeErr):
		return fmt.Sprintf("field %q holds a JSON %s, expected %s", typeErr.Field, typeErr.Value, typeErr.Type)
	case errors.As(err, &syntaxErr):
		return fmt.Sprintf("syntax error at offset %d", syntaxErr.Offset)
	default:
		return fmt.Sprintf("%T", err)
	}
}

func decodeProbeJIT(encoded string) (*probeJIT, error) {
	outer, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("JIT config is not base64")
	}
	var files map[string]string
	if err := json.Unmarshal(outer, &files); err != nil {
		return nil, errors.New("JIT config is not a JSON file map")
	}
	read := func(name string, into any) error {
		b64, ok := files[name]
		if !ok {
			return fmt.Errorf("JIT config has no %s file", name)
		}
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return fmt.Errorf("%s is not base64", name)
		}
		data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
		if err := json.Unmarshal(data, into); err != nil {
			return fmt.Errorf("%s is not valid JSON (%d bytes, leading bytes %x): %v", name, len(data), data[:min(len(data), 2)], jsonErrorShape(err))
		}
		return nil
	}
	jit := &probeJIT{}
	for name := range files {
		jit.files = append(jit.files, name)
	}
	sort.Strings(jit.files)
	var params probeRSAParams
	if err := read(".runner", &jit.settings); err != nil {
		return nil, err
	}
	if err := read(".credentials", &jit.creds); err != nil {
		return nil, err
	}
	if err := read(".credentials_rsaparams", &params); err != nil {
		return nil, err
	}
	key := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{N: new(big.Int).SetBytes(params.Modulus), E: int(new(big.Int).SetBytes(params.Exponent).Int64())},
		D:         new(big.Int).SetBytes(params.D),
		Primes:    []*big.Int{new(big.Int).SetBytes(params.P), new(big.Int).SetBytes(params.Q)},
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("RSA key from JIT config is invalid: %w", err)
	}
	key.Precompute()
	jit.key = key
	return jit, nil
}

func probeGUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func signProbeAssertion(key *rsa.PrivateKey, clientID, audience string) (string, error) {
	now := time.Now().Unix()
	claims, err := json.Marshal(map[string]any{
		"iss": clientID, "sub": clientID, "aud": audience,
		"jti": probeGUID(), "nbf": now, "exp": now + 300,
	})
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	input := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + enc.EncodeToString(claims)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return input + "." + enc.EncodeToString(sig), nil
}

type probeHTTP struct {
	client *http.Client
}

func (p *probeHTTP) do(ctx context.Context, method, target, token, accept string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", accept)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, data, err
}

func snippet(data []byte) string {
	text := strings.Join(strings.Fields(string(data)), " ")
	if len(text) > 240 {
		text = text[:240] + "..."
	}
	return text
}

func (p *probeHTTP) oauthToken(ctx context.Context, jit *probeJIT, v2 bool) (string, error) {
	audience := jit.creds.Data.AuthorizationURL
	if v2 && jit.creds.Data.AuthorizationURLV2 != "" {
		audience = jit.creds.Data.AuthorizationURLV2
	}
	endpoint := jit.creds.Data.OAuthEndpointURL
	if endpoint == "" {
		endpoint = audience
	}
	assertion, err := signProbeAssertion(jit.key, jit.creds.Data.ClientID, audience)
	if err != nil {
		return "", err
	}
	form := url.Values{
		"grant_type":            {"client_credentials"},
		"client_assertion_type": {"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
		"client_assertion":      {assertion},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oauth token request returned HTTP %d: %s", resp.StatusCode, snippet(data))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(data, &out); err != nil || out.AccessToken == "" {
		return "", errors.New("oauth response has no access_token")
	}
	return out.AccessToken, nil
}

func resolveProbeURL(base, ref string) (string, error) {
	b, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	r, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	return b.ResolveReference(r).String(), nil
}

type probeRunner struct {
	name  string
	id    int
	jit   *probeJIT
	http  *probeHTTP
	owner string
	ver   string
	// counters, when set, aggregates this session into its group's stats.
	counters *sessionCounters
	// messageBackoff pauses polling after a message; zero polls again at once.
	messageBackoff time.Duration

	token     string
	sessionID string
	stage     atomic.Value
	lastErr   atomic.Value
	polls     atomic.Int64
	messages  atomic.Int64
	emptyPoll atomic.Int64
	msgTypes  sync.Map
	sessionMS atomic.Int64
	tokenMS   atomic.Int64
}

func (r *probeRunner) setStage(stage string) { r.stage.Store(stage) }
func (r *probeRunner) fail(err error)        { r.lastErr.Store(safeError(err)); r.setStage("error") }

func (r *probeRunner) v2() bool {
	return bool(r.jit.settings.UseV2Flow) && r.jit.settings.ServerURLV2 != ""
}

// urls returns the session, message and (per-session) delete endpoints plus
// the Accept header for the flow the JIT config selects.
func (r *probeRunner) urls() (session, message, accept string, err error) {
	if r.v2() {
		session, err = resolveProbeURL(r.jit.settings.ServerURLV2, "session")
		if err != nil {
			return
		}
		message, err = resolveProbeURL(r.jit.settings.ServerURLV2, "message")
		return session, message, "application/json", err
	}
	base := strings.TrimRight(r.jit.settings.ServerURL, "/") + "/"
	pool := fmt.Sprintf("_apis/distributedtask/pools/%d", r.jit.settings.PoolID)
	if session, err = resolveProbeURL(base, pool+"/sessions"); err != nil {
		return
	}
	message, err = resolveProbeURL(base, pool+"/messages")
	return session, message, "application/json; api-version=5.1-preview.1", err
}

func (r *probeRunner) openSession(ctx context.Context) error {
	start := time.Now()
	token, err := r.http.oauthToken(ctx, r.jit, r.v2())
	if err != nil {
		return err
	}
	r.token = token
	r.tokenMS.Store(time.Since(start).Milliseconds())
	r.setStage("token_ok")

	sessionURL, _, accept, err := r.urls()
	if err != nil {
		return err
	}
	start = time.Now()
	body := map[string]any{
		"sessionId":         "00000000-0000-0000-0000-000000000000",
		"ownerName":         r.owner,
		"agent":             map[string]any{"id": r.jit.settings.AgentID, "name": r.jit.settings.AgentName, "version": r.ver, "osDescription": probeOS},
		"useFipsEncryption": false,
	}
	if r.v2() {
		body["brokerSession"] = true
	}
	status, data, err := r.http.do(ctx, http.MethodPost, sessionURL, r.token, accept, body)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("create session returned HTTP %d: %s", status, snippet(data))
	}
	var out struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(data, &out); err != nil || out.SessionID == "" {
		return errors.New("create session response has no sessionId")
	}
	r.sessionID = out.SessionID
	r.sessionMS.Store(time.Since(start).Milliseconds())
	r.setStage("session_open")
	return nil
}

func (r *probeRunner) refreshToken(ctx context.Context) error {
	token, err := r.http.oauthToken(ctx, r.jit, r.v2())
	if err != nil {
		return err
	}
	r.token = token
	if c := r.counters; c != nil {
		c.refreshes.Add(1)
	}
	return nil
}

func (r *probeRunner) pollError(msg string) {
	r.lastErr.Store(msg)
	if c := r.counters; c != nil {
		c.pollErrors.Add(1)
		c.setError(msg)
	}
}

// poll long-polls for messages until ctx ends. Messages are only counted:
// a fake runner never acquires, renews or completes a job.
func (r *probeRunner) poll(ctx context.Context) {
	_, messageURL, accept, err := r.urls()
	if err != nil {
		r.fail(err)
		return
	}
	query := url.Values{
		"sessionId":     {r.sessionID},
		"status":        {"Online"},
		"runnerVersion": {r.ver},
		"os":            {probeOS},
		"architecture":  {probeArch},
		"disableUpdate": {"true"},
	}
	target := messageURL + "?" + query.Encode()
	unauthorized := 0
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, sessionPollTimeout)
		status, data, err := r.http.do(callCtx, http.MethodGet, target, r.token, accept, nil)
		cancel()
		if ctx.Err() != nil {
			return
		}
		r.polls.Add(1)
		if c := r.counters; c != nil {
			c.polls.Add(1)
		}
		if err != nil || status != http.StatusUnauthorized {
			unauthorized = 0
		}
		switch {
		case err != nil:
			r.pollError(safeError(err))
			sleepCtx(ctx, sessionRetryDelay)
		case status == http.StatusOK && len(bytes.TrimSpace(data)) > 0:
			r.messages.Add(1)
			var msg struct {
				MessageType string `json:"messageType"`
			}
			_ = json.Unmarshal(data, &msg)
			count, _ := r.msgTypes.LoadOrStore(msg.MessageType, new(atomic.Int64))
			count.(*atomic.Int64).Add(1)
			if c := r.counters; c != nil {
				c.noteUnexpected(msg.MessageType, r.name)
			}
			if r.messageBackoff > 0 {
				sleepCtx(ctx, r.messageBackoff)
			}
		case status == http.StatusOK || status == http.StatusAccepted || status == http.StatusNoContent:
			r.emptyPoll.Add(1)
		case status == http.StatusUnauthorized:
			// The OAuth token expired: refresh it and poll again. A second
			// 401 in a row means the refresh did not help, so back off.
			unauthorized++
			if unauthorized > 1 {
				r.pollError("message poll returned HTTP 401 after a token refresh")
				sleepCtx(ctx, sessionRetryDelay)
			}
			if err := r.refreshToken(ctx); err != nil && ctx.Err() == nil {
				r.pollError(safeError(err))
				sleepCtx(ctx, sessionRetryDelay)
			}
		default:
			r.pollError(redactErrorText(fmt.Sprintf("message poll returned HTTP %d: %s", status, snippet(data))))
			sleepCtx(ctx, sessionRetryDelay)
		}
	}
}

func (r *probeRunner) closeSession(ctx context.Context) error {
	if r.sessionID == "" {
		return nil
	}
	sessionURL, _, accept, err := r.urls()
	if err != nil {
		return err
	}
	target := sessionURL + "?sessionId=" + url.QueryEscape(r.sessionID)
	if !r.v2() {
		target = sessionURL + "/" + url.PathEscape(r.sessionID)
	}
	status, data, err := r.http.do(ctx, http.MethodDelete, target, r.token, accept, nil)
	if err == nil && status == http.StatusUnauthorized {
		if err = r.refreshToken(ctx); err == nil {
			status, data, err = r.http.do(ctx, http.MethodDelete, target, r.token, accept, nil)
		}
	}
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNoContent {
		return fmt.Errorf("delete session returned HTTP %d: %s", status, snippet(data))
	}
	r.sessionID = ""
	return nil
}

func (r *probeRunner) openWithRetry(ctx context.Context) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			sleepCtx(ctx, time.Duration(attempt)*2*time.Second)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = r.openSession(ctx); err == nil {
			return nil
		}
	}
	return err
}

// runLimited runs fn for each index with bounded concurrency and stops
// dispatching after the first error.
func runLimited(parent context.Context, n, limit int, fn func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for i := 0; i < n && ctx.Err() == nil; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			if err := fn(ctx, i); err != nil {
				once.Do(func() { first = err; cancel() })
			}
		}(i)
	}
	wg.Wait()
	if first == nil {
		first = parent.Err()
	}
	return first
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// sessionGroupStats is a point-in-time view of one runner group's fake-online
// sessions. Open is the number currently open; Opened and Closed are totals.
type sessionGroupStats struct {
	Requested, Open, OpenFailed, Polls, PollErrors, Unexpected int64
	UnexpectedTypes                                            map[string]int64
	LastError                                                  string // redacted
	Opened, Closed, CloseErrors, TokenRefreshes                int64
}

type sessionCounters struct {
	group string
	out   io.Writer

	requested, open, opened, openFailed, closed, closeErrors atomic.Int64
	polls, pollErrors, unexpected, refreshes                 atomic.Int64

	mu      sync.Mutex
	types   map[string]int64
	lastErr string
}

func (c *sessionCounters) setError(msg string) {
	c.mu.Lock()
	c.lastErr = msg
	c.mu.Unlock()
}

func (c *sessionCounters) noteUnexpected(msgType, runner string) {
	if msgType == "" {
		msgType = "unknown"
	}
	c.unexpected.Add(1)
	c.mu.Lock()
	first := c.types[msgType] == 0
	c.types[msgType]++
	c.mu.Unlock()
	if first {
		fmt.Fprintf(c.out, "%s WARNING: fake runner %s in group %s received an unexpected %s message (counted, not acted upon)\n",
			time.Now().Format("15:04:05"), runner, c.group, msgType)
	}
}

func (c *sessionCounters) snapshot() sessionGroupStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	types := make(map[string]int64, len(c.types))
	for k, v := range c.types {
		types[k] = v
	}
	return sessionGroupStats{
		Requested: c.requested.Load(), Open: c.open.Load(), OpenFailed: c.openFailed.Load(),
		Polls: c.polls.Load(), PollErrors: c.pollErrors.Load(), Unexpected: c.unexpected.Load(),
		UnexpectedTypes: types, LastError: c.lastErr,
		Opened: c.opened.Load(), Closed: c.closed.Load(), CloseErrors: c.closeErrors.Load(), TokenRefreshes: c.refreshes.Load(),
	}
}

// sessionManager keeps fake-online runner sessions open for many runner
// groups. Each session gets its own goroutine that opens (bounded by
// openConcurrency) and then long-polls until closeAll.
type sessionManager struct {
	http             *probeHTTP
	version          string
	owner            string
	out              io.Writer
	openSem          chan struct{}
	closeConcurrency int
	messageBackoff   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	sessions []*probeRunner
	groups   map[string]*sessionCounters

	closeMu sync.Mutex
}

func newSessionManager(httpClient *http.Client, openConcurrency int, runnerVersion string, out io.Writer) *sessionManager {
	openConcurrency = max(openConcurrency, 1)
	if out == nil {
		out = io.Discard
	}
	hostname, _ := os.Hostname()
	ctx, cancel := context.WithCancel(context.Background())
	return &sessionManager{
		http:             &probeHTTP{client: httpClient},
		version:          runnerVersion,
		owner:            fmt.Sprintf("%s (PID: %d)", hostname, os.Getpid()),
		out:              out,
		openSem:          make(chan struct{}, openConcurrency),
		closeConcurrency: openConcurrency,
		messageBackoff:   sessionMessageBackoff,
		ctx:              ctx,
		cancel:           cancel,
		groups:           map[string]*sessionCounters{},
	}
}

func (m *sessionManager) groupLocked(group string) *sessionCounters {
	c, ok := m.groups[group]
	if !ok {
		c = &sessionCounters{group: group, out: m.out, types: map[string]int64{}}
		m.groups[group] = c
	}
	return c
}

// add takes a just-generated encoded JIT config, decodes it in memory, opens
// the session and long-polls until closeAll. It returns at once; ctx only
// bounds the wait for an open slot and the open itself. Calls after closeAll
// are ignored.
func (m *sessionManager) add(ctx context.Context, group string, runnerID int, runnerName string, encodedJIT string) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	counters := m.groupLocked(group)
	counters.requested.Add(1)
	r := &probeRunner{
		name: runnerName, id: runnerID, http: m.http, owner: m.owner, ver: m.version,
		counters: counters, messageBackoff: m.messageBackoff,
	}
	r.setStage("queued")
	m.sessions = append(m.sessions, r)
	m.wg.Add(1)
	m.mu.Unlock()
	go m.run(ctx, r, encodedJIT)
}

func (m *sessionManager) run(ctx context.Context, r *probeRunner, encodedJIT string) {
	defer m.wg.Done()
	jit, err := decodeProbeJIT(encodedJIT)
	if err != nil {
		m.openFailed(r, fmt.Errorf("decode JIT config: %w", err))
		return
	}
	r.jit = jit
	r.setStage("jit_ok")

	openCtx, cancel := context.WithCancel(m.ctx)
	stop := context.AfterFunc(ctx, cancel)
	err = m.open(openCtx, r)
	stop()
	cancel()
	if err != nil {
		if openCtx.Err() != nil {
			r.setStage("cancelled")
		} else {
			m.openFailed(r, err)
		}
		return
	}
	r.counters.open.Add(1)
	r.counters.opened.Add(1)
	r.poll(m.ctx)
}

func (m *sessionManager) open(ctx context.Context, r *probeRunner) error {
	select {
	case m.openSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.openSem }()
	return r.openWithRetry(ctx)
}

func (m *sessionManager) openFailed(r *probeRunner, err error) {
	r.fail(err)
	r.counters.openFailed.Add(1)
	r.counters.setError(safeError(err))
}

// stats returns per-group session counters keyed by group name.
func (m *sessionManager) stats() map[string]sessionGroupStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]sessionGroupStats, len(m.groups))
	for name, c := range m.groups {
		out[name] = c.snapshot()
	}
	return out
}

// closeAll stops opens and polls, then deletes every open session with
// bounded concurrency. It is idempotent: a repeated call retries only the
// sessions an earlier call could not delete.
func (m *sessionManager) closeAll(ctx context.Context) error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.cancel()

	stopped := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		return fmt.Errorf("close sessions: waiting for pollers to stop: %w", ctx.Err())
	}

	m.mu.Lock()
	var open []*probeRunner
	for _, r := range m.sessions {
		if r.sessionID != "" {
			open = append(open, r)
		}
	}
	m.mu.Unlock()
	if len(open) == 0 {
		return nil
	}
	var closed, failed atomic.Int64
	var firstErr atomic.Value
	_ = runLimited(ctx, len(open), m.closeConcurrency, func(ctx context.Context, i int) error {
		r := open[i]
		if err := r.closeSession(ctx); err != nil {
			msg := safeError(err)
			failed.Add(1)
			r.counters.closeErrors.Add(1)
			r.counters.setError(msg)
			firstErr.CompareAndSwap(nil, msg)
			return nil
		}
		r.counters.open.Add(-1)
		r.counters.closed.Add(1)
		r.setStage("closed")
		closed.Add(1)
		return nil
	})
	fmt.Fprintf(m.out, "%s sessions: closed %d of %d, delete errors %d\n",
		time.Now().Format("15:04:05"), closed.Load(), len(open), failed.Load())
	var errs []error
	if f := failed.Load(); f > 0 {
		errs = append(errs, fmt.Errorf("%d of %d runner sessions could not be deleted; first: %v", f, len(open), firstErr.Load()))
	}
	if notAttempted := int64(len(open)) - closed.Load() - failed.Load(); notAttempted > 0 {
		errs = append(errs, fmt.Errorf("%d runner sessions were not deleted before the deadline: %w", notAttempted, ctx.Err()))
	}
	return errors.Join(errs...)
}
