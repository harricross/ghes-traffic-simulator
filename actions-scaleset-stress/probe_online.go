package main

// Spike for the "online but idle" runner idea: can a small protocol client
// built from a JIT config make a registered runner appear online in GHES
// without starting the real runner? The probe uses a handful of runners on a
// throwaway scale set and removes everything it creates. It prints protocol
// metadata only; credentials, tokens and RSA parameters are never logged.

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
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/actions/scaleset"
)

const (
	probeMaxRunners = 1000
	probeMaxHold    = 30 * time.Minute
	probeOS         = "Linux"
	probeArch       = "X64"
	probeSmallRun   = 20
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
func (r *probeRunner) fail(err error)        { r.lastErr.Store(err.Error()); r.setStage("error") }

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
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
		status, data, err := r.http.do(callCtx, http.MethodGet, target, r.token, accept, nil)
		cancel()
		if ctx.Err() != nil {
			return
		}
		r.polls.Add(1)
		switch {
		case err != nil:
			r.lastErr.Store(err.Error())
			sleepCtx(ctx, 5*time.Second)
		case status == http.StatusOK && len(bytes.TrimSpace(data)) > 0:
			r.messages.Add(1)
			var msg struct {
				MessageType string `json:"messageType"`
			}
			_ = json.Unmarshal(data, &msg)
			count, _ := r.msgTypes.LoadOrStore(msg.MessageType, new(atomic.Int64))
			count.(*atomic.Int64).Add(1)
		case status == http.StatusOK || status == http.StatusAccepted || status == http.StatusNoContent:
			r.emptyPoll.Add(1)
		case status == http.StatusUnauthorized:
			if token, err := r.http.oauthToken(ctx, r.jit, r.v2()); err == nil {
				r.token = token
			} else {
				r.lastErr.Store(err.Error())
				sleepCtx(ctx, 5*time.Second)
			}
		default:
			r.lastErr.Store(fmt.Sprintf("message poll returned HTTP %d: %s", status, snippet(data)))
			sleepCtx(ctx, 5*time.Second)
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
			fmt.Printf("cleanup: delete scale set %d: %v (removed %d runners)\n", created.ID, err, removed)
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
		first.settings.PoolID, first.settings.ServerURL, first.settings.ServerURLV2)
	fmt.Printf("jit credentials: scheme=%s has_client_id=%t authorization_url=%s authorization_url_v2=%s oauth_endpoint_url=%s\n",
		first.creds.Scheme, first.creds.Data.ClientID != "", first.creds.Data.AuthorizationURL, first.creds.Data.AuthorizationURLV2, first.creds.Data.OAuthEndpointURL)
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
			fmt.Printf("close session %s: %v\n", probes[i].name, err)
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
