package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var testRunnerKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

// encodeTestJIT builds a JIT config shaped like the one GHES returns for a
// legacy (non-V2) runner, with a credential signed by testRunnerKey.
func encodeTestJIT(t *testing.T, serverURL, clientID string, agentID int) string {
	t.Helper()
	key := testRunnerKey()
	file := func(v any) string {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return base64.StdEncoding.EncodeToString(data)
	}
	return file(map[string]string{
		".runner": file(map[string]string{
			"AgentId": strconv.Itoa(agentID), "AgentName": clientID, "PoolId": "1",
			"ServerUrl": serverURL + "/_services/pipelines/SECRETtoken/", "UseV2Flow": "false",
		}),
		".credentials": file(map[string]any{"Scheme": "OAuth", "Data": map[string]string{
			"clientId": clientID, "authorizationUrl": serverURL + "/oauth/token", "oauthEndpointUrl": serverURL + "/oauth/token",
		}}),
		".credentials_rsaparams": file(probeRSAParams{
			Exponent: big.NewInt(int64(key.E)).Bytes(), Modulus: key.N.Bytes(), D: key.D.Bytes(),
			P: key.Primes[0].Bytes(), Q: key.Primes[1].Bytes(),
		}),
	})
}

const testPoolPath = "/_services/pipelines/SECRETtoken/_apis/distributedtask/pools/1/"

// fakeSessionServer implements the token, session and message endpoints a
// fake runner may use. Any other request, such as acquiring a job or
// deleting a message, is recorded as unknown unless handle serves it.
type fakeSessionServer struct {
	srv *httptest.Server
	// handle, when set, may serve a request first and report true.
	handle func(w http.ResponseWriter, r *http.Request) bool

	mu              sync.Mutex
	issued          map[string]int    // tokens issued per client
	tokens          map[string]string // token -> client
	sessions        map[string]string // open session -> client
	deletes         map[string]int
	polls           map[string]int
	message         map[string]string         // client -> message type delivered once
	rawMessage      map[string]map[string]any // client -> whole message delivered once
	sessionKey      []byte                    // returned RSA-encrypted when set
	fips            bool                      // encrypt sessionKey with OAEP-SHA256 and say so
	revokeFirstPoll map[string]bool
	unknown         []string
}

func newFakeSessionServer(t *testing.T) *fakeSessionServer {
	f := &fakeSessionServer{
		issued: map[string]int{}, tokens: map[string]string{}, sessions: map[string]string{},
		deletes: map[string]int{}, polls: map[string]int{}, message: map[string]string{},
		rawMessage: map[string]map[string]any{}, revokeFirstPoll: map[string]bool{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

// client returns the runner a request's OAuth bearer token was issued to.
func (f *fakeSessionServer) client(r *http.Request) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	client, ok := f.tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
	return client, ok
}

func (f *fakeSessionServer) serve(w http.ResponseWriter, r *http.Request) {
	const pool = testPoolPath
	if r.Method == http.MethodPost && r.URL.Path == "/oauth/token" {
		f.issueToken(w, r)
		return
	}
	if f.handle != nil && f.handle(w, r) {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	client, ok := f.tokens[token]
	switch {
	case !strings.HasPrefix(r.URL.Path, pool):
		f.unknown = append(f.unknown, r.Method+" "+r.URL.Path)
		http.NotFound(w, r)
	case !ok:
		w.WriteHeader(http.StatusUnauthorized)
	case r.Method == http.MethodPost && r.URL.Path == pool+"sessions":
		id := "sess-" + client
		f.sessions[id] = client
		resp := map[string]any{"sessionId": id}
		if f.sessionKey != nil {
			hash := sha1.New()
			if f.fips {
				hash = sha256.New()
				resp["useFipsEncryption"] = true
			}
			encrypted, err := rsa.EncryptOAEP(hash, rand.Reader, &testRunnerKey().PublicKey, f.sessionKey, nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp["encryptionKey"] = map[string]any{"encrypted": true, "value": encrypted}
		}
		_ = json.NewEncoder(w).Encode(resp)
	case r.Method == http.MethodGet && r.URL.Path == pool+"messages":
		if f.sessions[r.URL.Query().Get("sessionId")] != client {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.polls[client]++
		if f.revokeFirstPoll[client] && f.polls[client] == 1 {
			delete(f.tokens, token)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if msg := f.rawMessage[client]; msg != nil {
			delete(f.rawMessage, client)
			_ = json.NewEncoder(w).Encode(msg)
			return
		}
		if msgType := f.message[client]; msgType != "" {
			delete(f.message, client)
			_ = json.NewEncoder(w).Encode(map[string]any{"messageId": 1, "messageType": msgType, "body": `{"jobId":"x"}`})
			return
		}
		f.mu.Unlock()
		select {
		case <-r.Context().Done():
		case <-time.After(20 * time.Millisecond):
		}
		f.mu.Lock()
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, pool+"sessions/"):
		id := strings.TrimPrefix(r.URL.Path, pool+"sessions/")
		f.deletes[id]++
		delete(f.sessions, id)
	default:
		f.unknown = append(f.unknown, r.Method+" "+r.URL.Path)
		http.NotFound(w, r)
	}
}

func (f *fakeSessionServer) issueToken(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.PostFormValue("client_assertion"), ".")
	if len(parts) != 3 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	claimsJSON, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct {
		Sub string `json:"sub"`
	}
	if rsa.VerifyPKCS1v15(&testRunnerKey().PublicKey, crypto.SHA256, digest[:], sig) != nil ||
		json.Unmarshal(claimsJSON, &claims) != nil || claims.Sub == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	f.issued[claims.Sub]++
	token := fmt.Sprintf("tok-%s-%d", claims.Sub, f.issued[claims.Sub])
	f.tokens[token] = claims.Sub
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": token})
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSessionManagerCountsFailedOpens(t *testing.T) {
	fake := newFakeSessionServer(t)
	fake.handle = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions") {
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	}
	m := newSessionManager(fake.srv.Client(), 1, "2.338.0", nil)
	m.openBackoff = time.Millisecond
	m.add(context.Background(), "g01", 101, "fake-1", encodeTestJIT(t, fake.srv.URL, "client-1", 101))

	deadline := time.Now().Add(10 * time.Second)
	for s := m.stats()["g01"]; s.OpenFailed != 1; s = m.stats()["g01"] {
		if time.Now().After(deadline) {
			t.Fatalf("a failed open was not counted: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := m.stats()["g01"]; !strings.Contains(s.LastError, "HTTP 500") {
		t.Fatalf("last error %q, want the session create failure", s.LastError)
	}
	if err := m.closeAll(context.Background()); err != nil {
		t.Fatalf("closeAll: %v", err)
	}
}

func TestSessionManagerKeepsFakeRunnersIdleAndClosesSessions(t *testing.T) {
	fake := newFakeSessionServer(t)
	fake.message["client-1"] = "PipelineAgentJobRequest"
	fake.revokeFirstPoll["client-2"] = true
	var out syncBuffer
	m := newSessionManager(fake.srv.Client(), 2, "2.338.0", &out)
	m.messageBackoff = 10 * time.Millisecond
	ctx := context.Background()
	m.add(ctx, "g01", 101, "fake-1", encodeTestJIT(t, fake.srv.URL, "client-1", 101))
	m.add(ctx, "g02", 102, "fake-2", encodeTestJIT(t, fake.srv.URL, "client-2", 102))
	m.add(ctx, "g02", 103, "fake-3", "not-a-jit-config")

	deadline := time.Now().Add(10 * time.Second)
	for {
		s := m.stats()
		if s["g01"].Unexpected == 1 && s["g01"].Polls >= 3 && s["g02"].TokenRefreshes == 1 && s["g02"].Polls >= 3 && s["g02"].OpenFailed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sessions did not settle: %+v", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats := m.stats()
	g01, g02 := stats["g01"], stats["g02"]
	if g01.Requested != 1 || g01.Open != 1 || g01.OpenFailed != 0 || fmt.Sprint(g01.UnexpectedTypes) != "map[PipelineAgentJobRequest:1]" {
		t.Fatalf("g01 stats %+v", g01)
	}
	if g02.Requested != 2 || g02.Open != 1 || g02.Unexpected != 0 || !strings.Contains(g02.LastError, "not base64") {
		t.Fatalf("g02 stats %+v", g02)
	}
	fake.mu.Lock()
	issued := fake.issued["client-2"]
	fake.mu.Unlock()
	if issued != 2 {
		t.Fatalf("client-2 got %d tokens, want a refresh after the 401", issued)
	}

	if err := m.closeAll(ctx); err != nil {
		t.Fatalf("closeAll: %v", err)
	}
	fake.mu.Lock()
	deletes := fmt.Sprint(fake.deletes)
	open, unknown := len(fake.sessions), fake.unknown
	fake.mu.Unlock()
	if deletes != "map[sess-client-1:1 sess-client-2:1]" || open != 0 {
		t.Fatalf("deletes %s, %d sessions left open", deletes, open)
	}
	if len(unknown) != 0 {
		t.Fatalf("fake runners must only poll, got %v", unknown)
	}
	for group, s := range m.stats() {
		if s.Open != 0 || s.Closed != 1 || s.CloseErrors != 0 {
			t.Fatalf("%s after closeAll: %+v", group, s)
		}
	}
	if err := m.closeAll(ctx); err != nil {
		t.Fatalf("second closeAll: %v", err)
	}
	m.add(ctx, "g03", 104, "fake-4", encodeTestJIT(t, fake.srv.URL, "client-4", 104))
	fake.mu.Lock()
	deletes = fmt.Sprint(fake.deletes)
	fake.mu.Unlock()
	if _, added := m.stats()["g03"]; added || deletes != "map[sess-client-1:1 sess-client-2:1]" {
		t.Fatalf("closeAll must be idempotent and later adds ignored: deletes %s", deletes)
	}
	text := out.String()
	if !strings.Contains(text, "unexpected PipelineAgentJobRequest message") || strings.Contains(text, "SECRETtoken") {
		t.Fatalf("output %q", text)
	}
}
