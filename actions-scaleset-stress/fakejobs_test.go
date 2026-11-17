package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func encryptTestBody(t *testing.T, key, iv, plain []byte) string {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	padded := append(append([]byte{}, plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, padded)
	return base64.StdEncoding.EncodeToString(out)
}

func TestFakeJobMessageDecryption(t *testing.T) {
	key := testRunnerKey()
	aesKey := bytes.Repeat([]byte{7}, 32)
	encrypted, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, &key.PublicKey, aesKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeSessionKey(key, &sessionEncryptionKey{Encrypted: true, Value: encrypted}); err != nil || !bytes.Equal(got, aesKey) {
		t.Fatalf("decodeSessionKey(encrypted) = %x, %v", got, err)
	}
	if got, err := decodeSessionKey(key, &sessionEncryptionKey{Value: aesKey}); err != nil || !bytes.Equal(got, aesKey) {
		t.Fatalf("decodeSessionKey(plain) = %x, %v", got, err)
	}
	if got, err := decodeSessionKey(key, nil); got != nil || err != nil {
		t.Fatalf("decodeSessionKey(nil) = %x, %v", got, err)
	}
	if _, err := decodeSessionKey(key, &sessionEncryptionKey{Encrypted: true, Value: aesKey}); err == nil {
		t.Fatal("a key not encrypted for the runner must be rejected")
	}

	iv := bytes.Repeat([]byte{9}, aes.BlockSize)
	body := encryptTestBody(t, aesKey, iv, []byte("\xef\xbb\xbf{\"jobId\":\"j\"}"))
	if plain, err := decryptMessageBody(aesKey, iv, body); err != nil || string(plain) != `{"jobId":"j"}` {
		t.Fatalf("decryptMessageBody() = %q, %v; want the body without its BOM", plain, err)
	}
	if plain, err := decryptMessageBody(nil, iv, "plain"); err != nil || string(plain) != "plain" {
		t.Fatalf("a body without a session key is plaintext, got %q, %v", plain, err)
	}

	// A block that decrypts to a zero final byte has no valid padding.
	block, _ := aes.NewCipher(aesKey)
	unpadded := make([]byte, aes.BlockSize)
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(unpadded, make([]byte, aes.BlockSize))
	for name, tc := range map[string]struct {
		iv   []byte
		body string
		want string
	}{
		"bad padding": {iv, base64.StdEncoding.EncodeToString(unpadded), "invalid padding"},
		"short iv":    {iv[:8], body, "IV has 8 bytes"},
		"partial":     {iv, base64.StdEncoding.EncodeToString([]byte("short")), "whole number of blocks"},
		"not base64":  {iv, "%%%", "not base64"},
	} {
		if _, err := decryptMessageBody(aesKey, tc.iv, tc.body); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: decryptMessageBody() = %v, want %q", name, err, tc.want)
		}
	}
}

func TestFakeJobFailsFollowsFailureShards(t *testing.T) {
	matrix := func(shard string) json.RawMessage {
		return json.RawMessage(`{"t":2,"d":[{"k":"Shard","v":` + shard + `}]}`)
	}
	inputs := func(failures string) json.RawMessage {
		return json.RawMessage(`{"t":2,"d":[{"k":"failure_shards","v":` + failures + `}]}`)
	}
	github := json.RawMessage(`{"t":2,"d":[{"k":"event","v":{"t":2,"d":[{"k":"inputs","v":{"t":2,"d":[
		{"k":"failure_shards","v":{"t":1,"a":["0",{"s":"3"}]}}]}}]}}]}`)
	for name, tc := range map[string]struct {
		data map[string]json.RawMessage
		want bool
	}{
		"numeric shard listed":    {map[string]json.RawMessage{"matrix": matrix(`{"t":4,"n":1}`), "inputs": inputs(`"[0, 1]"`)}, true},
		"numeric shard unlisted":  {map[string]json.RawMessage{"matrix": matrix(`{"t":4,"n":2}`), "inputs": inputs(`"[0, 1]"`)}, false},
		"string shard":            {map[string]json.RawMessage{"Matrix": matrix(`"1"`), "inputs": inputs(`"[1]"`)}, true},
		"github event fallback":   {map[string]json.RawMessage{"matrix": matrix(`3`), "github": github}, true},
		"no failures":             {map[string]json.RawMessage{"matrix": matrix(`0`), "inputs": inputs(`"[]"`)}, false},
		"malformed failures":      {map[string]json.RawMessage{"matrix": matrix(`0`), "inputs": inputs(`"zero"`)}, false},
		"job without a shard":     {map[string]json.RawMessage{"inputs": inputs(`"[0]"`)}, false},
		"undecodable matrix data": {map[string]json.RawMessage{"matrix": json.RawMessage(`{"t":9}`), "inputs": inputs(`"[0]"`)}, false},
	} {
		if got := fakeJobFails(tc.data); got != tc.want {
			t.Errorf("%s: fakeJobFails() = %v, want %v", name, got, tc.want)
		}
	}
}

// fakeJobEndpoints serves the job protocol a fake runner uses after it
// receives a job: message delete, lock renewal, the plan's JobCompleted
// event and the job-request finish.
type fakeJobEndpoints struct {
	sessions *fakeSessionServer

	mu          sync.Mutex
	renewStatus int
	eventStatus []int // per events call; 200 once exhausted
	deletes     int
	renews      int
	events      []map[string]any
	eventAuth   []string
	finishes    []map[string]any
}

func (e *fakeJobEndpoints) handle(w http.ResponseWriter, r *http.Request) bool {
	const events = "/vss/scope-1/_apis/distributedtask/hubs/build/plans/plan-1/events"
	path := r.URL.Path
	if path != events && path != testPoolPath+"messages/1" && path != testPoolPath+"jobrequests/7" {
		return false
	}
	var body map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if path == events {
		if r.Method != http.MethodPost || r.Header.Get("Accept") != fakeJobEventsAccept {
			w.WriteHeader(http.StatusTeapot)
			return true
		}
		e.events = append(e.events, body)
		e.eventAuth = append(e.eventAuth, r.Header.Get("Authorization"))
		status := http.StatusOK
		if len(e.eventStatus) > 0 {
			status, e.eventStatus = e.eventStatus[0], e.eventStatus[1:]
		}
		w.WriteHeader(status)
		return true
	}
	if _, ok := e.sessions.client(r); !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return true
	}
	switch {
	case r.Method == http.MethodDelete && r.URL.Query().Get("sessionId") == "sess-client-1":
		e.deletes++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPatch && r.URL.Query().Get("lockToken") == fakeJobEmptyLockToken && body["result"] != nil:
		e.finishes = append(e.finishes, body)
	case r.Method == http.MethodPatch && r.URL.Query().Get("lockToken") == fakeJobEmptyLockToken:
		e.renews++
		w.WriteHeader(e.renewStatus)
	default:
		w.WriteHeader(http.StatusTeapot)
	}
	return true
}

func testJobMessage(t *testing.T, serverURL string, aesKey []byte, planVersion int, failureShards string) map[string]any {
	t.Helper()
	job, err := json.Marshal(map[string]any{
		"requestId": 7, "jobId": "job-1", "jobDisplayName": "Test shard 1",
		"plan": map[string]any{"scopeIdentifier": "scope-1", "planType": "build", "planId": "plan-1", "version": planVersion},
		"resources": map[string]any{"endpoints": []any{map[string]any{
			"name": "SystemVssConnection", "url": serverURL + "/vss/",
			"authorization": map[string]any{"scheme": "OAuth", "parameters": map[string]string{"AccessToken": "job-token"}},
		}}},
		"contextData": map[string]json.RawMessage{
			"matrix": json.RawMessage(`{"t":2,"d":[{"k":"shard","v":{"t":4,"n":1}}]}`),
			"inputs": json.RawMessage(`{"t":2,"d":[{"k":"failure_shards","v":"` + failureShards + `"}]}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	iv := bytes.Repeat([]byte{3}, aes.BlockSize)
	return map[string]any{
		"messageId": 1, "messageType": fakeJobMessageType, "iv": iv,
		"body": encryptTestBody(t, aesKey, iv, append([]byte("\xef\xbb\xbf"), job...)),
	}
}

func TestFakeRunnerRunsJobAndReportsResult(t *testing.T) {
	for name, tc := range map[string]struct {
		planVersion   int
		failureShards string
		renewStatus   int
		eventStatus   []int
		replaceErr    error
		done          func(sessionGroupStats) bool
		check         func(t *testing.T, s sessionGroupStats, e *fakeJobEndpoints, replaced []string, fatal error)
	}{
		"failed shard retries numeric result and finishes old plan": {
			planVersion: 7, failureShards: "[1]", renewStatus: http.StatusOK,
			eventStatus: []int{http.StatusBadRequest},
			done:        func(s sessionGroupStats) bool { return s.Replacements == 1 },
			check: func(t *testing.T, s sessionGroupStats, e *fakeJobEndpoints, replaced []string, fatal error) {
				if s.JobsReceived != 1 || s.JobsFailed != 1 || s.JobErrors != 0 || fatal != nil {
					t.Fatalf("stats %+v, fatal %v", s, fatal)
				}
				if len(e.events) != 2 || e.events[0]["result"] != "failed" || e.events[1]["result"] != float64(2) ||
					e.events[1]["jobId"] != "job-1" || e.events[1]["requestId"] != float64(7) || e.events[1]["name"] != "JobCompleted" {
					t.Fatalf("events %v, want string then numeric failed result", e.events)
				}
				if len(e.finishes) != 1 || e.finishes[0]["result"] != "failed" {
					t.Fatalf("finishes %v, want one for a version 7 plan", e.finishes)
				}
				if fmt.Sprint(replaced) != "[fake-1]" {
					t.Fatalf("replaced %v", replaced)
				}
			},
		},
		"server cancel completes as canceled without finish or replacement": {
			planVersion: 8, failureShards: "[]", renewStatus: http.StatusNotFound,
			replaceErr: errReplacementSkipped,
			done:       func(s sessionGroupStats) bool { return s.JobsCanceled == 1 },
			check: func(t *testing.T, s sessionGroupStats, e *fakeJobEndpoints, replaced []string, fatal error) {
				if s.Replacements != 0 || s.ReplaceErrors != 0 || s.JobErrors != 0 || fatal != nil {
					t.Fatalf("a skipped replacement must not count: %+v, fatal %v", s, fatal)
				}
				if len(e.events) != 1 || e.events[0]["result"] != "canceled" || len(e.finishes) != 0 {
					t.Fatalf("events %v, finishes %v", e.events, e.finishes)
				}
			},
		},
		"rejected completion is fatal": {
			planVersion: 8, failureShards: "[]", renewStatus: http.StatusOK,
			eventStatus: []int{http.StatusForbidden},
			done:        func(s sessionGroupStats) bool { return s.JobErrors == 1 },
			check: func(t *testing.T, s sessionGroupStats, e *fakeJobEndpoints, replaced []string, fatal error) {
				if fatal == nil || !strings.Contains(fatal.Error(), "JobCompleted returned HTTP 403") {
					t.Fatalf("fatal %v, want the rejected completion", fatal)
				}
				if len(replaced) != 0 || s.JobsSucceeded != 0 {
					t.Fatalf("a failed job must not be replaced: %v %+v", replaced, s)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSessionServer(t)
			fake.sessionKey = bytes.Repeat([]byte{5}, 32)
			endpoints := &fakeJobEndpoints{sessions: fake, renewStatus: tc.renewStatus, eventStatus: tc.eventStatus}
			fake.handle = endpoints.handle
			fake.rawMessage["client-1"] = testJobMessage(t, fake.srv.URL, fake.sessionKey, tc.planVersion, tc.failureShards)

			var out syncBuffer
			m := newSessionManager(fake.srv.Client(), 1, "2.338.0", &out)
			var mu sync.Mutex
			var replaced []string
			m.setFakeJobs("g01", &fakeJobConfig{replace: func(_ context.Context, finished string) error {
				mu.Lock()
				defer mu.Unlock()
				replaced = append(replaced, finished)
				return tc.replaceErr
			}})
			m.add(context.Background(), "g01", 101, "fake-1", encodeTestJIT(t, fake.srv.URL, "client-1", 101))

			deadline := time.Now().Add(10 * time.Second)
			for s := m.stats()["g01"]; !(s.Closed == 1 && tc.done(s)); s = m.stats()["g01"] {
				if time.Now().After(deadline) {
					t.Fatalf("job did not finish: %+v", s)
				}
				time.Sleep(10 * time.Millisecond)
			}
			var fatal error
			select {
			case fatal = <-m.fatalErrors():
			default:
			}
			if err := m.closeAll(context.Background()); err != nil {
				t.Fatalf("closeAll: %v", err)
			}
			s := m.stats()["g01"]
			fake.mu.Lock()
			deletes, unknown := fake.deletes["sess-client-1"], fake.unknown
			fake.mu.Unlock()
			endpoints.mu.Lock()
			defer endpoints.mu.Unlock()
			mu.Lock()
			defer mu.Unlock()
			if s.Open != 0 || s.Closed != 1 || deletes != 1 || len(unknown) != 0 {
				t.Fatalf("session must close once after its job: %+v, deletes %d, unknown %v", s, deletes, unknown)
			}
			if endpoints.deletes != 1 || endpoints.renews == 0 {
				t.Fatalf("message deletes %d, renews %d", endpoints.deletes, endpoints.renews)
			}
			for _, auth := range endpoints.eventAuth {
				if auth != "Bearer job-token" {
					t.Fatalf("events must use the job's access token, got %q", auth)
				}
			}
			tc.check(t, s, endpoints, replaced, fatal)
			if text := out.String(); strings.Contains(text, "job-token") || strings.Contains(text, "SECRETtoken") {
				t.Fatalf("output leaks a token: %q", text)
			}
			if fatal != nil && (strings.Contains(fatal.Error(), "job-token") || strings.Contains(fatal.Error(), "SECRETtoken")) {
				t.Fatalf("fatal error leaks a token: %v", fatal)
			}
		})
	}
}

func TestDecodeJobMessageRejectsIncompleteJobs(t *testing.T) {
	for name, body := range map[string]string{
		"no request": `{"jobId":"j","plan":{"scopeIdentifier":"s","planType":"build","planId":"p"}}`,
		"no job":     `{"requestId":1,"plan":{"scopeIdentifier":"s","planType":"build","planId":"p"}}`,
		"no plan":    `{"requestId":1,"jobId":"j"}`,
		"not json":   `{`,
	} {
		if _, err := decodeJobMessage(nil, probeMessage{Body: body}); err == nil {
			t.Errorf("%s: decodeJobMessage accepted %s", name, body)
		}
	}
	job, err := decodeJobMessage(nil, probeMessage{Body: `{"requestId":"1","jobId":"j","plan":{"scopeIdentifier":"s","planType":"build","planId":"p"}}`})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := job.systemConnection(); err == nil || !strings.Contains(err.Error(), "SystemVssConnection") {
		t.Fatalf("systemConnection() = %v, want a missing endpoint error", err)
	}
}
