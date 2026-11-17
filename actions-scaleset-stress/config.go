package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/actions/scaleset"
)

type config struct {
	APIBaseURL   string `json:"api_base_url"`
	ConfigURL    string `json:"config_url"`
	Owner        string `json:"owner"`
	Repo         string `json:"repo"`
	WorkflowFile string `json:"workflow_file"`
	Ref          string `json:"ref"`
	ScaleSetName string `json:"scale_set_name"`
	RunnerGroup  string `json:"runner_group"`
	RunnerImage  string `json:"runner_image"`
	TokenEnv     string `json:"token_env"`
}

type target struct {
	cfg       config
	token     string
	api       *ghesAPI
	scaleSet  *scaleset.Client
	identity  string
	repoState string
}

var (
	safeName    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
	safeRunID   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	safeRef     = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
	imageDigest = regexp.MustCompile(`^.+@sha256:[a-fA-F0-9]{64}$`)
)

func loadConfig(file string) (config, string, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return config{}, "", fmt.Errorf("read config: %w", err)
	}
	var cfg config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return config{}, "", fmt.Errorf("parse non-secret JSON config: %w", err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return config{}, "", errors.New("config must contain exactly one JSON object")
	}
	if cfg.TokenEnv == "" {
		cfg.TokenEnv = "GHES_ACTIONS_TOKEN"
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(cfg.TokenEnv) {
		return config{}, "", errors.New("token_env must be an environment variable name")
	}
	token := os.Getenv(cfg.TokenEnv)
	if token == "" {
		return config{}, "", fmt.Errorf("environment variable %s is not set", cfg.TokenEnv)
	}
	if err := cfg.validate(); err != nil {
		return config{}, "", err
	}
	return cfg, token, nil
}

func (c config) validate() error {
	if c.Owner == "" || !safeName.MatchString(c.Owner) {
		return errors.New("owner must be a valid repository owner name")
	}
	if c.Repo == "" || !safeName.MatchString(c.Repo) {
		return errors.New("repo must be a valid repository name")
	}
	if c.WorkflowFile == "" {
		return errors.New("workflow_file is required")
	}
	if c.Ref == "" || !safeRef.MatchString(c.Ref) || strings.HasPrefix(c.Ref, "-") ||
		strings.Contains(c.Ref, "..") || strings.Contains(c.Ref, "//") ||
		strings.HasSuffix(c.Ref, "/") || strings.HasSuffix(c.Ref, ".") {
		return errors.New("ref must name an existing branch or tag")
	}
	clean := path.Clean(c.WorkflowFile)
	if clean != c.WorkflowFile || strings.HasPrefix(clean, "/") || !strings.HasPrefix(clean, ".github/workflows/") || !strings.HasSuffix(clean, ".yml") && !strings.HasSuffix(clean, ".yaml") {
		return errors.New("workflow_file must be a .yml or .yaml path under .github/workflows/")
	}
	if c.ScaleSetName == "" || !safeName.MatchString(c.ScaleSetName) || len(c.ScaleSetName) > 40 {
		return errors.New("scale_set_name must be 1-40 safe characters")
	}
	if c.RunnerGroup == "" {
		return errors.New("runner_group is required (use default for the default group)")
	}
	apiURL, err := url.Parse(c.APIBaseURL)
	if err != nil || apiURL.Hostname() == "" || apiURL.User != nil || apiURL.RawQuery != "" || apiURL.Fragment != "" || !strings.HasSuffix(strings.TrimRight(apiURL.Path, "/"), "/api/v3") {
		return errors.New("api_base_url must be a GHES API URL ending in /api/v3")
	}
	if isHostedGitHub(apiURL.Hostname()) {
		return errors.New("this tool refuses GitHub.com and hosted GitHub domains")
	}
	if apiURL.Scheme != "https" && !(apiURL.Scheme == "http" && isLoopback(apiURL.Hostname())) {
		return errors.New("TLS is required except for a loopback test endpoint")
	}
	configURL, err := url.Parse(c.ConfigURL)
	if err != nil || configURL.Host == "" || configURL.User != nil || configURL.RawQuery != "" || configURL.Fragment != "" {
		return errors.New("config_url must be the repository HTTPS URL without credentials or query")
	}
	webHost := strings.ToLower(configURL.Hostname())
	if isHostedGitHub(webHost) || !strings.EqualFold(configURL.Host, apiURL.Host) ||
		!strings.EqualFold(configURL.Scheme, apiURL.Scheme) {
		return errors.New("config_url must use the same GHES host as api_base_url")
	}
	if configURL.Scheme != "https" && !(configURL.Scheme == "http" && isLoopback(configURL.Hostname())) {
		return errors.New("config_url must use HTTPS except for a loopback test endpoint")
	}
	wantPath := "/" + url.PathEscape(c.Owner) + "/" + url.PathEscape(c.Repo)
	if strings.TrimRight(configURL.Path, "/") != wantPath {
		return errors.New("config_url path must exactly match owner/repo (repository scope only)")
	}
	if c.RunnerImage != "" && !imageDigest.MatchString(c.RunnerImage) {
		return errors.New("runner_image must be pinned by sha256 digest")
	}
	return nil
}

func newTarget(cfg config, token, caCert string, insecure bool) (*target, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	roots := (*x509.CertPool)(nil)
	if caCert != "" {
		pem, err := os.ReadFile(caCert)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA certificate file contains no usable PEM certificates")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	apiURL, _ := url.Parse(cfg.APIBaseURL)
	if apiURL.Scheme == "https" {
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, InsecureSkipVerify: insecure}
	}
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Minute}
	sdkClient, err := scaleset.NewClientWithPersonalAccessToken(
		scaleset.NewClientWithPersonalAccessTokenConfig{
			GitHubConfigURL:     strings.TrimRight(cfg.ConfigURL, "/"),
			PersonalAccessToken: token,
			SystemInfo: scaleset.SystemInfo{
				System:    "ghes-traffic-simulator",
				Version:   "dev",
				Subsystem: "actions-load",
			},
		},
		scaleset.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("create official scale-set client: %w", err)
	}
	apiClient := *httpClient
	apiClient.Timeout = 30 * time.Second
	api := newGHESAPI(cfg.APIBaseURL, token, &apiClient)
	return &target{cfg: cfg, token: token, api: api, scaleSet: sdkClient}, nil
}

func isHostedGitHub(host string) bool {
	host = strings.ToLower(host)
	return host == "github.com" || host == "www.github.com" || host == "api.github.com" ||
		strings.HasSuffix(host, ".github.com") || strings.HasSuffix(host, ".ghe.com")
}

func isLoopback(host string) bool {
	host = strings.ToLower(host)
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
