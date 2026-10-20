#!/usr/bin/env python3
"""Create issues with generated image attachments on GitHub Enterprise Server.

Each image is uploaded through the same web upload flow the browser uses when
an image is dropped into an issue, so it is stored in the appliance's user
asset object storage. The issue body then references the uploaded images.

GHES has no token-based attachment API (`gh --attach` refuses GHES hosts), so
uploads need a web session. Provide one of:

  GHES_WEB_LOGIN and GHES_WEB_PASSWORD   username and password (built-in or
                                          LDAP authentication, no 2FA)
  GHES_WEB_SESSION                        value of the `user_session` cookie
                                          copied from a signed-in browser

The same keys can be set in .gh-api-examples.conf as web_login,
web_password, and web_session_cookie. Issues are created through the REST API
with GITHUB_TOKEN from the config.
"""

import argparse
import concurrent.futures
import html
import http.cookiejar
import importlib.util
import json
import os
import random
import re
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONF = Path("./.gh-api-examples.conf")
CONF_KEYS = (
    "hostname", "org", "repo", "GITHUB_TOKEN", "GITHUB_API_BASE_URL", "github_api_version",
    "curl_custom_flags", "web_login", "web_password", "web_session_cookie",
)
MAX_RETRIES = 5
USER_AGENT = "ghes-traffic-simulator/seed-issue-attachments"


def _load_image_module():
    spec = importlib.util.spec_from_file_location("generate_image", SCRIPT_DIR / "generate-image.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generate_image = _load_image_module()


def load_conf(path):
    if not path.is_file():
        sys.exit(f"ERROR: {path} not found. Populate it before running this script.")
    # Source with bash so values that reference other variables expand as they do for the shell scripts.
    script = '. "$1"; printf "%s\\0" ' + " ".join(f'"${{{key}-}}"' for key in CONF_KEYS)
    result = subprocess.run(
        ["bash", "-c", script, "_", str(path)],
        check=True, capture_output=True,
    )
    values = result.stdout.decode().split("\0")
    return dict(zip(CONF_KEYS, values))


class RetryableError(Exception):
    def __init__(self, message, wait):
        super().__init__(message)
        self.wait = wait


def _retry_wait(err, attempt):
    retry_after = err.headers.get("Retry-After") if err.headers else None
    if retry_after and retry_after.isdigit():
        return int(retry_after)
    reset = err.headers.get("X-RateLimit-Reset") if err.headers else None
    if reset and reset.isdigit() and err.headers.get("X-RateLimit-Remaining") == "0":
        return max(1, int(reset) - int(time.time()) + 1)
    return min(60, 2 ** attempt + random.random())


def _encode_multipart(fields, file_field=None):
    boundary = f"----ghes-traffic-sim-{uuid.uuid4().hex}"
    out = bytearray()
    for name, value in fields:
        out += f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n".encode()
        out += str(value).encode() + b"\r\n"
    if file_field:
        name, filename, content_type, data = file_field
        out += (
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n"
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode()
        out += data + b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return bytes(out), f"multipart/form-data; boundary={boundary}"


class Ghes:
    def __init__(self, conf, insecure, verbose):
        self.host = conf["hostname"]
        self.base = f"https://{self.host}"
        self.api = conf["GITHUB_API_BASE_URL"].rstrip("/")
        self.token = conf["GITHUB_TOKEN"]
        self.api_version = conf.get("github_api_version") or "2022-11-28"
        self.verbose = verbose
        self.jar = http.cookiejar.CookieJar()
        ctx = ssl.create_default_context()
        if insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar),
            urllib.request.HTTPSHandler(context=ctx),
        )
        self.csrf_token = None
        self.use_csrf = False
        self.lock = threading.Lock()

    def log(self, message):
        if self.verbose:
            print(f"    DEBUG: {message}", file=sys.stderr)

    def request(self, method, url, data=None, headers=None):
        req = urllib.request.Request(url, data=data, method=method, headers={"User-Agent": USER_AGENT, **(headers or {})})
        self.log(f"{method} {url}")
        with self.opener.open(req, timeout=300) as resp:
            return resp.status, resp.geturl(), resp.read()

    def with_retries(self, label, func):
        for attempt in range(MAX_RETRIES + 1):
            try:
                return func()
            except urllib.error.HTTPError as err:
                body = err.read().decode(errors="replace")[:500]
                retryable = err.code in (429, 502, 503, 504) or (
                    err.code == 403 and "rate limit" in body.lower()
                )
                if not retryable or attempt == MAX_RETRIES:
                    raise RuntimeError(f"{label}: HTTP {err.code}: {body}") from None
                wait = _retry_wait(err, attempt)
                print(f"    WARN: {label}: HTTP {err.code}, retrying in {wait:.0f}s", file=sys.stderr)
                time.sleep(wait)
            except urllib.error.URLError as err:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"{label}: {err.reason}") from None
                time.sleep(min(60, 2 ** attempt))

    # REST API (token)

    def api_call(self, method, path, payload=None):
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": self.api_version,
        }
        data = None
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        _, _, body = self.with_retries(f"{method} {path}", lambda: self.request(method, f"{self.api}{path}", data, headers))
        return json.loads(body) if body else None

    # Web session

    def _set_cookie(self, name, value):
        self.jar.set_cookie(http.cookiejar.Cookie(
            0, name, value, None, False, self.host, False, False, "/", True, self.base.startswith("https://"),
            None, False, None, None, {},
        ))

    def _cookie(self, name):
        return next((c.value for c in self.jar if c.name == name), None)

    def use_session_cookie(self, value):
        # GHES sets both cookies to the same value; state-changing requests check the same-site one.
        self._set_cookie("user_session", value)
        self._set_cookie("__Host-user_session_same_site", value)
        self._set_cookie("logged_in", "yes")

    def login(self, login, password):
        _, _, page = self.request("GET", f"{self.base}/login")
        match = re.search(rb'name="authenticity_token" value="([^"]+)"', page)
        if not match:
            raise RuntimeError("could not find the sign-in form at /login; is built-in or LDAP authentication enabled?")
        form = urllib.parse.urlencode({
            "commit": "Sign in",
            "authenticity_token": html.unescape(match.group(1).decode()),
            "login": login,
            "password": password,
        }).encode()
        _, final_url, _ = self.request("POST", f"{self.base}/session", form, {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.base,
            "Referer": f"{self.base}/login",
        })
        if "two-factor" in final_url or "/sessions/" in urllib.parse.urlparse(final_url).path:
            raise RuntimeError("sign-in requires two-factor authentication; use GHES_WEB_SESSION instead")

    def verify_session(self):
        _, final_url, page = self.request("GET", f"{self.base}/settings/profile")
        if "/login" in urllib.parse.urlparse(final_url).path or self._cookie("logged_in") != "yes":
            raise RuntimeError("web sign-in failed; check credentials or the session cookie")
        match = re.search(rb'<meta name="user-login" content="([^"]*)"', page)
        return match.group(1).decode() if match else "unknown"

    def _fetch_csrf(self, org, repo):
        _, _, page = self.request("GET", f"{self.base}/{org}/{repo}/issues/new")
        match = re.search(rb'class="js-data-upload-policy-url-csrf"[^>]*value="([^"]+)"', page) or re.search(
            rb'value="([^"]+)"[^>]*class="js-data-upload-policy-url-csrf"', page
        )
        if not match:
            raise RuntimeError("could not find an upload policy CSRF token on the new issue page")
        return html.unescape(match.group(1).decode())

    def _absolute(self, url):
        return urllib.parse.urljoin(self.base + "/", url)

    def upload_image(self, org, repo, repo_id, name, data):
        web_headers = {
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "GitHub-Verified-Fetch": "true",
            "Origin": self.base,
            "Referer": f"{self.base}/{org}/{repo}/issues/new",
        }

        # Step 1: create the upload policy.
        def create_policy():
            fields = [("name", name), ("size", len(data)), ("content_type", "image/png"), ("repository_id", repo_id)]
            if self.use_csrf:
                fields.append(("authenticity_token", self.csrf_token))
            body, content_type = _encode_multipart(fields)
            return self.request("POST", f"{self.base}/upload/policies/assets", body, {**web_headers, "Content-Type": content_type})

        try:
            _, _, raw = self.with_retries("upload policy", create_policy)
        except RuntimeError as err:
            # Older releases do not accept verified fetch here; retry once with the page's CSRF token.
            if self.use_csrf or not re.search(r"HTTP (403|422)", str(err)) or '"errors"' in str(err):
                raise
            with self.lock:
                if not self.use_csrf:
                    self.csrf_token = self._fetch_csrf(org, repo)
                    self.use_csrf = True
            _, _, raw = self.with_retries("upload policy", create_policy)
        policy = json.loads(raw)

        # Step 2: upload the bytes to the storage endpoint named by the policy.
        upload_url = self._absolute(policy["upload_url"])
        fields = list((policy.get("form") or {}).items())
        if policy.get("same_origin") and policy.get("upload_authenticity_token"):
            fields.append(("authenticity_token", policy["upload_authenticity_token"]))
        body, content_type = _encode_multipart(fields, ("file", name, "image/png", data))
        headers = {**web_headers, **(policy.get("header") or {}), "Content-Type": content_type}
        _, _, uploaded_raw = self.with_retries("upload file", lambda: self.request("POST", upload_url, body, headers))

        # Step 3: mark the asset uploaded when the storage backend requires it.
        if policy.get("asset_upload_url"):
            fields = [("authenticity_token", policy.get("asset_upload_authenticity_token", ""))]
            body, content_type = _encode_multipart(fields)
            asset_url = self._absolute(policy["asset_upload_url"])
            self.with_retries("finalize upload", lambda: self.request(
                "PUT", asset_url, body, {**web_headers, "Content-Type": content_type}
            ))

        asset = policy.get("asset")
        href = asset.get("href") if isinstance(asset, dict) else None
        if not href:
            try:
                uploaded = json.loads(uploaded_raw)
            except (ValueError, UnicodeDecodeError):
                raise RuntimeError("upload succeeded but neither the policy nor the upload response provided an asset href") from None
            href = uploaded.get("href") if isinstance(uploaded, dict) else None
        if not isinstance(href, str) or not href:
            raise RuntimeError("upload succeeded but neither the policy nor the upload response provided an asset href")
        return self._absolute(href)


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.issues = 0
        self.images = 0
        self.bytes = 0
        self.failures = 0

    def add(self, **counts):
        with self.lock:
            for key, value in counts.items():
                setattr(self, key, getattr(self, key) + value)


def seed_issue(args, ghes, repo_id, run_id, index, stats, manifest):
    rng = random.Random(f"{run_id}-{index}")
    images = []
    for image_index in range(1, args.images + 1):
        size = rng.randint(*args.size_kb)
        name = f"sim-{run_id}-{index:05d}-{image_index:03d}.png"
        data = generate_image.generate_png(target_bytes=size, seed=f"{run_id}-{index}-{image_index}")
        if args.save_dir:
            (args.save_dir / name).write_bytes(data)
        href = None if args.dry_run else ghes.upload_image(args.org, args.repo, repo_id, name, data)
        images.append({"name": name, "bytes": len(data), "href": href})
        stats.add(images=1, bytes=len(data))

    title = f"{args.title_prefix} {run_id} #{index:05d}"
    body_lines = [
        f"Generated by `seed-issue-attachments.py` run `{run_id}`.",
        "",
        f"{len(images)} image(s), {sum(i['bytes'] for i in images)} bytes total.",
        "",
    ]
    body_lines += [f"![{i['name']}]({i['href'] or i['name']})" for i in images]
    number = None
    if not args.dry_run:
        issue = ghes.api_call("POST", f"/repos/{args.org}/{args.repo}/issues", {"title": title, "body": "\n".join(body_lines)})
        number = issue["number"]
    stats.add(issues=1)
    record = {"run_id": run_id, "index": index, "issue": number, "images": images}
    with stats.lock:
        manifest.write(json.dumps(record) + "\n")
        manifest.flush()
    return number, images


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--repo", help="target repository (default: repo from the config)")
    parser.add_argument("-n", "--issues", type=int, default=10, help="number of issues to create (default: 10)")
    parser.add_argument("-i", "--images", type=int, default=3, help="images per issue, 1-100 (default: 3)")
    parser.add_argument("-s", "--size-kb", type=generate_image.parse_size_kb, default=generate_image.parse_size_kb("256"),
                        help="image size in KB, or MIN-MAX for a random size per image, 1-10240 (default: 256)")
    parser.add_argument("-p", "--workers", type=int, default=4, help="concurrent issue workers, 1-32 (default: 4)")
    parser.add_argument("-t", "--title-prefix", default="Attachment load test", help="issue title prefix")
    parser.add_argument("-c", "--config", type=Path, default=DEFAULT_CONF, help="config file (default: ./.gh-api-examples.conf)")
    parser.add_argument("-k", "--insecure", action="store_true", help="skip TLS verification (also enabled by -k in curl_custom_flags)")
    parser.add_argument("--save-dir", type=Path, help="also write each generated image to this directory")
    parser.add_argument("--dry-run", action="store_true", help="generate images and report totals without contacting GHES")
    parser.add_argument("-v", "--verbose", action="store_true", help="log each HTTP request")
    args = parser.parse_args()

    if not 1 <= args.issues <= 100000:
        parser.error("-n must be between 1 and 100000")
    if not 1 <= args.images <= 100:
        parser.error("-i must be between 1 and 100")
    if not 1 <= args.workers <= 32:
        parser.error("-p must be between 1 and 32")
    return args


def main():
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    run_id = time.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    if args.save_dir:
        args.save_dir.mkdir(parents=True, exist_ok=True)

    ghes = None
    repo_id = None
    if args.dry_run:
        args.org = "dry-run"
        args.repo = args.repo or "dry-run"
    else:
        conf = load_conf(args.config)
        for key in ("hostname", "org", "repo", "GITHUB_TOKEN", "GITHUB_API_BASE_URL"):
            if not conf.get(key):
                sys.exit(f"ERROR: {key} not set in {args.config}")
        args.org = conf["org"]
        args.repo = args.repo or conf["repo"]
        curl_flags = conf.get("curl_custom_flags", "").split()
        insecure = args.insecure or "-k" in curl_flags or "--insecure" in curl_flags
        ghes = Ghes(conf, insecure, args.verbose)

        print(f">>> Resolving {args.org}/{args.repo}")
        repo_id = ghes.api_call("GET", f"/repos/{args.org}/{args.repo}")["id"]

        print(">>> Starting web session for uploads")
        session = os.environ.get("GHES_WEB_SESSION") or conf.get("web_session_cookie")
        login = os.environ.get("GHES_WEB_LOGIN") or conf.get("web_login")
        password = os.environ.get("GHES_WEB_PASSWORD") or conf.get("web_password")
        try:
            if session:
                ghes.use_session_cookie(session)
            elif login and password:
                ghes.login(login, password)
            else:
                sys.exit("ERROR: set GHES_WEB_SESSION, or GHES_WEB_LOGIN and GHES_WEB_PASSWORD (see --help)")
            print(f"    OK: signed in as {ghes.verify_session()}")
        except (RuntimeError, urllib.error.URLError) as err:
            sys.exit(f"ERROR: {err}")

    lo, hi = args.size_kb
    print(f">>> Creating {args.issues} issue(s) x {args.images} image(s), "
          f"{lo // 1024}-{hi // 1024} KB each, {args.workers} worker(s), run {run_id}")

    Path("tmp").mkdir(exist_ok=True)
    manifest_path = Path("tmp") / f"issue-attachments-{run_id}.jsonl"
    stats = Stats()
    start = time.monotonic()
    with manifest_path.open("w") as manifest, concurrent.futures.ThreadPoolExecutor(args.workers) as pool:
        futures = {
            pool.submit(seed_issue, args, ghes, repo_id, run_id, index, stats, manifest): index
            for index in range(1, args.issues + 1)
        }
        try:
            for future in concurrent.futures.as_completed(futures):
                index = futures[future]
                try:
                    number, images = future.result()
                    label = f"#{number}" if number else f"(dry run {index})"
                    print(f"    OK: issue {label}: {len(images)} image(s), {sum(i['bytes'] for i in images)} bytes")
                except Exception as err:  # noqa: BLE001 - report and keep seeding other issues
                    stats.add(failures=1)
                    print(f"    FAIL: issue {index}: {err}", file=sys.stderr)
        except KeyboardInterrupt:
            print("Interrupted; waiting for in-flight uploads to finish", file=sys.stderr)
            for future in futures:
                future.cancel()
            raise

    elapsed = time.monotonic() - start
    mib = stats.bytes / 1024 / 1024
    print("")
    print(f"Issues created:  {stats.issues}")
    print(f"Images uploaded: {stats.images}" + (" (dry run, not uploaded)" if args.dry_run else ""))
    print(f"Bytes uploaded:  {stats.bytes} ({mib:.1f} MiB)")
    print(f"Failed issues:   {stats.failures}")
    print(f"Elapsed:         {elapsed:.1f}s ({mib / elapsed if elapsed else 0:.2f} MiB/s)")
    print(f"Manifest:        {manifest_path}")
    sys.exit(1 if stats.failures else 0)


if __name__ == "__main__":
    main()
