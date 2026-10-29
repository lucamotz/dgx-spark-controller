# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Offline tests for the allowlisted catalog HTTP client and TTL cache."""

import email.message
import http.client
import io
import unittest
import urllib.error
import urllib.request
import urllib.response
from concurrent.futures import ThreadPoolExecutor

from controller.catalog import http as catalog_http
from controller.catalog.http import (
    ALLOWED_HOSTS,
    USER_AGENT,
    AllowlistRedirectHandler,
    CatalogError,
    TTLCache,
    bearer_header,
    build_opener,
    check_url,
    get_json,
)

SECRET = "hf_TOPSECRETtokenValue123"
HF_URL = "https://huggingface.co/api/models/Qwen/Qwen3-8B?blobs=true"


def _headers(values=None):
    message = email.message.Message()
    for name, value in (values or {}).items():
        message[name] = value
    return message


class FakeResponse:
    def __init__(
        self, body=b"{}", *, status=200, headers=None, url=HF_URL, read_error=None
    ):
        self.body = body
        self.status = status
        self.headers = _headers(headers)
        self.url = url
        self.read_error = read_error
        self.read_sizes = []
        self.closed = False

    def geturl(self):
        return self.url

    def read(self, size=-1):
        self.read_sizes.append(size)
        if self.read_error is not None:
            raise self.read_error
        return self.body if size < 0 else self.body[:size]

    def close(self):
        self.closed = True


class FakeOpener:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append((request, timeout))
        if self.error is not None:
            raise self.error
        return self.response


def _http_error(code, headers=None, url=HF_URL):
    return urllib.error.HTTPError(
        url, code, "error", _headers(headers), io.BytesIO(b'{"error": "x"}')
    )


class FakeHTTPSHandler(urllib.request.BaseHandler):
    """Socket-free HTTPS layer so the real redirect/error processors run."""

    def __init__(self, routes):
        self.routes = routes
        self.requests = []

    def https_open(self, request):
        self.requests.append((request.full_url, dict(request.header_items())))
        status, headers, body = self.routes[request.full_url]
        response = urllib.response.addinfourl(
            io.BytesIO(body), _headers(headers), request.full_url, status
        )
        response.msg = "OK" if status == 200 else "Redirect"
        return response


class CheckUrlTests(unittest.TestCase):
    def test_allowlisted_https_hosts_are_accepted(self):
        for host in sorted(ALLOWED_HOSTS):
            with self.subTest(host=host):
                self.assertEqual(host, check_url(f"https://{host}/x?y=1").hostname)

    def test_policy_violations_are_rejected(self):
        rejected = [
            None,
            7,
            "",
            "https://" + "a" * 3000,
            "http://huggingface.co/api/models/x/y",
            "ftp://huggingface.co/x",
            "file:///etc/passwd",
            "huggingface.co/api",
            "https://evil.example/x",
            "https://huggingface.co.evil.example/x",
            "https://cdn-lfs.hf.co/x",
            "https://cas-bridge.xethub.hf.co/x",
            "https://hf.co/x",
            "https://user:pw@huggingface.co/x",
            "https://huggingface.co@evil.example/x",
            "https://huggingface.co:443/x",
            "https://huggingface.co:8443/x",
            "https://huggingface.co./x",
            "https://[::1]/x",
            "https://huggingface.co/x y",
            "https://huggingface.co/\nx",
            "https://huggingface.co\\@evil.example/",
        ]
        for url in rejected:
            with self.subTest(url=url), self.assertRaises(CatalogError) as context:
                check_url(url)
            self.assertEqual("host_not_allowed", context.exception.code)


class GetJsonTests(unittest.TestCase):
    def test_success_sends_identity_user_agent_and_caller_headers(self):
        opener = FakeOpener(FakeResponse(b'{"sha": "abc", "n": [1, 2]}'))
        result = get_json(
            HF_URL,
            headers={"Authorization": f"Bearer {SECRET}"},
            opener=opener,
            timeout=3,
        )
        self.assertEqual({"sha": "abc", "n": [1, 2]}, result)
        request, timeout = opener.requests[0]
        self.assertEqual(3, timeout)
        self.assertEqual("GET", request.get_method())
        self.assertEqual(USER_AGENT, request.get_header("User-agent"))
        self.assertEqual("dgx-spark-controller/0.2", USER_AGENT)
        self.assertEqual("identity", request.get_header("Accept-encoding"))
        self.assertEqual(f"Bearer {SECRET}", request.get_header("Authorization"))
        self.assertTrue(opener.response.closed)

    def test_caller_accept_header_overrides_default(self):
        opener = FakeOpener(FakeResponse(b"[]", url="https://api.github.com/x"))
        get_json(
            "https://api.github.com/x",
            headers={"Accept": "application/vnd.github+json"},
            opener=opener,
        )
        self.assertEqual(
            "application/vnd.github+json", opener.requests[0][0].get_header("Accept")
        )

    def test_body_is_bounded(self):
        exact = b'"' + b"a" * 8 + b'"'
        response = FakeResponse(exact)
        self.assertEqual(
            "a" * 8, get_json(HF_URL, max_bytes=len(exact), opener=FakeOpener(response))
        )
        self.assertEqual([len(exact) + 1], response.read_sizes)
        with self.assertRaises(CatalogError) as context:
            get_json(
                HF_URL, max_bytes=len(exact) - 1, opener=FakeOpener(FakeResponse(exact))
            )
        self.assertEqual("response_too_large", context.exception.code)

    def test_declared_oversized_body_is_rejected_before_reading(self):
        response = FakeResponse(b"{}", headers={"Content-Length": str(10**12)})
        with self.assertRaises(CatalogError) as context:
            get_json(HF_URL, max_bytes=1024, opener=FakeOpener(response))
        self.assertEqual("response_too_large", context.exception.code)
        self.assertEqual([], response.read_sizes)

    def test_unusable_bodies_are_upstream_unavailable(self):
        bodies = [
            b"",
            b"not json",
            b"{",
            b"\xff\xfe\x00garbage",
            b"NaN",
            b'{"a": Infinity}',
            b'{"a": -Infinity}',
            b"1" * 5000,
            b"[" * 100000 + b"]" * 100000,
        ]
        for body in bodies:
            with (
                self.subTest(body=body[:20]),
                self.assertRaises(CatalogError) as context,
            ):
                get_json(HF_URL, opener=FakeOpener(FakeResponse(body)))
            self.assertEqual("upstream_unavailable", context.exception.code)

    def test_unexpected_status_or_final_url_fails_closed(self):
        with self.assertRaises(CatalogError) as context:
            get_json(HF_URL, opener=FakeOpener(FakeResponse(b"{}", status=204)))
        self.assertEqual("upstream_unavailable", context.exception.code)
        with self.assertRaises(CatalogError) as context:
            get_json(
                HF_URL,
                opener=FakeOpener(FakeResponse(b"{}", url="https://evil.example/x")),
            )
        self.assertEqual("host_not_allowed", context.exception.code)

    def test_disallowed_url_never_reaches_the_opener(self):
        opener = FakeOpener(FakeResponse())
        with self.assertRaises(CatalogError) as context:
            get_json("https://evil.example/api", opener=opener)
        self.assertEqual("host_not_allowed", context.exception.code)
        self.assertEqual([], opener.requests)

    def test_http_status_mapping(self):
        cases = [
            (401, {}, "upstream_unauthorized"),
            (403, {}, "upstream_unauthorized"),
            (404, {}, "not_found"),
            (429, {"Retry-After": "60"}, "upstream_rate_limited"),
            (403, {"X-RateLimit-Remaining": "0"}, "upstream_rate_limited"),
            (403, {"X-RateLimit-Remaining": "12"}, "upstream_unauthorized"),
            (400, {}, "upstream_unavailable"),
            (500, {}, "upstream_unavailable"),
            (503, {}, "upstream_unavailable"),
        ]
        for status, headers, code in cases:
            with (
                self.subTest(status=status, headers=headers),
                self.assertRaises(CatalogError) as context,
            ):
                get_json(HF_URL, opener=FakeOpener(error=_http_error(status, headers)))
            self.assertEqual(code, context.exception.code)
            self.assertEqual(status, context.exception.status)

    def test_upstream_error_code_is_sanitized(self):
        with self.assertRaises(CatalogError) as context:
            get_json(
                HF_URL,
                opener=FakeOpener(
                    error=_http_error(401, {"X-Error-Code": "GatedRepo"})
                ),
            )
        self.assertEqual("GatedRepo", context.exception.upstream_code)
        with self.assertRaises(CatalogError) as context:
            get_json(
                HF_URL,
                opener=FakeOpener(
                    error=_http_error(
                        401, {"X-Error-Code": "<script>alert(1)</script>"}
                    )
                ),
            )
        self.assertIsNone(context.exception.upstream_code)

    def test_transport_failures_are_upstream_unavailable(self):
        errors = [
            urllib.error.URLError("dns failure"),
            urllib.error.URLError(TimeoutError("timed out")),
            TimeoutError("timed out"),
            ConnectionResetError("reset"),
            http.client.RemoteDisconnected("closed"),
            OSError("ssl"),
        ]
        for error in errors:
            with (
                self.subTest(error=type(error).__name__),
                self.assertRaises(CatalogError) as context,
            ):
                get_json(HF_URL, opener=FakeOpener(error=error))
            self.assertEqual("upstream_unavailable", context.exception.code)
        for error in (
            http.client.IncompleteRead(b"{"),
            TimeoutError("slow body"),
            ConnectionResetError("reset"),
        ):
            with (
                self.subTest(read_error=type(error).__name__),
                self.assertRaises(CatalogError) as context,
            ):
                get_json(HF_URL, opener=FakeOpener(FakeResponse(read_error=error)))
            self.assertEqual("upstream_unavailable", context.exception.code)

    def test_header_values_never_appear_in_errors(self):
        failures = [
            FakeOpener(error=_http_error(401)),
            FakeOpener(error=_http_error(500)),
            FakeOpener(error=urllib.error.URLError("boom")),
            FakeOpener(FakeResponse(b"broken")),
            FakeOpener(FakeResponse(b"x" * 100)),
        ]
        for opener in failures:
            with self.assertRaises(CatalogError) as context:
                get_json(
                    HF_URL,
                    max_bytes=50,
                    headers={"Authorization": f"Bearer {SECRET}"},
                    opener=opener,
                )
            rendered = " ".join(
                [
                    str(context.exception),
                    repr(context.exception),
                    context.exception.remediation,
                ]
            )
            self.assertNotIn(SECRET, rendered)
            self.assertIsNone(context.exception.__cause__)

    def test_invalid_header_is_rejected_without_echoing_value(self):
        for headers in (
            {"Authorization": f"Bearer {SECRET}\r\nX-Injected: 1"},
            {"Bad Header": SECRET},
            {"Authorization": 42},
        ):
            with (
                self.subTest(headers=type(headers).__name__),
                self.assertRaises(CatalogError) as context,
            ):
                get_json(HF_URL, headers=headers, opener=FakeOpener(FakeResponse()))
            self.assertEqual("invalid_request", context.exception.code)
            self.assertNotIn(SECRET, str(context.exception))

    def test_argument_validation(self):
        for kwargs in (
            {"timeout": 0},
            {"timeout": -1},
            {"timeout": True},
            {"timeout": 1000},
            {"max_bytes": 0},
            {"max_bytes": 1.5},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                get_json(HF_URL, opener=FakeOpener(FakeResponse()), **kwargs)

    def test_bearer_header_rejects_malformed_tokens_without_echo(self):
        self.assertEqual(
            {"Authorization": f"Bearer {SECRET}"},
            bearer_header(SECRET, service="Hugging Face", env_name="HF_TOKEN"),
        )
        for token in (
            "",
            f"{SECRET}\n",
            f"{SECRET} x",
            "x" * 513,
            None,
            5,
            f"{SECRET}é",
        ):
            with (
                self.subTest(token=repr(token)[:20]),
                self.assertRaises(CatalogError) as context,
            ):
                bearer_header(token, service="Hugging Face", env_name="HF_TOKEN")
            self.assertEqual("invalid_token", context.exception.code)
            self.assertNotIn(
                SECRET, str(context.exception) + context.exception.remediation
            )


class RedirectTests(unittest.TestCase):
    def _get(self, routes, url=HF_URL, headers=None):
        handler = FakeHTTPSHandler(routes)
        return handler, (
            lambda: get_json(url, headers=headers, opener=build_opener(handler))
        )

    def test_same_host_relative_redirect_keeps_authorization(self):
        cache_path = "/api/resolve-cache/models/Qwen/Qwen3-8B/abc/config.json?etag=1"
        target = f"https://huggingface.co{cache_path}"
        handler, call = self._get(
            {
                HF_URL: (307, {"Location": cache_path}, b""),
                target: (
                    200,
                    {"Content-Type": "application/json"},
                    b'{"model_type": "qwen3"}',
                ),
            },
            headers={"Authorization": f"Bearer {SECRET}"},
        )
        self.assertEqual({"model_type": "qwen3"}, call())
        self.assertEqual([HF_URL, target], [url for url, _ in handler.requests])
        self.assertEqual(
            f"Bearer {SECRET}", handler.requests[1][1].get("Authorization")
        )

    def test_redirect_to_unlisted_or_insecure_target_is_refused_before_request(self):
        for location in (
            "https://cdn-lfs.hf.co/repos/x/config.json",
            "https://evil.example/steal",
            "http://huggingface.co/api/models/x/y",
            "https://huggingface.co:8443/x",
        ):
            handler, call = self._get(
                {HF_URL: (302, {"Location": location}, b"")},
                headers={"Authorization": f"Bearer {SECRET}"},
            )
            with (
                self.subTest(location=location),
                self.assertRaises(CatalogError) as context,
            ):
                call()
            self.assertEqual("host_not_allowed", context.exception.code)
            self.assertEqual([HF_URL], [url for url, _ in handler.requests])

    def test_cross_host_redirect_drops_authorization(self):
        target = "https://api.github.com/repos/x"
        handler, call = self._get(
            {
                HF_URL: (302, {"Location": target}, b""),
                target: (200, {}, b"[]"),
            },
            headers={"Authorization": f"Bearer {SECRET}"},
        )
        self.assertEqual([], call())
        self.assertNotIn("Authorization", handler.requests[1][1])

    def test_redirect_loops_are_bounded(self):
        _, call = self._get({HF_URL: (302, {"Location": HF_URL}, b"")})
        with self.assertRaises(CatalogError) as context:
            call()
        self.assertEqual("upstream_unavailable", context.exception.code)
        hops = {
            f"https://huggingface.co/hop/{index}": (
                302,
                {"Location": f"/hop/{index + 1}"},
                b"",
            )
            for index in range(20)
        }
        hops[HF_URL] = (302, {"Location": "/hop/0"}, b"")
        handler, call = self._get(hops)
        with self.assertRaises(CatalogError):
            call()
        self.assertLessEqual(len(handler.requests), catalog_http.MAX_REDIRECTS + 1)

    def test_real_error_processing_maps_status(self):
        _, call = self._get({HF_URL: (404, {"X-Error-Code": "RepoNotFound"}, b"{}")})
        with self.assertRaises(CatalogError) as context:
            call()
        self.assertEqual(
            ("not_found", "RepoNotFound"),
            (context.exception.code, context.exception.upstream_code),
        )

    def test_handler_is_used_by_default_opener(self):
        opener = build_opener()
        self.assertTrue(
            any(
                isinstance(handler, AllowlistRedirectHandler)
                for handler in opener.handlers
            )
        )
        self.assertFalse(
            any(
                isinstance(handler, urllib.request.HTTPHandler)
                for handler in opener.handlers
            )
        )


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class TTLCacheTests(unittest.TestCase):
    def test_none_values_are_cached(self):
        cache = TTLCache(10, clock=FakeClock())
        calls = []
        for _ in range(3):
            self.assertIsNone(cache.get_or_compute("k", lambda: calls.append(1)))
        self.assertEqual(1, len(calls))

    def test_get_or_compute_recomputes_after_expiry_and_never_caches_errors(self):
        clock = FakeClock()
        cache = TTLCache(5, clock=clock)
        calls = []

        def compute():
            calls.append(1)
            return len(calls)

        self.assertEqual(1, cache.get_or_compute("k", compute))
        self.assertEqual(1, cache.get_or_compute("k", compute))
        clock.now += 5
        self.assertEqual(2, cache.get_or_compute("k", compute))

        def fail():
            raise CatalogError("upstream_unavailable", "down")

        with self.assertRaises(CatalogError):
            cache.get_or_compute("bad", fail)
        self.assertEqual("ok", cache.get_or_compute("bad", lambda: "ok"))

    def test_bounded_entries_evict_least_recently_used(self):
        cache = TTLCache(100, max_entries=2, clock=FakeClock())
        computed = []
        for key in ("a", "b", "a", "c", "b", "a"):
            cache.get_or_compute(key, lambda key=key: computed.append(key))
        # Reading "a" made "b" the least recently used entry when "c" arrived.
        self.assertEqual(["a", "b", "c", "b", "a"], computed)

    def test_constructor_validation(self):
        for args, kwargs in (
            ((0,), {}),
            ((-1,), {}),
            ((True,), {}),
            ((1,), {"max_entries": 0}),
            ((1,), {"max_entries": 1.5}),
        ):
            with self.subTest(args=args, kwargs=kwargs), self.assertRaises(ValueError):
                TTLCache(*args, **kwargs)

    def test_concurrent_eviction_keeps_every_value_intact(self):
        cache = TTLCache(100, max_entries=50)

        def worker(offset):
            for index in range(500):
                key = (offset, index % 60)
                self.assertEqual(key, cache.get_or_compute(key, lambda key=key: key))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(8)))


if __name__ == "__main__":
    unittest.main()
