# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Offline tests for vLLM release listing and arm64 image pinning."""

import copy
import json
import random
import unittest

from catalog_fakes import JUNK, RouteGetJson, fixture

from controller.catalog import vllm_releases
from controller.catalog.http import CatalogError
from controller.catalog.vllm_releases import (
    IMAGE_REPOSITORY,
    RuntimeImage,
    VllmRelease,
    list_releases,
    resolve_release_image,
)

SECRET = "ghp_TOPSECRETtokenValue123"
RELEASES_URL = "https://api.github.com/repos/vllm-project/vllm/releases?per_page=30"
TAG_URL = "https://hub.docker.com/v2/repositories/vllm/vllm-openai/tags/"
DOWNLOAD = "https://github.com/vllm-project/vllm/releases/download/"
ARM64_V0310 = "sha256:3f7dd5b777d34d1724456ce71f87385dca288c3bb23029ab27dee358f5d2b971"
INDEX_V0310 = "sha256:c1c9f6fd5c109ba7f0546a59f5b2f15fb87f64c77782e90a27b648b42a8e67c3"


def releases():
    return fixture("gh_vllm_releases.json")


def _release(tag, published_at, *, prerelease=False, draft=False, assets=None):
    return {
        "html_url": f"https://github.com/vllm-project/vllm/releases/tag/{tag}",
        "tag_name": tag,
        "name": tag,
        "draft": draft,
        "prerelease": prerelease,
        "published_at": published_at,
        "assets": assets or [],
    }


def _wheel(tag, name, **changes):
    asset = {
        "name": name,
        "state": "uploaded",
        "size": 1000,
        "digest": "sha256:" + "a" * 64,
        "browser_download_url": f"{DOWNLOAD}{tag}/{name.replace('+', '%2B')}",
    }
    asset.update(changes)
    return asset


def _list(data, **kwargs):
    return list_releases(get_json=RouteGetJson({RELEASES_URL: data}), **kwargs)


class ListReleasesTests(unittest.TestCase):
    def test_fixture_releases_are_parsed_newest_first(self):
        upstream = RouteGetJson({RELEASES_URL: releases()})
        result = list_releases(get_json=upstream)
        self.assertEqual(["v0.31.0", "v0.30.0", "v0.29.0"], [r.tag for r in result])
        latest = result[0]
        self.assertEqual(
            ("0.31.0", "v0.31.0", False),
            (latest.version, latest.name, latest.prerelease),
        )
        self.assertEqual("2026-10-05T06:44:55Z", latest.published_at)
        self.assertEqual(
            "https://github.com/vllm-project/vllm/releases/tag/v0.31.0",
            latest.html_url,
        )
        default, cu129 = latest.aarch64_wheels
        self.assertEqual(
            ("default", "vllm-0.31.0-cp38-abi3-manylinux_2_28_aarch64.whl"),
            (default.cuda_variant, default.name),
        )
        self.assertEqual(315390942, default.size_bytes)
        self.assertEqual(
            "56edd19c798510b6da5ae4dd38b3bdf55ea7ae261b339a906df006dc3faf751d",
            default.sha256,
        )
        self.assertEqual(
            f"{DOWNLOAD}v0.31.0/vllm-0.31.0-cp38-abi3-manylinux_2_28_aarch64.whl",
            default.download_url,
        )
        self.assertEqual(
            ("cu129", "vllm-0.31.0+cu129-cp38-abi3-manylinux_2_28_aarch64.whl"),
            (cu129.cuda_variant, cu129.name),
        )
        self.assertIn("%2Bcu129", cu129.download_url)

    def test_request_headers_and_page_size(self):
        upstream = RouteGetJson({RELEASES_URL: releases()})
        list_releases(get_json=upstream)
        url, headers, _ = upstream.calls[0]
        self.assertEqual(RELEASES_URL, url)
        self.assertEqual(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            headers,
        )

    def test_token_is_sent_as_bearer_and_never_echoed(self):
        upstream = RouteGetJson({RELEASES_URL: releases()})
        result = list_releases(token=SECRET, get_json=upstream)
        self.assertEqual(f"Bearer {SECRET}", upstream.calls[0][1]["Authorization"])
        rendered = json.dumps([release.to_dict() for release in result])
        self.assertNotIn(SECRET, rendered)
        for token in (f"{SECRET}\n", "", 5):
            with self.subTest(token=repr(token)[:12]):
                with self.assertRaises(CatalogError) as context:
                    list_releases(token=token, get_json=upstream)
                self.assertEqual("invalid_token", context.exception.code)
                self.assertNotIn(SECRET, str(context.exception))
        denied = RouteGetJson(
            {RELEASES_URL: CatalogError("upstream_unauthorized", "x", status=401)}
        )
        with self.assertRaises(CatalogError) as context:
            list_releases(token=SECRET, get_json=denied)
        self.assertEqual("upstream_unauthorized", context.exception.code)
        self.assertIn("GITHUB_TOKEN", context.exception.remediation)
        self.assertNotIn(SECRET, str(context.exception) + context.exception.remediation)

    def test_upstream_errors_propagate(self):
        for code in ("upstream_rate_limited", "upstream_unavailable", "not_found"):
            upstream = RouteGetJson({RELEASES_URL: CatalogError(code, "x")})
            with self.subTest(code=code), self.assertRaises(CatalogError) as context:
                list_releases(get_json=upstream)
            self.assertEqual(code, context.exception.code)

    def test_drafts_nightlies_and_prereleases(self):
        data = [
            _release("v0.32.0", "2026-10-09T00:00:00Z", draft=True),
            _release("nightly", "2026-10-08T00:00:00Z"),
            _release("v0.32.0-dev", "2026-10-08T00:00:00Z"),
            _release("0.32.0", "2026-10-08T00:00:00Z"),
            _release("v0.32.0rc1", "2026-10-07T00:00:00Z", prerelease=True),
            _release("v0.31.1rc0", "2026-10-06T12:00:00Z"),
            _release("v0.31.1", "2026-10-06T06:00:00Z", prerelease=True),
            _release("v0.11.0.post1", "2025-10-01T00:00:00Z"),
            *releases(),
        ]
        stable = [release.tag for release in _list(data)]
        self.assertEqual(["v0.31.0", "v0.30.0", "v0.29.0", "v0.11.0.post1"], stable)
        everything = _list(data, include_prereleases=True)
        self.assertEqual(
            ["v0.32.0rc1", "v0.31.1rc0", "v0.31.1", "v0.31.0"],
            [release.tag for release in everything[:4]],
        )
        self.assertTrue(all(release.prerelease for release in everything[:3]))

    def test_order_is_by_publication_time_not_input_order(self):
        data = releases()
        random.Random(7).shuffle(data)
        self.assertEqual(
            ["v0.31.0", "v0.30.0", "v0.29.0"], [r.tag for r in _list(data)]
        )

    def test_duplicate_tags_are_returned_once(self):
        data = releases() + releases()
        self.assertEqual(3, len(_list(data)))

    def test_wheel_filtering(self):
        tag = "v0.31.0"
        keep = [
            "vllm-0.31.0-cp38-abi3-manylinux_2_28_aarch64.whl",
            "vllm-0.31.0+cu129-cp38-abi3-manylinux_2_28_aarch64.whl",
            "vllm-0.31.0+cu130-cp38-abi3-manylinux2014_aarch64.whl",
            "vllm-0.31.0-cp312-cp312-manylinux_2_28_aarch64.manylinux_2_31_aarch64.whl",
        ]
        skip = [
            "vllm-0.31.0+cpu-cp38-abi3-manylinux_2_39_aarch64.whl",
            "vllm-0.31.0+xpu-cp38-abi3-manylinux_2_34_x86_64.whl",
            "vllm-0.31.0+rocm700-cp38-abi3-manylinux_2_28_aarch64.whl",
            "vllm-0.31.0+cu129-cp38-abi3-manylinux_2_28_x86_64.whl",
            "vllm-0.31.0+cpu-cp312-cp312-macosx_11_0_arm64.whl",
            "vllm-0.30.0-cp38-abi3-manylinux_2_28_aarch64.whl",
            "vllm-0.31.0-cp38-abi3-manylinux_2_28_aarch64.manylinux_2_28_x86_64.whl",
            "vllm-0.31.0-cp38-abi3-linux_aarch64.whl",
            "vllm-0.31.0.tar.gz",
            "evil-0.31.0-cp38-abi3-manylinux_2_28_aarch64.whl",
        ]
        assets = [_wheel(tag, name) for name in keep + skip]
        assets.append(
            _wheel(
                tag,
                "vllm-0.31.0+cu126-cp38-abi3-manylinux_2_28_aarch64.whl",
                state="open",
            )
        )
        data = [_release(tag, "2026-10-05T06:44:55Z", assets=assets)]
        wheels = _list(data)[0].aarch64_wheels
        self.assertEqual(sorted(keep), sorted(wheel.name for wheel in wheels))
        self.assertEqual("default", wheels[0].cuda_variant)
        self.assertEqual(
            {"default", "cu129", "cu130"}, {wheel.cuda_variant for wheel in wheels}
        )

    def test_missing_digest_is_allowed_but_reported_as_none(self):
        name = "vllm-0.31.0-cp38-abi3-manylinux_2_28_aarch64.whl"
        assets = [_wheel("v0.31.0", name, digest=None)]
        data = [_release("v0.31.0", "2026-10-05T06:44:55Z", assets=assets)]
        self.assertIsNone(_list(data)[0].aarch64_wheels[0].sha256)

    def test_unusable_shapes_are_upstream_unavailable(self):
        name = "vllm-0.31.0-cp38-abi3-manylinux_2_28_aarch64.whl"

        def with_release(**changes):
            entry = _release("v0.31.0", "2026-10-05T06:44:55Z")
            entry.update(changes)
            return [entry]

        def with_wheel(**changes):
            return with_release(assets=[_wheel("v0.31.0", name, **changes)])

        cases = {
            "object": {"message": "Not Found"},
            "string": "releases",
            "release string": ["v0.31.0"],
            "tag int": with_release(tag_name=31),
            "tag missing": with_release(tag_name=None),
            "draft string": with_release(draft="no"),
            "prerelease string": with_release(prerelease="false"),
            "published missing": with_release(published_at=None),
            "published malformed": with_release(published_at="yesterday"),
            "published impossible": with_release(published_at="2026-13-45T25:61:00Z"),
            "page elsewhere": with_release(html_url="https://evil.example/v0.31.0"),
            "assets object": with_release(assets={"a": 1}),
            "asset string": with_release(assets=["wheel"]),
            "asset no name": with_release(assets=[{"size": 1}]),
            "size bool": with_wheel(size=True),
            "size zero": with_wheel(size=0),
            "size huge": with_wheel(size=10**30),
            "size string": with_wheel(size="1000"),
            "digest md5": with_wheel(digest="md5:" + "a" * 32),
            "digest upper": with_wheel(digest="sha256:" + "A" * 64),
            "digest short": with_wheel(digest="sha256:abc"),
            "url elsewhere": with_wheel(
                browser_download_url=f"https://evil.example/{name}"
            ),
            "url other tag": with_wheel(
                browser_download_url=f"{DOWNLOAD}v0.30.0/{name}"
            ),
            "url other file": with_wheel(
                browser_download_url=f"{DOWNLOAD}v0.31.0/other.whl"
            ),
        }
        for label, data in cases.items():
            with self.subTest(label=label), self.assertRaises(CatalogError) as context:
                _list(data)
            self.assertEqual("upstream_unavailable", context.exception.code)

    def test_junk_values_never_escape_as_python_errors(self):
        attempts = 0
        keys = ["tag_name", "name", "draft", "prerelease", "published_at", "html_url"]
        for key in keys + ["assets"]:
            for junk in JUNK:
                data = releases()
                data[0][key] = copy.deepcopy(junk)
                attempts += self._attempt(data)
        for key in ["name", "size", "digest", "state", "browser_download_url"]:
            for junk in JUNK:
                data = releases()
                data[0]["assets"][6][key] = copy.deepcopy(junk)
                attempts += self._attempt(data)
        for junk in JUNK:
            attempts += self._attempt(copy.deepcopy(junk))
        self.assertGreater(attempts, 100)

    def _attempt(self, data):
        try:
            result = _list(data, include_prereleases=True)
        except CatalogError:
            return 1
        self.assertTrue(all(isinstance(item, VllmRelease) for item in result))
        json.dumps([item.to_dict() for item in result])
        return 1

    def test_to_dict_is_json_ready(self):
        data = _list(releases())[0].to_dict()
        self.assertEqual(json.loads(json.dumps(data)), data)
        self.assertEqual(
            {
                "tag",
                "version",
                "name",
                "published_at",
                "prerelease",
                "html_url",
                "aarch64_wheels",
            },
            set(data),
        )
        self.assertEqual(
            {"name", "size_bytes", "sha256", "cuda_variant", "download_url"},
            set(data["aarch64_wheels"][0]),
        )


class ResolveReleaseImageTests(unittest.TestCase):
    def _resolve(self, routes, tag="v0.31.0", **kwargs):
        upstream = RouteGetJson(routes)
        return upstream, resolve_release_image(tag, get_json=upstream, **kwargs)

    def test_default_variant_pins_arm64_manifest(self):
        upstream, image = self._resolve(
            {f"{TAG_URL}v0.31.0": fixture("dockerhub_tag_v0.31.0.json")}
        )
        self.assertIsInstance(image, RuntimeImage)
        self.assertEqual([f"{TAG_URL}v0.31.0"], upstream.urls)
        self.assertEqual(
            ("vllm/vllm-openai", "v0.31.0", "v0.31.0", "default"),
            (image.repository, image.tag, image.release_tag, image.cuda_variant),
        )
        self.assertEqual(
            (INDEX_V0310, ARM64_V0310), (image.index_digest, image.arm64_digest)
        )
        self.assertEqual(f"{IMAGE_REPOSITORY}@{ARM64_V0310}", image.reference)
        self.assertEqual("2026-10-04T01:11:23.53217Z", image.last_updated)
        data = image.to_dict()
        self.assertEqual(json.loads(json.dumps(data)), data)
        self.assertEqual(ARM64_V0310, data["arm64_digest"])

    def test_cuda_variant_selects_suffixed_tag(self):
        _, image = self._resolve(
            {f"{TAG_URL}v0.31.0-cu129": fixture("dockerhub_tag_v0.31.0-cu129.json")},
            cuda_variant="cu129",
        )
        self.assertEqual(("v0.31.0-cu129", "cu129"), (image.tag, image.cuda_variant))
        self.assertEqual(
            "sha256:ff7c43dcb2059e8f3c3235a09b0963cc241c3a05806377f79e089ea2a8eac194",
            image.arm64_digest,
        )
        self.assertTrue(image.reference.endswith(image.arm64_digest))

    def test_single_arch_tag_has_identical_index_and_arm64_digest(self):
        data = fixture("dockerhub_tag_v0.31.0.json")
        data["images"] = [data["images"][1]]
        data["digest"] = ARM64_V0310
        _, image = self._resolve({f"{TAG_URL}v0.31.0": data})
        self.assertEqual(image.index_digest, image.arm64_digest)

    def test_missing_tag_or_arm64_image_is_runtime_image_unavailable(self):
        cases = [
            ("v0.31.0", "cu130", CatalogError("not_found", "x", status=404)),
            ("v0.10.0", "default", fixture("dockerhub_tag_v0.10.0.json")),
        ]
        for tag, variant, response in cases:
            docker_tag = tag if variant == "default" else f"{tag}-{variant}"
            upstream = RouteGetJson({f"{TAG_URL}{docker_tag}": response})
            with (
                self.subTest(tag=docker_tag),
                self.assertRaises(CatalogError) as context,
            ):
                resolve_release_image(tag, cuda_variant=variant, get_json=upstream)
            self.assertEqual("runtime_image_unavailable", context.exception.code)
            self.assertIn("another vLLM release", context.exception.remediation)

    def test_arm64_selection_rules(self):
        base = fixture("dockerhub_tag_v0.31.0.json")
        other = "sha256:" + "b" * 64
        attestation = {
            "architecture": "unknown",
            "os": "unknown",
            "digest": "sha256:" + "c" * 64,
        }
        v8 = dict(base["images"][1], variant="v8")
        v7 = dict(base["images"][1], variant="v7")
        windows = dict(base["images"][1], os="windows", digest=other)
        same_twice = [base["images"][1], dict(base["images"][1])]
        conflicting = [base["images"][1], dict(base["images"][1], digest=other)]
        accepted = [[*base["images"], attestation], [v8], [windows, v8], same_twice]
        for images in accepted:
            data = dict(base, images=images)
            with self.subTest(images=len(images)):
                _, image = self._resolve({f"{TAG_URL}v0.31.0": data})
                self.assertEqual(ARM64_V0310, image.arm64_digest)
        for images in ([v7], [base["images"][0]], [], conflicting):
            data = dict(base, images=images)
            with (
                self.subTest(images=images),
                self.assertRaises(CatalogError) as context,
            ):
                self._resolve({f"{TAG_URL}v0.31.0": data})
            self.assertEqual("runtime_image_unavailable", context.exception.code)

    def test_invalid_input_is_rejected_before_any_request(self):
        upstream = RouteGetJson({})
        tags = [
            "0.31.0",
            "v0.31",
            "v0.31.0-cu129",
            "latest",
            "v0.31.0;rm -rf /",
            "v0.31.0\n",
            " v0.31.0",
            "v0.31.0/../x",
            "",
            None,
            31,
            "v0.31.0.dev1",
        ]
        for tag in tags:
            with self.subTest(tag=tag), self.assertRaises(CatalogError) as context:
                resolve_release_image(tag, get_json=upstream)
            self.assertEqual("invalid_release_tag", context.exception.code)
        for variant in ("cu118", "", None, "default ", "CU129", "cu129;id"):
            with (
                self.subTest(variant=variant),
                self.assertRaises(CatalogError) as context,
            ):
                resolve_release_image(
                    "v0.31.0", cuda_variant=variant, get_json=upstream
                )
            self.assertEqual("invalid_cuda_variant", context.exception.code)
        self.assertEqual([], upstream.calls)

    def test_prerelease_and_post_tags_are_accepted(self):
        for tag in ("v0.12.0rc1", "v0.11.0.post1"):
            data = dict(fixture("dockerhub_tag_v0.31.0.json"), name=tag)
            _, image = self._resolve({f"{TAG_URL}{tag}": data}, tag=tag)
            self.assertEqual(tag, image.release_tag)

    def test_unusable_shapes_are_upstream_unavailable(self):
        base = fixture("dockerhub_tag_v0.31.0.json")
        arm = base["images"][1]
        cases = {
            "list": [],
            "string": "tag",
            "name mismatch": dict(base, name="v0.30.0"),
            "index missing": {k: v for k, v in base.items() if k != "digest"},
            "index upper": dict(base, digest=INDEX_V0310.upper()),
            "index short": dict(base, digest="sha256:abc"),
            "index md5": dict(base, digest="md5:" + "a" * 32),
            "images missing": {k: v for k, v in base.items() if k != "images"},
            "images object": dict(base, images={"arm64": arm}),
            "image string": dict(base, images=["arm64"]),
            "arm64 digest missing": dict(base, images=[dict(arm, digest=None)]),
            "arm64 digest bad": dict(base, images=[dict(arm, digest="sha256:xyz")]),
        }
        for label, data in cases.items():
            with self.subTest(label=label), self.assertRaises(CatalogError) as context:
                self._resolve({f"{TAG_URL}v0.31.0": data})
            self.assertEqual("upstream_unavailable", context.exception.code)

    def test_malformed_last_updated_is_dropped(self):
        for value in (None, 5, "yesterday", "2026-10-04"):
            data = dict(fixture("dockerhub_tag_v0.31.0.json"), last_updated=value)
            with self.subTest(value=value):
                _, image = self._resolve({f"{TAG_URL}v0.31.0": data})
                self.assertIsNone(image.last_updated)

    def test_upstream_errors_propagate(self):
        for code in ("upstream_rate_limited", "upstream_unavailable"):
            upstream = RouteGetJson({f"{TAG_URL}v0.31.0": CatalogError(code, "x")})
            with self.subTest(code=code), self.assertRaises(CatalogError) as context:
                resolve_release_image("v0.31.0", get_json=upstream)
            self.assertEqual(code, context.exception.code)

    def test_junk_values_never_escape_as_python_errors(self):
        base = fixture("dockerhub_tag_v0.31.0.json")
        for key in ("name", "digest", "images", "last_updated"):
            for junk in JUNK:
                self._attempt(dict(base, **{key: copy.deepcopy(junk)}))
        for key in ("architecture", "os", "variant", "digest"):
            for junk in JUNK:
                images = copy.deepcopy(base["images"])
                images[1][key] = copy.deepcopy(junk)
                self._attempt(dict(base, images=images))
        for junk in JUNK:
            self._attempt(copy.deepcopy(junk))

    def _attempt(self, data):
        try:
            _, image = self._resolve({f"{TAG_URL}v0.31.0": data})
        except CatalogError:
            return
        self.assertRegex(image.arm64_digest, r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(f"{IMAGE_REPOSITORY}@{image.arm64_digest}", image.reference)


class ModuleContractTests(unittest.TestCase):
    def test_release_tag_pattern(self):
        good = ["v0.31.0", "v1.0.0", "v0.11.0.post1", "v0.12.0rc1", "v0.9.2.post2rc3"]
        bad = ["v0.31", "0.31.0", "v0.31.0-cu129", "v0.31.0.dev0", "vv0.31.0"]
        for tag in good:
            self.assertRegex(tag, vllm_releases.RELEASE_TAG_RE)
        for tag in bad:
            self.assertNotRegex(tag, vllm_releases.RELEASE_TAG_RE)


if __name__ == "__main__":
    unittest.main()
