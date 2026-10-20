import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import Mock


spec = importlib.util.spec_from_file_location(
    "seeder", Path(__file__).with_name("seed-issue-attachments.py")
)
seeder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seeder)


class UploadImageTests(unittest.TestCase):
    def upload(self, policy, response, finalize=False):
        ghes = seeder.Ghes(
            {"hostname": "ghes.example", "GITHUB_API_BASE_URL": "https://ghes.example/api/v3",
             "GITHUB_TOKEN": "test"}, False, False
        )
        responses = [(200, "", json.dumps(policy).encode()), (200, "", response)]
        if finalize:
            responses.append((204, "", b""))
        ghes.request = Mock(side_effect=responses)
        return ghes, lambda: ghes.upload_image("org", "repo", 1, "test.png", b"png")

    def test_href_from_upload_response(self):
        _, upload = self.upload(
            {"upload_url": "/storage/upload", "asset": {"name": "test.png"}},
            b'{"href": "/user-attachments/assets/example"}',
        )
        self.assertEqual(upload(), "https://ghes.example/user-attachments/assets/example")

    def test_policy_href_with_empty_storage_response(self):
        _, upload = self.upload(
            {"upload_url": "/storage/upload", "asset": {"href": "/assets/example"}}, b""
        )
        self.assertEqual(upload(), "https://ghes.example/assets/example")

    def test_null_policy_asset_uses_upload_response(self):
        _, upload = self.upload(
            {"upload_url": "/storage/upload", "asset": None},
            b'{"href": "/assets/example"}',
        )
        self.assertEqual(upload(), "https://ghes.example/assets/example")

    def test_finalize_is_preserved(self):
        ghes, upload = self.upload(
            {"upload_url": "/storage/upload", "asset": {"href": "/assets/example"},
             "asset_upload_url": "/assets/finalize"}, b"", finalize=True
        )
        self.assertEqual(upload(), "https://ghes.example/assets/example")
        self.assertEqual(ghes.request.call_args.args[:2],
                         ("PUT", "https://ghes.example/assets/finalize"))

    def test_missing_href_is_actionable(self):
        for response in (b"", b"not json", b"{}", b"[]", b'{"href": 42}'):
            with self.subTest(response=response):
                _, upload = self.upload({"upload_url": "/storage/upload", "asset": {}}, response)
                with self.assertRaisesRegex(RuntimeError, "provided an asset href"):
                    upload()


if __name__ == "__main__":
    unittest.main()
