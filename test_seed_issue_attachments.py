import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock


spec = importlib.util.spec_from_file_location(
    "seed_issue_attachments", Path(__file__).with_name("seed-issue-attachments.py")
)
seed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed)


class UploadImageTests(unittest.TestCase):
    def upload(self, policy, response):
        ghes = seed.Ghes({
            "hostname": "ghes.example.com",
            "GITHUB_API_BASE_URL": "https://ghes.example.com/api/v3",
            "GITHUB_TOKEN": "test-token",
        }, False, False)
        ghes.request = Mock(side_effect=[
            (200, "", json.dumps(policy).encode()),
            (201, "", response),
            (200, "", b""),
        ])
        return ghes.upload_image("org", "repo", 1, "image.png", b"image"), ghes

    def test_policy_href_with_finalization(self):
        href, ghes = self.upload({
            "upload_url": "/upload",
            "asset": {"href": "/user-attachments/assets/legacy"},
            "asset_upload_url": "/finalize",
        }, b"")
        self.assertEqual(href, "https://ghes.example.com/user-attachments/assets/legacy")
        self.assertEqual(ghes.request.call_args.args[0], "PUT")

    def test_upload_response_href(self):
        href, _ = self.upload(
            {"upload_url": "/upload", "asset": {"name": "image.png"}},
            b'{"href":"https://ghes.example.com/user-attachments/assets/current"}',
        )
        self.assertEqual(href, "https://ghes.example.com/user-attachments/assets/current")

    def test_missing_href_is_explicit(self):
        for response in (b"{}", b"[]", b'{"href":123}'):
            with self.subTest(response=response):
                with self.assertRaisesRegex(RuntimeError, "contained an asset href"):
                    self.upload({"upload_url": "/upload", "asset": {}}, response)

    def test_invalid_upload_metadata_is_explicit(self):
        with self.assertRaisesRegex(RuntimeError, "valid asset metadata"):
            self.upload({"upload_url": "/upload", "asset": {}}, b"not-json")


if __name__ == "__main__":
    unittest.main()
