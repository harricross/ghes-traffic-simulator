import hashlib
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from loadlab import checks  # noqa: E402


class HelperTests(unittest.TestCase):
    def test_slugify_collapses_separators(self):
        self.assertEqual(checks.slugify("  Hello,  GHES World! "), "hello-ghes-world")

    def test_slugify_falls_back_for_empty_text(self):
        self.assertEqual(checks.slugify("!!!"), "item")

    def test_chunk_splits_with_remainder(self):
        self.assertEqual(checks.chunk([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]])

    def test_chunk_rejects_non_positive_size(self):
        with self.assertRaises(ValueError):
            checks.chunk([1], 0)

    def test_shard_rejects_out_of_range_index(self):
        with self.assertRaises(ValueError):
            checks.shard([1, 2], 2, 2)

    def test_manifest_is_sorted_and_compact(self):
        self.assertEqual(checks.manifest({"b": 2, "a": 1}), '{"a":1,"b":2}')


class VectorTests(unittest.TestCase):
    pass


def _vector_test(number):
    def test(self):
        items = ["Item %d/%d" % (number, i) for i in range(number + 1)]
        total = number % 5 + 1
        parts = [checks.shard(items, index, total) for index in range(total)]
        self.assertEqual(sorted(sum(parts, [])), sorted(items))
        text = checks.manifest({item: checks.slugify(item) for item in items})
        self.assertEqual(checks.digest(text), hashlib.sha256(text.encode("utf-8")).hexdigest())
    return test


# At least 25 cases so every test shard (maximum 25) runs real tests.
for _number in range(32):
    setattr(VectorTests, "test_vector_%02d" % _number, _vector_test(_number))
