"""Run a deterministic round-robin shard of the unit tests and write a JSON report."""

import argparse
import json
import pathlib
import sys
import unittest


def flatten(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from flatten(item)
        else:
            yield item


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--total", type=int, required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    if args.total < 1 or not 0 <= args.shard < args.total:
        parser.error("--shard must be in [0, --total)")

    root = pathlib.Path(__file__).resolve().parent
    tests = sorted(flatten(unittest.defaultTestLoader.discover(str(root))), key=lambda test: test.id())
    selected = [test for index, test in enumerate(tests) if index % args.total == args.shard]
    result = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(unittest.TestSuite(selected))
    report = {
        "shard": args.shard,
        "total": args.total,
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
    }
    pathlib.Path(args.report).write_text(json.dumps(report) + "\n", encoding="utf-8")
    return 0 if result.wasSuccessful() and result.testsRun > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
