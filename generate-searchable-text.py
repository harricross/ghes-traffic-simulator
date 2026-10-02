#!/usr/bin/env python3

import argparse
import random
from pathlib import Path


SAMPLE_CORPUS = Path(__file__).parent / "test-data" / "searchable-samples.txt"
BOOK_CORPUS = Path(__file__).parent / "test-data" / "books"


def load_samples():
    samples = []
    with SAMPLE_CORPUS.open(encoding="utf-8") as corpus:
        for line in corpus:
            sample_id, separator, text = line.rstrip("\n").partition("|")
            if separator and sample_id and text:
                samples.append((sample_id, text))

    for book in sorted(BOOK_CORPUS.rglob("*.txt")):
        content = book.read_text(encoding="utf-8")
        start_marker = next(
            (line for line in content.splitlines() if line.startswith("*** START OF THE PROJECT GUTENBERG EBOOK ")),
            None,
        )
        if start_marker:
            content = content.split(start_marker, 1)[1]
        end_marker = next(
            (line for line in content.splitlines() if line.startswith("*** END OF THE PROJECT GUTENBERG EBOOK ")),
            None,
        )
        if end_marker:
            content = content.split(end_marker, 1)[0]

        for index, paragraph in enumerate(content.split("\n\n"), start=1):
            text = " ".join(paragraph.split())
            if len(text) >= 100 and not text.startswith("[Illustration"):
                sample_id = f"{book.parent.name}-{index:05d}"
                samples.append((sample_id, text))

    return samples


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic plain text for repository indexing tests."
    )
    parser.add_argument("output", type=Path, help="path to the text file to create")
    parser.add_argument(
        "size_kb",
        type=int,
        help="file size in KB (1-1024)",
    )
    parser.add_argument(
        "document_id",
        help="stable identifier included in the generated searchable text",
    )
    args = parser.parse_args()

    if not 1 <= args.size_kb <= 1024:
        parser.error("size_kb must be between 1 and 1024")

    samples = load_samples()
    if not samples:
        parser.error(f"no usable samples found under {SAMPLE_CORPUS.parent}")

    target_bytes = args.size_kb * 1024
    sample_picker = random.Random(args.document_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb") as output:
        written = 0
        line_number = 0
        while written < target_bytes:
            sample_id, text = sample_picker.choice(samples)
            line = (
                f"dataset=ghes-traffic-simulator document_id={args.document_id} "
                f"record={line_number:06d} sample_id={sample_id} "
                f"searchable=true {text}\n"
            ).encode("utf-8")
            chunk = line[: target_bytes - written]
            try:
                chunk.decode("utf-8")
            except UnicodeDecodeError as error:
                chunk = chunk[: error.start]
            output.write(chunk)
            written += len(chunk)
            line_number += 1


if __name__ == "__main__":
    main()
