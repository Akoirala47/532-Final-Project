#!/usr/bin/env python3
"""
Download and preprocess books from Project Gutenberg.

Fetches ~20 classic novels as plain text, strips the Gutenberg header/footer
boilerplate, and saves cleaned files into data/corpus/.
"""

import os
import re
from typing import Optional
import requests

# Curated list of Project Gutenberg book IDs and titles
BOOKS = {
    1342: "Pride and Prejudice",
    11: "Alice in Wonderland",
    1661: "Sherlock Holmes",
    84: "Frankenstein",
    2701: "Moby Dick",
    1952: "The Yellow Wallpaper",
    98: "A Tale of Two Cities",
    174: "The Picture of Dorian Gray",
    1080: "A Modest Proposal",
    2542: "The Strange Case of Dr Jekyll and Mr Hyde",
    74: "Tom Sawyer",
    76: "Huckleberry Finn",
    345: "Dracula",
    1260: "Jane Eyre",
    16328: "Beowulf",
    2591: "Grimms Fairy Tales",
    1400: "Great Expectations",
    514: "Little Women",
    6130: "The Iliad",
    5200: "Metamorphosis",
}

GUTENBERG_URL = "https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
CORPUS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corpus")


def strip_gutenberg_boilerplate(text: str) -> str:
    """Remove Project Gutenberg header and footer from the text."""
    # Find the start marker
    start_match = re.search(
        r"\*\*\*\s*START OF (THE|THIS) PROJECT GUTENBERG EBOOK.*?\*\*\*",
        text,
        re.IGNORECASE,
    )
    # Find the end marker
    end_match = re.search(
        r"\*\*\*\s*END OF (THE|THIS) PROJECT GUTENBERG EBOOK.*?\*\*\*",
        text,
        re.IGNORECASE,
    )

    start_idx = start_match.end() if start_match else 0
    end_idx = end_match.start() if end_match else len(text)

    return text[start_idx:end_idx].strip()


def download_book(book_id: int, title: str) -> Optional[str]:
    """Download a single book from Project Gutenberg. Returns cleaned text or None."""
    url = GUTENBERG_URL.format(book_id=book_id)
    print(f"  Downloading {title} (ID: {book_id})...", end=" ")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        cleaned = strip_gutenberg_boilerplate(resp.text)
        print(f"OK ({len(cleaned):,} chars)")
        return cleaned
    except requests.RequestException as e:
        print(f"FAILED ({e})")
        return None


def main():
    os.makedirs(CORPUS_DIR, exist_ok=True)

    print(f"Downloading {len(BOOKS)} books from Project Gutenberg...\n")
    success_count = 0
    total_words = 0

    for book_id, title in BOOKS.items():
        text = download_book(book_id, title)
        if text is None:
            continue

        # Save to corpus directory
        safe_name = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_").lower()
        filepath = os.path.join(CORPUS_DIR, f"{safe_name}.txt")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)

        word_count = len(text.split())
        total_words += word_count
        success_count += 1

    print(f"\n{'='*50}")
    print(f"Download complete!")
    print(f"  Books downloaded: {success_count}/{len(BOOKS)}")
    print(f"  Total words:      {total_words:,}")
    print(f"  Corpus directory: {CORPUS_DIR}")


if __name__ == "__main__":
    main()
