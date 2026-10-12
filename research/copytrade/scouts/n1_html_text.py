"""N1 scout: strip an archived docs HTML page to readable text.

Reads one downloaded HTML file (path as argument), drops scripts/styles and
tags, and prints text lines containing any of the given keywords (or all
lines when none are given), with a little context. Mechanics only.

    .venv/bin/python -I scouts/n1_html_text.py FILE [keyword ...]
"""
import html
import re
import sys


def text_of(path):
    raw = open(path, encoding='utf-8', errors='replace').read()
    raw = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', raw, flags=re.S | re.I)
    raw = re.sub(r'<(br|/p|/tr|/li|/h[1-6]|/div|/td|/th)[^>]*>', '\n', raw, flags=re.I)
    raw = re.sub(r'<[^>]+>', ' ', raw)
    raw = html.unescape(raw)
    lines = [re.sub(r'[ \t]+', ' ', ln).strip() for ln in raw.split('\n')]
    return [ln for ln in lines if ln]


def main():
    path, kws = sys.argv[1], [k.lower() for k in sys.argv[2:]]
    lines = text_of(path)
    if not kws:
        print('\n'.join(lines))
        return
    keep = set()
    for i, ln in enumerate(lines):
        if any(k in ln.lower() for k in kws):
            keep.update(range(max(0, i - 1), min(len(lines), i + 3)))
    for i in sorted(keep):
        print(lines[i][:400])


if __name__ == '__main__':
    main()
