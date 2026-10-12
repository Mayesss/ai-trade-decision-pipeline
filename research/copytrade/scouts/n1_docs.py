"""N1 scout: compact view of the trade.xyz Specification Index table.

Reads the downloaded docs text (path as argument; never executed from the
download dir). Prints one line per instrument with the description column
dropped. Behaviour/mechanics only.

    .venv/bin/python -I scouts/n1_docs.py data/scout_n1/docs_tradexyz_20261012/llms-full.txt
"""
import html
import re
import sys


def rows(text, start_header, end_header):
    a = text.index(start_header)
    b = text.index(end_header, a + len(start_header))
    seg = text[a:b]
    out = []
    for tr in re.findall(r'<tr>(.*?)</tr>', seg, flags=re.S):
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, flags=re.S)
        clean = [html.unescape(re.sub(r'<[^>]+>', ' ', c)).strip() for c in cells]
        clean = [re.sub(r'\s+', ' ', c) for c in clean]
        out.append(clean)
    return out


def main(path):
    text = open(path, encoding='utf-8').read()
    for hdr, end in (('# Specification Index', '# Pre-IPO Specification Index'),
                     ('# Pre-IPO Specification Index', '# Holiday Closures')):
        print('==', hdr)
        for r in rows(text, hdr, end):
            if len(r) > 2:
                r = [r[0]] + r[2:]  # drop description
            print(' | '.join(c[:160] for c in r))


if __name__ == '__main__':
    main(sys.argv[1])
