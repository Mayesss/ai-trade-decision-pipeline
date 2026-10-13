"""A1 scout: crude pure-python PDF text extractor (FlateDecode streams, Tj/TJ,
ToUnicode CMaps). Usage: python -I a1_pdftext.py <in.pdf> <out.txt>
Reads a document only; computes nothing."""
import re
import sys
import zlib


def streams(data):
    for m in re.finditer(rb"(\d+) 0 obj(.*?)stream\r?\n", data, re.S):
        start = m.end()
        end = data.find(b"endstream", start)
        raw = data[start:end]
        head = m.group(2)
        if b"FlateDecode" in head:
            try:
                raw = zlib.decompressobj().decompress(raw)
            except Exception:
                continue
        yield int(m.group(1)), head, raw


def parse_cmap(raw):
    cmap = {}
    for blk in re.findall(rb"beginbfchar(.*?)endbfchar", raw, re.S):
        for a, b in re.findall(rb"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", blk):
            try:
                cmap[int(a, 16)] = bytes.fromhex(b.decode()).decode("utf-16-be", "ignore")
            except Exception:
                pass
    for blk in re.findall(rb"beginbfrange(.*?)endbfrange", raw, re.S):
        for a, b, c in re.findall(rb"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", blk):
            lo, hi, base = int(a, 16), int(b, 16), int(c, 16)
            for i in range(lo, min(hi, lo + 2000) + 1):
                cmap[i] = chr(base + i - lo)
    return cmap


def main(inp, out):
    data = open(inp, "rb").read()
    allst = list(streams(data))
    cmap = {}
    for _, _, raw in allst:
        if b"beginbfchar" in raw or b"beginbfrange" in raw:
            cmap.update(parse_cmap(raw))
    lines = []
    for _, _, raw in allst:
        if b"BT" not in raw:
            continue
        for bt in re.findall(rb"BT(.*?)ET", raw, re.S):
            parts = []
            for hx in re.findall(rb"<([0-9A-Fa-f]+)>|\(((?:\\.|[^\\)])*)\)", bt):
                h, lit = hx
                if h:
                    s = h.decode()
                    w = 4 if len(s) % 4 == 0 else 2
                    for i in range(0, len(s), w):
                        parts.append(cmap.get(int(s[i:i + w], 16), ""))
                else:
                    parts.append(lit.decode("latin-1"))
            t = "".join(parts).strip()
            if t:
                lines.append(t)
    open(out, "w").write("\n".join(lines))
    print(len(lines), "text runs")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
