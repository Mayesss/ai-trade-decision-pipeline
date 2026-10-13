"""SYNTHETIC: Deribit marks options off the expiry's forward F = S(1+b), but straddle_entry inverts the mark of
ONE leg (whichever traded last at K) with F = index S, then re-prices BOTH legs at S with that IV. Error of the
straddle premium vs its true value, in % of premium, for a call-last or put-last trade."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ct import shortvol as sv
S, K, T = 100.0, 100.0, 7 / 365
for sig in (0.5, 0.8):
    for ann in (0.02, 0.10, 0.25, 0.40):
        b = ann * 7 / 365
        Fp = S * (1 + b)
        true = sv.b76(Fp, K, T, sig, True) + sv.b76(Fp, K, T, sig, False)
        out = []
        for call in (True, False):
            mark = sv.b76(Fp, K, T, sig, call)
            iv = sv.implied_vol(mark, S, K, T, call)
            prem = sv.b76(S, K, T, iv, True) + sv.b76(S, K, T, iv, False)
            out.append(f"{'call' if call else 'put '}-last {100 * (prem / true - 1):+.1f}%")
        print(f'sigma {sig:.1f}  annualised basis {ann:4.0%} (7-day {100 * b:.2f}%):  ', ' | '.join(out))
