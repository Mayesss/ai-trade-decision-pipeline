// Signal library. Pure functions of OHLCV. Shared by the discovery run and the
// holdout run so both are scored by identical code.
export function indicators(c) {
    const n = c.length;
    const cl = c.map((x) => x[4]), hi = c.map((x) => x[2]), lo = c.map((x) => x[3]), vol = c.map((x) => x[5]);
    const ema = (v, p) => { const k = 2 / (p + 1); const o = []; let e = v[0];
        for (let i = 0; i < v.length; i++) { e = i ? v[i] * k + e * (1 - k) : v[0]; o.push(e); } return o; };
    const atr = (() => { const o = new Array(n).fill(null); let prev = null, a = null;
        for (let i = 0; i < n; i++) { const tr = prev === null ? hi[i] - lo[i]
            : Math.max(hi[i] - lo[i], Math.abs(hi[i] - prev), Math.abs(lo[i] - prev));
            a = i === 0 ? tr : (a * 13 + tr) / 14; prev = cl[i]; if (i >= 14) o[i] = a; } return o; })();
    const rsi = (() => { const o = new Array(n).fill(null); let ag = 0, al = 0;
        for (let i = 1; i < n; i++) { const d = cl[i] - cl[i - 1], g = Math.max(0, d), l = Math.max(0, -d);
            if (i <= 14) { ag += g / 14; al += l / 14; if (i === 14) o[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al); }
            else { ag = (ag * 13 + g) / 14; al = (al * 13 + l) / 14; o[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al); } } return o; })();
    const ret = cl.map((v, i) => (i ? Math.log(v / cl[i - 1]) : 0));
    const rstd = (w) => { const o = new Array(n).fill(null);
        for (let i = w; i < n; i++) { const s = ret.slice(i - w + 1, i + 1); const m = s.reduce((a, b) => a + b, 0) / w;
            o[i] = Math.sqrt(s.reduce((a, b) => a + (b - m) ** 2, 0) / (w - 1)); } return o; };
    const rmax = (v, w) => { const o = new Array(n).fill(null); for (let i = w; i < n; i++) o[i] = Math.max(...v.slice(i - w + 1, i + 1)); return o; };
    const rmin = (v, w) => { const o = new Array(n).fill(null); for (let i = w; i < n; i++) o[i] = Math.min(...v.slice(i - w + 1, i + 1)); return o; };
    const rmean = (v, w) => { const o = new Array(n).fill(null); for (let i = w; i < n; i++) { let s = 0; for (let j = i - w + 1; j <= i; j++) s += v[j]; o[i] = s / w; } return o; };
    return { cl, hi, lo, vol, ema20: ema(cl, 20), ema50: ema(cl, 50), ema200: ema(cl, 200),
        atr, rsi, ret, sd20: rstd(20), sd60: rstd(60), max60: rmax(hi, 60), min60: rmin(lo, 60),
        vmean20: rmean(vol, 20), vmean60: rmean(vol, 60), rmean60: rmean(cl, 60) };
}

// Each entry: [name, fn(ind, i) -> number|null]. Two-sided tests, so the sign
// of a signal is not a free parameter that needs its own correction.
export const SIGNALS = [
    // --- momentum / reversal at horizons -----------------------------------
    ['ret_1',        (d, i) => (d.cl[i] - d.cl[i - 1]) / d.atr[i]],
    ['ret_2',        (d, i) => (d.cl[i] - d.cl[i - 2]) / d.atr[i]],
    ['ret_6',        (d, i) => (d.cl[i] - d.cl[i - 6]) / d.atr[i]],
    ['ret_12',       (d, i) => (d.cl[i] - d.cl[i - 12]) / d.atr[i]],
    ['ret_30',       (d, i) => (d.cl[i] - d.cl[i - 30]) / d.atr[i]],
    ['ret_60',       (d, i) => (d.cl[i] - d.cl[i - 60]) / d.atr[i]],
    ['ret_120',      (d, i) => (d.cl[i] - d.cl[i - 120]) / d.atr[i]],
    ['ret_180',      (d, i) => (d.cl[i] - d.cl[i - 180]) / d.atr[i]],
    // --- momentum shape -----------------------------------------------------
    ['accel_6_30',   (d, i) => (d.cl[i] - d.cl[i - 6]) / d.atr[i] - (d.cl[i - 6] - d.cl[i - 30]) / d.atr[i]],
    ['mom_6_minus_60',(d, i) => (d.cl[i] - d.cl[i-6]) / d.atr[i] - (d.cl[i] - d.cl[i-60]) / d.atr[i]],
    // --- trend --------------------------------------------------------------
    ['ema20_50',     (d, i) => (d.ema20[i] - d.ema50[i]) / d.atr[i]],
    ['ema50_200',    (d, i) => (d.ema50[i] - d.ema200[i]) / d.atr[i]],
    ['px_vs_ema20',  (d, i) => (d.cl[i] - d.ema20[i]) / d.atr[i]],
    ['px_vs_ema50',  (d, i) => (d.cl[i] - d.ema50[i]) / d.atr[i]],
    ['px_vs_ema200', (d, i) => (d.cl[i] - d.ema200[i]) / d.atr[i]],
    // --- mean reversion / oscillators ---------------------------------------
    ['rsi14',        (d, i) => d.rsi[i]],
    ['zscore60',     (d, i) => d.sd60[i] > 0 ? (d.cl[i] - d.rmean60[i]) / (d.cl[i] * d.sd60[i]) : null],
    ['px_vs_mean60', (d, i) => (d.cl[i] - d.rmean60[i]) / d.atr[i]],
    // --- volatility ---------------------------------------------------------
    ['atr_pct',      (d, i) => (d.atr[i] / d.cl[i]) * 100],
    ['vol20',        (d, i) => d.sd20[i]],
    ['vol60',        (d, i) => d.sd60[i]],
    ['vol_ratio',    (d, i) => d.sd60[i] > 0 ? d.sd20[i] / d.sd60[i] : null],
    ['hl_range',     (d, i) => (d.hi[i] - d.lo[i]) / d.atr[i]],
    // --- volume -------------------------------------------------------------
    ['vol_z20',      (d, i) => d.vmean20[i] > 0 ? d.vol[i] / d.vmean20[i] : null],
    ['vol_trend',    (d, i) => d.vmean60[i] > 0 ? d.vmean20[i] / d.vmean60[i] : null],
    ['dollar_vol',   (d, i) => Math.log(1 + d.vol[i] * d.cl[i])],
    ['vol_per_atr',  (d, i) => d.atr[i] > 0 ? Math.log(1 + d.vol[i]) / d.atr[i] : null],
    // --- range position / structure -----------------------------------------
    ['range_pos60',  (d, i) => d.max60[i] > d.min60[i] ? (d.cl[i] - d.min60[i]) / (d.max60[i] - d.min60[i]) : null],
    ['dist_high60',  (d, i) => (d.max60[i] - d.cl[i]) / d.atr[i]],
    ['dist_low60',   (d, i) => (d.cl[i] - d.min60[i]) / d.atr[i]],
    ['range_width60',(d, i) => (d.max60[i] - d.min60[i]) / d.atr[i]],
    // --- return distribution shape ------------------------------------------
    ['skew60',       (d, i) => { const s = d.ret.slice(i - 59, i + 1); const m = s.reduce((a,b)=>a+b,0)/60;
        const v = s.reduce((a,b)=>a+(b-m)**2,0)/60; return v > 0 ? s.reduce((a,b)=>a+(b-m)**3,0)/60/Math.pow(v,1.5) : null; }],
    ['kurt60',       (d, i) => { const s = d.ret.slice(i - 59, i + 1); const m = s.reduce((a,b)=>a+b,0)/60;
        const v = s.reduce((a,b)=>a+(b-m)**2,0)/60; return v > 0 ? s.reduce((a,b)=>a+(b-m)**4,0)/60/(v*v) : null; }],
    ['up_ratio30',   (d, i) => { let u = 0; for (let j = i - 29; j <= i; j++) if (d.ret[j] > 0) u++; return u / 30; }],
    ['max_ret30',    (d, i) => Math.max(...d.ret.slice(i - 29, i + 1))],
    ['min_ret30',    (d, i) => Math.min(...d.ret.slice(i - 29, i + 1))],
];
export const WARMUP = 210;
