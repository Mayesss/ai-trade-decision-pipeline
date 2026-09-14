// How many signals can THIS panel support testing, before the multiple-testing
// correction eats the effects we could plausibly find?
// Uses the measured IC dispersion from .study/23 (reversal: mean 0.0275, ICIR 0.140).
const meanIC = 0.0275, icir = 0.140;
const sdIC = meanIC / icir;                 // per-date IC dispersion
const dates = 5474, H = 6;
const indep = Math.floor(dates / H);        // non-overlapping forward windows
const se = sdIC / Math.sqrt(indep);

// inverse normal (Acklam)
function zFor(p) {
  const a=[-3.969683028665376e+01,2.209460984245205e+02,-2.759285104469687e+02,1.383577518672690e+02,-3.066479806614716e+01,2.506628277459239e+00];
  const b=[-5.447609879822406e+01,1.615858368580409e+02,-1.556989798598866e+02,6.680131188771972e+01,-1.328068155288572e+01];
  const c=[-7.784894002430293e-03,-3.223964580411365e-01,-2.400758277161838e+00,-2.549732539343734e+00,4.374664141464968e+00,2.938163982698783e+00];
  const d=[7.784695709041462e-03,3.224671290700398e-01,2.445134137142996e+00,3.754408661907416e+00];
  const pl=0.02425;
  if (p<pl){const q=Math.sqrt(-2*Math.log(p));return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);}
  if (p<=1-pl){const q=p-0.5,r=q*q;return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q/(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);}
  const q=Math.sqrt(-2*Math.log(1-p));return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
}
console.log(`per-date IC dispersion sd = ${sdIC.toFixed(3)}`);
console.log(`non-overlapping dates      = ${indep}`);
console.log(`SE of mean IC              = ${se.toFixed(5)}\n`);
console.log('  family size N   required |t|   smallest IC detectable at 80% power');
for (const N of [1, 4, 10, 50, 100, 200, 704]) {
  const z = zFor(1 - 0.05 / (2 * N));
  const mde = (z + 0.84) * se;
  const flag = mde > meanIC ? '   <- the reversal effect (0.0275) would NOT survive' : '';
  console.log(`  ${String(N).padStart(11)}   ${z.toFixed(2).padStart(11)}   ${mde.toFixed(4).padStart(10)}${flag}`);
}
console.log(`\nobserved reversal IC = ${meanIC}`);
