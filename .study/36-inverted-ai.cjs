// Inverted-AI check (2026-10-11, docs/inverted-ai-and-prompt-data-2026-10-11.md).
// (1) 48h return of the inverted AI vs a mechanical fade of the last-24h move,
//     at the same 411 decision times; (2) the mechanical fade on every 6th 4H bar
//     BEFORE the AI traded (2024-11 -> 2026-06-26), 25 instruments. Local only.
// Run: node .study/36-inverted-ai.cjs "$PWD"
const root=process.argv[2];
const C=require(root+'/.study/candles-deep.json');const P=require(root+'/.study/panel.json');
const BAR=4*3600e3, COST={bitget:12,capital:10};
function atr14(c){const o=Array(c.length).fill(null);let pv=null,a=null;for(let i=0;i<c.length;i++){const tr=pv===null?c[i][2]-c[i][3]:Math.max(c[i][2]-c[i][3],Math.abs(c[i][2]-pv),Math.abs(c[i][3]-pv));a=i===0?tr:(a*13+tr)/14;pv=c[i][4];if(i>=14)o[i]=a}return o}
function ct(v,k){const n=v.length;if(n<3)return{n,m:NaN,t:NaN};const m=v.reduce((a,b)=>a+b,0)/n,g=new Map();v.forEach((x,i)=>g.set(k[i],(g.get(k[i])||0)+(x-m)));let s=0;for(const x of g.values())s+=x*x;const G=g.size;return{n,m,t:m/Math.sqrt(s/(n*n)*G/(G-1)),G}}
const wk=ts=>Math.floor(ts/(7*86400e3));
const A={};for(const k in C)A[k]=atr14(C[k]);
// (1) at decision times
const rows=[];
for(const p of P){if(p.y12==null)continue;const k=p.platform+':'+p.symbol,c=C[k];if(!c)continue;
 let i=-1;for(let j=0;j<c.length;j++)if(c[j][0]+BAR>p.ts){i=j;break}if(i<20)continue;
 const a=A[k][i];const rev6=(c[i][4]-c[i-6][4])/a;const costA=(COST[p.platform]/100)/p.atrPct;
 const fadeDir=-Math.sign(rev6);
 rows.push({wk:wk(p.ts),inv:-p.y12-costA,fade:fadeDir*p.dir*p.y12-costA,chase:p.dir*Math.sign(rev6)>0,cat:p.category});}
const s=(f,sel)=>{const r=rows.filter(sel||(()=>true));const x=ct(r.map(f),r.map(q=>q.wk));return`n=${x.n} mean=${x.m.toFixed(3)} t_wk=${x.t.toFixed(2)}`};
console.log('AT DECISION TIMES, 48h net (ATR):');
console.log(' inverted AI            ',s(r=>r.inv));
console.log(' mechanical fade 24h    ',s(r=>r.fade));
console.log(' inverted AI | AI chased',s(r=>r.inv,r=>r.chase));
console.log(' inverted AI | AI faded ',s(r=>r.inv,r=>!r.chase));
// (2) mechanical fade on all bars BEFORE the AI traded (independent period), every 6th bar (daily, non-overlapping-ish entries)
const START=Date.parse('2026-06-27');
for(const H of [6,12]){
const out={all:[],crypto:[],capital:[]},K={all:[],crypto:[],capital:[]};
for(const k in C){const c=C[k],a=A[k],plat=k.split(':')[0];
 for(let i=20;i+H<c.length;i+=6){if(c[i][0]+BAR>=START)break;if(!(a[i]>0))continue;
  const rev6=(c[i][4]-c[i-6][4])/a[i];if(rev6===0)continue;
  const y=-Math.sign(rev6)*(c[i+H][4]-c[i][4])/a[i]-(COST[plat]/1e4)*c[i][4]/a[i];
  const w=wk(c[i][0]);out.all.push(y);K.all.push(w);const g=plat==='bitget'?'crypto':'capital';out[g].push(y);K[g].push(w);}}
for(const g of ['all','crypto','capital']){const x=ct(out[g],K[g]);console.log(`PRE-AI period fade-24h, ${H*4}h hold, net ATR [${g}]: n=${x.n} mean=${x.m.toFixed(4)} t_wk=${x.t.toFixed(2)} weeks=${x.G}`)}
}
