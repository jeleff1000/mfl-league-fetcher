# _ledger_template.py -- HTML/CSS/JS shell for the Witness & Coverage Ledger.
# Split out of build_master_inventory.py to keep the builder readable.

STYLE = """
<style>
:root{
  --paper:#F4EFE3; --paper-2:#EBE4D3; --card:#FBF8F0; --ink:#20242C; --ink-soft:#4A4E58;
  --muted:#6B6558; --line:#D9CFB8; --line-2:#CFC3A6; --gold:#9C7A2E; --gold-soft:#B9963f;
  --good:#4F7A4A; --warn:#B0842B; --bad:#A8433B; --slate:#5E6B7A; --info:#4C6472; --neutral:#8A8578;
  --shadow:0 1px 0 rgba(90,74,40,.06),0 8px 24px -18px rgba(60,48,20,.5);
}
@media (prefers-color-scheme:dark){:root{
  --paper:#141821; --paper-2:#1A1F29; --card:#1B212C; --ink:#ECE7DA; --ink-soft:#C3BDAE;
  --muted:#948C79; --line:#2C333F; --line-2:#39414F; --gold:#C9A24A; --gold-soft:#B7913f;
  --good:#7FB077; --warn:#D6A94A; --bad:#D8776C; --slate:#93A2B4; --info:#8FB0C0; --neutral:#8f8877;
  --shadow:0 1px 0 rgba(0,0,0,.3),0 12px 30px -20px rgba(0,0,0,.7);
}}
:root[data-theme="light"]{
  --paper:#F4EFE3; --paper-2:#EBE4D3; --card:#FBF8F0; --ink:#20242C; --ink-soft:#4A4E58;
  --muted:#6B6558; --line:#D9CFB8; --line-2:#CFC3A6; --gold:#9C7A2E; --gold-soft:#B9963f;
  --good:#4F7A4A; --warn:#B0842B; --bad:#A8433B; --slate:#5E6B7A; --info:#4C6472; --neutral:#8A8578;
}
:root[data-theme="dark"]{
  --paper:#141821; --paper-2:#1A1F29; --card:#1B212C; --ink:#ECE7DA; --ink-soft:#C3BDAE;
  --muted:#948C79; --line:#2C333F; --line-2:#39414F; --gold:#C9A24A; --gold-soft:#B7913f;
  --good:#7FB077; --warn:#D6A94A; --bad:#D8776C; --slate:#93A2B4; --info:#8FB0C0; --neutral:#8f8877;
}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);
  font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;line-height:1.5;
  font-size:15px;-webkit-font-smoothing:antialiased;}
.wrap{max-width:1240px;margin:0 auto;padding:0 22px 96px}
.serif{font-family:"Iowan Old Style",Palatino,Georgia,"Times New Roman",serif}
h1,h2,h3{font-family:"Iowan Old Style",Palatino,Georgia,serif;font-weight:600;text-wrap:balance;margin:0}
a{color:var(--gold)}
.mono{font-family:ui-monospace,"Cascadia Code","SF Mono",Menlo,Consolas,monospace;font-variant-numeric:tabular-nums}
.eyebrow{font-size:11.5px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted);font-weight:600}
.rule{height:2px;background:var(--gold);border:0;opacity:.85}
.rule-thin{height:1px;background:var(--line);border:0}

/* masthead */
.masthead{padding:40px 0 20px;border-bottom:2px solid var(--gold)}
.masthead h1{font-size:38px;line-height:1.08;letter-spacing:-.01em}
.masthead .sub{color:var(--muted);margin-top:10px;max-width:64ch}
.badges{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px}
.chip{display:inline-flex;align-items:center;gap:7px;padding:4px 10px;border:1px solid var(--line-2);
  border-radius:2px;background:var(--card);font-size:12.5px;color:var(--ink-soft)}
.chip b{color:var(--ink)}

/* three-question thesis */
.thesis{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin:26px 0 8px}
.lens{background:var(--card);border:1px solid var(--line);border-top:3px solid var(--q);border-radius:3px;
  padding:16px 16px 14px;box-shadow:var(--shadow)}
.lens .qn{font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:var(--q);font-weight:700}
.lens h3{font-size:19px;margin:6px 0 6px}
.lens p{margin:0;color:var(--muted);font-size:13.5px}
.lens.w{--q:var(--good)} .lens.c{--q:var(--warn)} .lens.v{--q:var(--bad)}

section{margin-top:44px}
.sechd{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:14px}
.sechd h2{font-size:24px}
.sechd .note{color:var(--muted);font-size:13px}

/* rollup bars */
.rollup{display:grid;grid-template-columns:110px 1fr auto;gap:12px 16px;align-items:center}
.rollup .g{font-family:"Iowan Old Style",Georgia,serif;font-size:16px}
.bar{display:flex;height:22px;border-radius:2px;overflow:hidden;border:1px solid var(--line)}
.bar span{display:block;min-width:2px}
.rollup .tot{color:var(--muted);font-size:13px;white-space:nowrap}
.legend{display:flex;flex-wrap:wrap;gap:6px 14px;margin-top:14px;font-size:12.5px;color:var(--ink-soft)}
.legend i{display:inline-block;width:11px;height:11px;border-radius:2px;margin-right:6px;vertical-align:-1px}

/* status colors */
.s-sealed{background:var(--good)} .s-era_gap{background:var(--warn)} .s-verify{background:var(--gold-soft)}
.s-empty{background:var(--bad)} .s-unsealed_defect{background:var(--bad)} .s-queued{background:var(--slate)}
.s-unwitnessed{background:var(--slate)} .s-watch{background:var(--info)} .s-meta{background:var(--neutral)}

/* defect register cards */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px}
.dcard{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--sev);border-radius:3px;
  padding:14px 15px;box-shadow:var(--shadow)}
.dcard .top{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:7px}
.sev-tag{font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;color:#fff;
  background:var(--sev);padding:2px 7px;border-radius:2px}
.dcard h3{font-size:15.5px;line-height:1.25}
.dcard .era{font-family:ui-monospace,monospace;font-size:11.5px;color:var(--muted)}
.dcard p{margin:8px 0 0;font-size:13px;color:var(--ink-soft)}
.dcard .ref{margin-top:9px;font-size:11.5px;color:var(--muted)}
.sev-open_hole,.sev-open_gap,.sev-broken_compute,.sev-fix_built_unapplied,.sev-incomplete_promoted,.sev-single_witness{--sev:var(--bad)}
.sev-empty{--sev:var(--bad)} .sev-queue{--sev:var(--slate)} .sev-unwitnessed{--sev:var(--slate)}
.sev-verify{--sev:var(--gold-soft)} .sev-resolved_live{--sev:var(--good)} .sev-era_limit{--sev:var(--info)}

/* tables */
.tbl{width:100%;border-collapse:collapse;font-size:13px}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:3px;background:var(--card)}
.tbl th{position:sticky;top:0;background:var(--paper-2);text-align:left;font-size:11px;letter-spacing:.06em;
  text-transform:uppercase;color:var(--muted);padding:9px 12px;border-bottom:1px solid var(--line-2);white-space:nowrap;z-index:1}
.tbl td{padding:8px 12px;border-bottom:1px solid var(--line);vertical-align:top}
.tbl tr:hover td{background:color-mix(in srgb,var(--gold) 6%,transparent)}
.reg td:nth-child(3),.reg td:nth-child(4),.reg td:nth-child(5){font-family:ui-monospace,monospace;text-align:right;white-space:nowrap}

/* ledger controls */
.controls{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:12px}
.tabs{display:flex;gap:2px;background:var(--paper-2);border:1px solid var(--line);border-radius:3px;padding:3px}
.tab{border:0;background:transparent;color:var(--ink-soft);font-family:"Iowan Old Style",Georgia,serif;
  font-size:15px;padding:6px 14px;border-radius:2px;cursor:pointer}
.tab[aria-selected=true]{background:var(--card);color:var(--ink);box-shadow:inset 0 -2px 0 var(--gold)}
.search{flex:1;min-width:180px;padding:8px 11px;border:1px solid var(--line-2);border-radius:3px;
  background:var(--card);color:var(--ink);font-size:14px}
.search:focus,.tab:focus-visible,.fchip:focus-visible{outline:2px solid var(--gold);outline-offset:1px}
.fchips{display:flex;flex-wrap:wrap;gap:6px}
.fchip{border:1px solid var(--line-2);background:var(--card);color:var(--ink-soft);font-size:12px;
  padding:4px 9px;border-radius:20px;cursor:pointer;display:inline-flex;align-items:center;gap:6px}
.fchip[aria-pressed=true]{border-color:var(--gold);color:var(--ink);background:color-mix(in srgb,var(--gold) 12%,var(--card))}
.fchip i{width:9px;height:9px;border-radius:50%;display:inline-block}
.count{color:var(--muted);font-size:12.5px;margin-left:auto}

/* ledger table */
.ledger td:first-child{font-family:ui-monospace,"Cascadia Code",monospace;font-size:12.5px;color:var(--ink)}
.stat{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:600;white-space:nowrap}
.stat .dot{width:9px;height:9px;border-radius:2px;display:inline-block}
.qdots{display:inline-flex;gap:5px}
.qdots .d{width:10px;height:10px;border-radius:50%;border:1px solid transparent}
.d-yes{background:var(--good)} .d-partial{background:var(--warn)} .d-no{background:var(--bad)}
.d-unknown{background:transparent;border-color:var(--slate)} .d-na{background:transparent;border-color:var(--line-2)}
.fam{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.wl{font-size:11.5px;color:var(--muted);font-family:ui-monospace,monospace}
.era{font-family:ui-monospace,monospace;font-size:12px;color:var(--ink-soft);white-space:nowrap}
.gapnote{font-size:11.5px;color:var(--warn)}
.mirror-note{font-size:12px;color:var(--muted);margin:6px 2px 0}
.foot{margin-top:60px;padding-top:16px;border-top:1px solid var(--line);color:var(--muted);font-size:12.5px}
@media (max-width:820px){.thesis{grid-template-columns:1fr}.rollup{grid-template-columns:80px 1fr}.rollup .tot{display:none}}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
</style>
"""

BODY = """
<div class="wrap">
  <header class="masthead">
    <div class="eyebrow">NFL Super-Table &middot; Data-Quality Control</div>
    <h1>The Witness &amp; Coverage Ledger</h1>
    <p class="sub">Every column in every super-table &mdash; player bio, weekly, season, career &mdash;
      mapped to every witness we hold, with the gaps that aren&rsquo;t sealed shown in the open.
      One page, because things keep slipping and a stack of contracts isn&rsquo;t a control until you can see it.</p>
    <div class="badges" id="badges"></div>
  </header>

  <div class="thesis">
    <div class="lens w"><div class="qn">Question 1</div><h3>Witnessed?</h3>
      <p>Does a reconciliation <em>path</em> exist &mdash; a source or a witnessed formula that could reproduce the number? This is all the old matrix ever asked.</p></div>
    <div class="lens c"><div class="qn">Question 2</div><h3>Covered?</h3>
      <p>Is the column actually <em>populated</em> across the era the witness can reach &mdash; or is it empty, or short of its achievable floor? A path to a number nobody wrote down seals nothing.</p></div>
    <div class="lens v"><div class="qn">Question 3</div><h3>Verified?</h3>
      <p>Does a live <em>value control</em> (golden recipe, mirror identity, conservation law) gate the stored number against the witness? Most columns have no such gate &mdash; that is where things slip.</p></div>
  </div>
  <p class="mirror-note">A column is <b>sealed</b> only when all three are yes. The buckets below are every place one of them is no.</p>

  <section id="sec-rollup">
    <div class="sechd"><h2>Where the gaps are</h2><span class="note">status of every column, by grain</span></div>
    <div class="rollup" id="rollup"></div>
    <div class="legend" id="legend"></div>
  </section>

  <section id="sec-defects">
    <div class="sechd"><h2>The unsealed-gap register</h2><span class="note">re-verified live against the current release &middot; sorted by severity</span></div>
    <div class="cards" id="cards"></div>
  </section>

  <section id="sec-registry">
    <div class="sechd"><h2>The witness pages</h2><span class="note">every source we can reconcile against, and how far back it reaches</span></div>
    <div class="scroll"><table class="tbl reg"><thead><tr>
      <th>Witness page</th><th>Grain</th><th>Sources</th><th>Atoms</th><th>Era</th></tr></thead>
      <tbody id="regbody"></tbody></table></div>
  </section>

  <section id="sec-drift">
    <div class="sechd"><h2>Drift since the 2026-07-17 matrix</h2><span class="note">what the last promote changed under the audit&rsquo;s feet</span></div>
    <div id="drift"></div>
  </section>

  <section id="sec-ledger">
    <div class="sechd"><h2>The column ledger</h2><span class="note">search, or filter to a single gap type</span></div>
    <div class="controls">
      <div class="tabs" id="tabs" role="tablist"></div>
      <input class="search" id="search" type="search" placeholder="search a column, family, or witness&hellip;" aria-label="search columns">
    </div>
    <div class="fchips" id="fchips"></div>
    <div style="display:flex;align-items:center;margin:10px 2px 8px"><span class="count" id="count"></span></div>
    <p class="mirror-note" id="mirror"></p>
    <div class="scroll"><table class="tbl ledger"><thead><tr>
      <th data-k="0">Column</th><th data-k="1">Family</th><th data-k="2">Status</th>
      <th title="Witnessed / Covered / Verified">W&middot;C&middot;V</th>
      <th data-k="6">Witnesses</th><th>Witness era</th><th>Populated</th><th>Gap</th></tr></thead>
      <tbody id="ledbody"></tbody></table></div>
  </section>

  <div class="foot" id="foot"></div>
</div>
"""

SCRIPT = """
<script>
const DATA = __DATA__;
const $=s=>document.querySelector(s);
const el=(t,c,h)=>{const e=document.createElement(t);if(c)e.className=c;if(h!=null)e.innerHTML=h;return e;};
const STAT_ORDER=["unsealed_defect","empty","verify","queued","era_gap","unwitnessed","watch","meta","sealed"];
const STAT_LABEL={unsealed_defect:"Unsealed defect",empty:"Empty",verify:"Needs re-verify",queued:"Adjudication queue",
  era_gap:"Era / coverage gap",unwitnessed:"Unwitnessed",watch:"Resolved — watch",meta:"Identity / meta",sealed:"Sealed"};

// badges
const m=DATA.meta;
$("#badges").append(
  el("span","chip",`<b>${DATA.grains.weekly.n_rows.toLocaleString()}</b>&nbsp;weekly rows`),
  el("span","chip",`<b>1085</b>&nbsp;weekly cols &middot; <b>748</b> season &middot; <b>734</b> career &middot; <b>47</b> bio`),
  el("span","chip",`release&nbsp;<b class="mono">${m.release}</b>`),
  el("span","chip",`generated&nbsp;<b>${m.generated}</b>`),
  el("span","chip",`<b>${Object.keys(DATA.defects).length}</b>&nbsp;register entries`));

// rollup bars
const ru=$("#rollup");
const GK=[["player_bio","Bio"],["weekly","Weekly"],["player_nfl_season","Season"],["player_nfl_career","Career"]];
for(const [k,lab] of GK){
  const s=DATA.summary[k]; if(!s) continue;
  const tot=s.n_cols; const bar=el("div","bar");
  for(const st of STAT_ORDER){const n=s.status[st]||0; if(!n)continue;
    const seg=el("span","s-"+st); seg.style.flex=n; seg.title=`${STAT_LABEL[st]}: ${n}`; bar.append(seg);}
  const gaps=tot-(s.status.sealed||0)-(s.status.meta||0);
  ru.append(el("div","g",lab),bar,el("div","tot",`${gaps} open / ${tot}`));
}
const leg=$("#legend");
for(const st of STAT_ORDER) leg.append(el("span",null,`<i class="s-${st}"></i>${STAT_LABEL[st]}`));

// defect cards (sorted by severity weight)
const SEVW={open_hole:0,open_gap:0,broken_compute:0,incomplete_promoted:1,single_witness:1,fix_built_unapplied:1,
  empty:2,verify:3,queue:4,unwitnessed:5,era_limit:6,resolved_live:7};
const defs=Object.entries(DATA.defects).sort((a,b)=>(SEVW[a[1].sev]??9)-(SEVW[b[1].sev]??9));
const cw=$("#cards");
for(const [id,d] of defs){
  const c=el("div","dcard sev-"+d.sev);
  const ncols=(d.cols||[]).length;
  c.append(el("div","top",
    `<span class="sev-tag">${d.sev.replace(/_/g,' ')}</span>`+
    (d.era?`<span class="era">${d.era}</span>`:'')+
    (ncols?`<span class="era">&middot; ${ncols} col${ncols>1?'s':''}</span>`:'')));
  c.append(el("h3",null,d.title));
  c.append(el("p",null,d.detail));
  c.append(el("div","ref",`audit ${d.ref||''}`));
  cw.append(c);
}

// witness registry
const rb=$("#regbody");
const reg=Object.entries(DATA.registry).sort((a,b)=>(b[1].era_max||0)-(a[1].era_max||0)||b[1].n_atoms-a[1].n_atoms);
for(const [label,r] of reg){
  const tr=el("tr");
  tr.append(el("td",null,label),el("td","fam",r.page_grain||''),
    el("td","wl",(r.sources||[]).join(", ")),
    el("td",null,r.n_atoms),
    el("td",null,`${r.era_min||'?'}–${r.era_max||'?'}`));
  rb.append(tr);
}

// drift
const dv=$("#drift");
for(const [k,d] of Object.entries(DATA.drift)){
  if(!d.dropped.length && d.matrix===d.current) continue;
  const line=el("p","mirror-note");
  if(d.dropped.length) line.innerHTML=`<b>${k}</b>: ${d.matrix}→${d.current} cols. Dropped by the 07-20 promote (the audit &sect;6 cleanup): `+
    d.dropped.map(c=>`<span class="mono">${c}</span>`).join(", ");
  else line.innerHTML=`<b>${k}</b>: ${d.current} cols, unchanged.`;
  dv.append(line);
}
dv.append(el("p","mirror-note",`<span class="mono">player_bio</span> (47 cols) was never in the stat witness matrix at all &mdash; its witnesses are mapped for the first time in this ledger.`));

// ---------- ledger ----------
let curGrain="weekly", curFilter=null, sortK=0, sortDir=1;
const F={0:"col",1:"fam",2:"status",6:"nwit"};
const R=i=>({col:i[0],fam:i[1],status:i[2],w:i[3],cov:i[4],ver:i[5],nwit:i[6],gera:i[7],
  floor:i[8],nz:i[9],ach:i[10],defs:i[11],wit:i[12],inputs:i[13],note:i[14]});

const tabs=$("#tabs");
for(const [k,lab] of GK){
  const b=el("button","tab",lab); b.setAttribute("role","tab");
  b.setAttribute("aria-selected", k===curGrain);
  b.onclick=()=>{curGrain=k;[...tabs.children].forEach(x=>x.setAttribute("aria-selected",false));
    b.setAttribute("aria-selected",true);buildChips();render();};
  tabs.append(b);
}
function grainRows(){return DATA.grains[curGrain].rows.map(R);}
function buildChips(){
  const fc=$("#fchips"); fc.innerHTML="";
  const counts={}; for(const r of grainRows()) counts[r.status]=(counts[r.status]||0)+1;
  const all=el("button","fchip"); all.setAttribute("aria-pressed",curFilter===null);
  all.innerHTML=`All`; all.onclick=()=>{curFilter=null;buildChips();render();}; fc.append(all);
  const gaponly=el("button","fchip"); const isGap=curFilter==="__gaps";
  gaponly.setAttribute("aria-pressed",isGap);
  gaponly.innerHTML=`<i class="s-unsealed_defect"></i>Only gaps`;
  gaponly.onclick=()=>{curFilter="__gaps";buildChips();render();}; fc.append(gaponly);
  for(const st of STAT_ORDER){const n=counts[st]||0; if(!n)continue;
    const b=el("button","fchip"); b.setAttribute("aria-pressed",curFilter===st);
    b.innerHTML=`<i class="s-${st}"></i>${STAT_LABEL[st]} ${n}`;
    b.onclick=()=>{curFilter=st;buildChips();render();}; fc.append(b);}
}
const GAPSET=new Set(["unsealed_defect","empty","verify","queued","era_gap","unwitnessed"]);
function render(){
  const q=$("#search").value.trim().toLowerCase();
  let rows=grainRows();
  if(curFilter==="__gaps") rows=rows.filter(r=>GAPSET.has(r.status));
  else if(curFilter) rows=rows.filter(r=>r.status===curFilter);
  if(q) rows=rows.filter(r=>r.col.toLowerCase().includes(q)||r.fam.toLowerCase().includes(q)||
     (r.wit||[]).join(" ").toLowerCase().includes(q)||(r.defs||[]).join(" ").toLowerCase().includes(q));
  const kf=F[sortK]||"col";
  rows.sort((a,b)=>{let x=a[kf],y=b[kf]; if(kf==="status"){x=STAT_ORDER.indexOf(x);y=STAT_ORDER.indexOf(y);}
    if(x==null)x=-1; if(y==null)y=-1; return (x>y?1:x<y?-1:0)*sortDir;});
  const tb=$("#ledbody"); tb.innerHTML="";
  const dq=v=>`<span class="d d-${v||'na'}" title="${v||'n/a'}"></span>`;
  const frag=document.createDocumentFragment();
  for(const r of rows){
    const tr=el("tr");
    const witTxt=(r.wit||[]).slice(0,3).join(", ")+((r.nwit>3)?` +${r.nwit-3}`:"");
    let gap="";
    if(r.status==="empty") gap=`<span class="gapnote">0 populated rows</span>`;
    else if(r.status==="era_gap"&&r.ach) gap=`<span class="gapnote">reach → ${r.ach}</span>`;
    else if(r.status==="unsealed_defect"||r.status==="verify"||r.status==="queued")
      gap=`<span class="gapnote">${(r.defs||[])[0]||''}</span>`;
    else if(r.status==="unwitnessed") gap=`<span class="gapnote">no witness</span>`;
    const pop=(r.floor!=null)?`<span class="era">${r.floor}–${(r.nz||0).toLocaleString()}r</span>`:
              (r.status==="empty"?`<span class="era" style="color:var(--bad)">empty</span>`:"");
    tr.append(
      el("td",null,r.col),
      el("td","fam",r.fam),
      el("td",null,`<span class="stat"><span class="dot s-${r.status}"></span>${STAT_LABEL[r.status]}</span>`),
      el("td",null,`<span class="qdots">${dq(r.w)}${dq(r.cov)}${dq(r.ver)}</span>`),
      el("td","wl",witTxt||'—'),
      el("td",null,r.gera?`<span class="era">${r.gera}+</span>`:'—'),
      el("td",null,pop||'—'),
      el("td",null,gap));
    frag.append(tr);
  }
  tb.append(frag);
  $("#count").textContent=`${rows.length.toLocaleString()} columns`;
  const mir=DATA.grains[curGrain].mirror;
  $("#mirror").innerHTML = mir? `Identical column set to <span class="mono">${mir}</span> (the all-players mirror &mdash; same verdicts, more rows).`:"";
}
document.querySelectorAll(".ledger th[data-k]").forEach(th=>{th.style.cursor="pointer";
  th.onclick=()=>{const k=+th.dataset.k; if(sortK===k)sortDir*=-1; else{sortK=k;sortDir=1;} render();};});
$("#search").addEventListener("input",render);
$("#foot").innerHTML=`Sources: <span class="mono">witness-column-master-matrix.json</span> (verdicts, 2026-07-17) &middot; `+
  `<span class="mono">witness-contracts-v2.json</span> (158 contracts) &middot; column set &amp; weekly era-floors recomputed live on the ${m.release} release &middot; `+
  `defect register hand-encoded &amp; re-verified from <span class="mono">supertable-witness-master-audit-2026-07-16.md</span>. `+
  `Machine source of truth: <span class="mono">docs/witness-coverage-master-inventory.json</span>.`;
buildChips(); render();
</script>
"""
