import { useState, useEffect, useRef, useCallback } from 'react'
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, Cell,
} from 'recharts'

const GW = ''

// ─── Palette ──────────────────────────────────────────────────────────────────
const P = {
  page:   '#05111f',
  panel:  '#0a1c32',
  card:   '#0d2240',
  input:  '#061426',
  border: '#1a3554',
  sep:    '#0f2035',
  th: '#ddeeff',
  tb: '#6a9abf',
  tm: '#3a6080',
  td: '#1e3a52',
}

const TC = {
  'command-patient-admit': '#7dd3fc',
  'patient-admitted':      '#38bdf8',
  'patient-discharged':    '#86efac',
  'patient-transferred':   '#fde68a',
  'lab-result-created':    '#34d399',
  'fhir-document-created': '#a78bfa',
  'notification-sent':     '#fb923c',
  'alert-created':         '#f87171',
}
const SEV = { low:'#34d399', medium:'#fbbf24', high:'#fb923c', critical:'#ef4444' }

const PIPE = [
  { id:'gateway',              name:'Gateway',  sub:'REST+sim',  col:'#60a5fa' },
  { id:'adt-service',          name:'ADT',      sub:'Events',    col:'#38bdf8' },
  { id:'lab-service',          name:'Lab',      sub:'Results',   col:'#34d399' },
  { id:'fhir-bridge',          name:'FHIR',     sub:'HL7 docs',  col:'#a78bfa' },
  { id:'notification-service', name:'Notify',   sub:'Alerts',    col:'#fb923c' },
  { id:'audit-service',        name:'Audit',    sub:'Compliance',col:'#f9a8d4' },
]

const CHAOS_SVCS = ['adt-service','lab-service','fhir-bridge','notification-service','audit-service']
const WARDS      = ['ICU','Cardiology','Oncology','Pediatrics','Emergency','Surgery','Neurology']
const LAB_TESTS  = [
  { name:'Hemoglobin',        lo:12.0, hi:17.5, unit:'g/dL'    },
  { name:'White Blood Cells', lo:4.5,  hi:11.0, unit:'10^3/uL' },
  { name:'Platelets',         lo:150,  hi:400,  unit:'10^3/uL' },
  { name:'Glucose',           lo:70,   hi:100,  unit:'mg/dL'   },
  { name:'Creatinine',        lo:0.6,  hi:1.2,  unit:'mg/dL'   },
]

const SCENARIOS = [
  {
    id:'fanout', label:'Fan-out Demo', col:'#38bdf8', tag:'Routing',
    desc:'One admission triggers ADT, Lab, FHIR, Notify and Audit in parallel — zero boilerplate.',
    steps:[
      { t:0,     msg:'Simulator at 2 evt/s — watch the fan-out ratio!', sim:{ enabled:true,  rate:2 } },
      { t:30000, msg:'Done.',                                            sim:{ enabled:false, rate:2 } },
    ],
  },
  {
    id:'scale', label:'Scale-Up', col:'#fbbf24', tag:'Throughput',
    desc:'Ramp from 1 to 20 events/sec with zero code changes and zero message loss.',
    steps:[
      { t:0,     msg:'1 evt/s baseline...',        sim:{ enabled:true,  rate:1  } },
      { t:4000,  msg:'5 evt/s...',                 sim:{ enabled:true,  rate:5  } },
      { t:8000,  msg:'10 evt/s...',                sim:{ enabled:true,  rate:10 } },
      { t:12000, msg:'20 evt/s — full throttle!',  sim:{ enabled:true,  rate:20 } },
      { t:17000, msg:'Slowing back down...',        sim:{ enabled:true,  rate:3  } },
      { t:21000, msg:'Done.',                       sim:{ enabled:false, rate:1  } },
    ],
  },
  {
    id:'dlq', label:'Retry + DLQ', col:'#f87171', tag:'Resilience',
    desc:'Poison lab-service: Watermill retries 5x with exponential backoff then routes to DLQ.',
    steps:[
      { t:0,     msg:'Healthy at 3 evt/s...',                 sim:{ enabled:true,  rate:3 } },
      { t:4000,  msg:'lab-service poisoned — DLQ growing!',   chaos:{ service:'lab-service', mode:'poison', error_rate:1.0 } },
      { t:18000, msg:'Chaos reset — auto-recovering...',       chaosReset:true },
      { t:22000, msg:'Fully recovered.',                       sim:{ enabled:false, rate:1 } },
    ],
  },
  {
    id:'cascade', label:'Cascade Failure', col:'#fb923c', tag:'Fault Isolation',
    desc:'3 services fail independently — no domino effect, no data loss.',
    steps:[
      { t:0,     msg:'All healthy at 4 evt/s...',              sim:{ enabled:true, rate:4 } },
      { t:4000,  msg:'Poisoning lab-service...',                chaos:{ service:'lab-service',          mode:'poison', error_rate:1.0 } },
      { t:7000,  msg:'Poisoning fhir-bridge...',                chaos:{ service:'fhir-bridge',          mode:'poison', error_rate:1.0 } },
      { t:10000, msg:'Dropping 50% in notification-service...',chaos:{ service:'notification-service', mode:'drop',   error_rate:0.5 } },
      { t:16000, msg:'Resetting ALL — watch recovery!',         chaosReset:true },
      { t:21000, msg:'Done.',                                   sim:{ enabled:false, rate:1 } },
    ],
  },
]

async function apiPost(path, body) {
  const r = await fetch(GW+path, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) })
  if (!r.ok) { const t = await r.text(); throw new Error(t) }
  return r.json()
}
async function apiDel(path) { await fetch(GW+path, { method:'DELETE' }) }

// ─── Hooks ────────────────────────────────────────────────────────────────────
function useSSE(url) {
  const [events,    setEvents]    = useState([])
  const [connected, setConnected] = useState(false)
  const tickRef = useRef({})
  useEffect(() => {
    const es = new EventSource(url)
    es.onopen  = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.addEventListener('event', e => {
      try {
        const ev = JSON.parse(e.data)
        tickRef.current[ev.type] = (tickRef.current[ev.type] || 0) + 1
        setEvents(prev => [ev, ...prev].slice(0, 500))
      } catch (_) {}
    })
    return () => es.close()
  }, [url])
  return { events, connected, tickRef }
}

function useWidth() {
  const [w, setW] = useState(() => window.innerWidth)
  useEffect(() => {
    const h = () => setW(window.innerWidth)
    window.addEventListener('resize', h)
    return () => window.removeEventListener('resize', h)
  }, [])
  return w
}

// ─── CSS injected once ────────────────────────────────────────────────────────
const CSS = `
  * { box-sizing: border-box; }
  input, select, button { font-family: inherit; }
  input::placeholder { color: #3a6080; }
  select option { background: #0a1c32; }
  input[type=range] { height: 4px; accent-color: #60a5fa; cursor: pointer; }
  @keyframes wmPulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50%       { opacity: 0.3; transform: scale(0.5); }
  }
  @keyframes pipeFlow {
    0%, 100% { opacity: 0.5; }
    50%       { opacity: 1; }
  }
  .pipe-wrap { display: flex; align-items: center; gap: 8px; overflow-x: auto; padding-bottom: 6px; }
  .pipe-wrap::-webkit-scrollbar { height: 4px; }
  .pipe-wrap::-webkit-scrollbar-track { background: transparent; }
  .pipe-wrap::-webkit-scrollbar-thumb { background: #1a3554; border-radius: 2px; }
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(100px, 1fr)); gap: 8px; }
  .charts-row { display: flex; gap: 16px; flex-wrap: wrap; }
  .chart-main { flex: 3 1 300px; min-width: 260px; }
  .chart-bar  { flex: 1 1 180px; min-width: 160px; }
  .chart-dlq  { flex: 1 1 160px; min-width: 140px; }
  .two-col    { display: flex; gap: 16px; flex-wrap: wrap; }
  .two-col > *{ flex: 1 1 260px; min-width: 220px; }
  .scenario-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
  @media (max-width: 640px) {
    .kpi-grid { grid-template-columns: repeat(3, 1fr); }
    .charts-row { flex-direction: column; }
    .chart-main, .chart-bar, .chart-dlq { min-width: 0; }
    .two-col { flex-direction: column; }
    .scenario-grid { grid-template-columns: 1fr; }
  }
`

// ─── Primitives ───────────────────────────────────────────────────────────────
function Tag({ col, children }) {
  const c = col || P.tm
  return (
    <span style={{
      display:'inline-block', background:c+'22', color:c,
      border:'1px solid '+c+'44', borderRadius:4,
      padding:'2px 7px', fontSize:9, fontWeight:700,
      letterSpacing:0.8, textTransform:'uppercase', whiteSpace:'nowrap',
    }}>{children}</span>
  )
}

function Kpi({ label, value, col, note }) {
  return (
    <div style={{
      background:P.panel, borderTop:'3px solid '+col,
      borderRadius:'0 0 8px 8px', padding:'10px 12px',
    }}>
      <div style={{ fontSize:8, color:P.tm, textTransform:'uppercase', letterSpacing:0.6, marginBottom:4 }}>{label}</div>
      <div style={{ fontSize:20, fontWeight:800, color:col, lineHeight:1, fontVariantNumeric:'tabular-nums' }}>{value}</div>
      {note && <div style={{ fontSize:8, color:P.tm, marginTop:3 }}>{note}</div>}
    </div>
  )
}

function Box({ title, children, accent, style }) {
  return (
    <div style={{
      background:P.panel, borderRadius:10,
      border:'1px solid '+(accent ? accent+'44' : P.border),
      padding:'16px', ...style,
    }}>
      {title && (
        <div style={{ fontSize:9, color:P.tm, fontWeight:700, textTransform:'uppercase', letterSpacing:1.2, marginBottom:14 }}>
          {title}
        </div>
      )}
      {children}
    </div>
  )
}

function Btn({ children, onClick, col, active, sm, disabled, block }) {
  const c = col || '#60a5fa'
  return (
    <button type="button" onClick={disabled ? undefined : onClick} style={{
      display: block ? 'block' : 'inline-flex', width: block ? '100%' : undefined,
      alignItems:'center', justifyContent:'center',
      background: active ? c+'30' : c+'18',
      border:'1px solid '+(active ? c+'90' : c+'40'),
      color: active ? c : c+'cc',
      borderRadius:6, padding: sm ? '5px 10px' : '8px 16px',
      fontSize: sm ? 10 : 12, fontWeight:600,
      cursor: disabled ? 'not-allowed' : 'pointer',
      opacity: disabled ? 0.4 : 1,
      whiteSpace:'nowrap', lineHeight:1.4,
      userSelect:'none', transition:'background 0.15s, border-color 0.15s',
    }}>{children}</button>
  )
}

function TIn({ value, onChange, placeholder, style }) {
  return (
    <input type="text" value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder}
      style={{ background:P.input, border:'1px solid '+P.border, color:P.th, borderRadius:6, padding:'8px 11px', fontSize:12, width:'100%', outline:'none', ...style }} />
  )
}
function NIn({ value, onChange, placeholder }) {
  return (
    <input type="number" value={value} onChange={e => onChange(e.target.value==='' ? 0 : Number(e.target.value))} placeholder={placeholder}
      style={{ background:P.input, border:'1px solid '+P.border, color:P.th, borderRadius:6, padding:'8px 11px', fontSize:12, width:'100%', outline:'none' }} />
  )
}
function Sel({ value, onChange, options, style }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ background:P.input, border:'1px solid '+P.border, color:P.th, borderRadius:6, padding:'8px 11px', fontSize:12, width:'100%', outline:'none', ...style }}>
      {options.map(o => <option key={o.value!=null?o.value:o} value={o.value!=null?o.value:o}>{o.label!=null?o.label:o}</option>)}
    </select>
  )
}
function Row({ children }) {
  return <div style={{ display:'flex', gap:8, marginBottom:8 }}>{children.map((c,i)=><div key={i} style={{flex:1,minWidth:0}}>{c}</div>)}</div>
}
function RangeRow({ label, value, min, max, step, onChange, fmt }) {
  return (
    <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:8 }}>
      {label && <span style={{ fontSize:11, color:P.tb, minWidth:90, flexShrink:0 }}>{label}</span>}
      <input type="range" min={min} max={max} step={step} value={value} onChange={e => onChange(Number(e.target.value))} style={{ flex:1, minWidth:0 }} />
      <span style={{ fontSize:12, color:P.th, minWidth:60, textAlign:'right', flexShrink:0, fontVariantNumeric:'tabular-nums' }}>
        {fmt ? fmt(value) : value}
      </span>
    </div>
  )
}

// ─── Pipeline ─────────────────────────────────────────────────────────────────
function PipeNode({ svc, count, chaotic, on }) {
  const c = svc.col
  return (
    <div style={{
      background:P.card,
      border:'1.5px solid '+(chaotic ? '#f87171' : on ? c+'80' : P.border),
      borderRadius:10, padding:'12px 14px', width:108, flexShrink:0, textAlign:'center',
      position:'relative',
      boxShadow: chaotic ? '0 0 16px #f8717128' : on ? '0 0 20px '+c+'28, 0 0 6px '+c+'18' : 'none',
      transition:'border-color 0.4s, box-shadow 0.4s',
    }}>
      {chaotic && (
        <div style={{ position:'absolute', top:-8, right:-8, background:'#ef4444', color:'#fff', borderRadius:'50%', width:18, height:18, display:'flex', alignItems:'center', justifyContent:'center', fontSize:11, fontWeight:800 }}>!</div>
      )}
      <div style={{ fontSize:11, fontWeight:700, color: chaotic ? '#f87171' : c, marginBottom:1, lineHeight:1.2 }}>{svc.name}</div>
      <div style={{ fontSize:8, color:P.tm, marginBottom:8 }}>{svc.sub}</div>
      <div style={{ fontSize:26, fontWeight:800, color: chaotic ? '#f87171cc' : P.th, lineHeight:1, fontVariantNumeric:'tabular-nums' }}>
        {count.toLocaleString()}
      </div>
      <div style={{ fontSize:8, color:P.td, marginTop:2 }}>events</div>
    </div>
  )
}

function Pipeline({ svcCounts, cmdCount, chaos, tph }) {
  const on = tph > 0
  return (
    <>
      {on && (
        <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:12 }}>
          <div style={{ height:1, flex:1, background:'linear-gradient(90deg,transparent,'+P.border+')' }} />
          <span style={{ fontSize:11, color:P.tm, whiteSpace:'nowrap' }}>
            <span style={{ color:'#60a5fa', fontWeight:700 }}>{tph}</span> evt/s in flight
          </span>
          <div style={{ height:1, flex:1, background:'linear-gradient(90deg,'+P.border+',transparent)' }} />
        </div>
      )}
      <div className="pipe-wrap">
        {PIPE.map((svc, i) => {
          const cs = chaos[svc.id]
          const chaotic = !!(cs && cs.mode && cs.mode !== '')
          const count = svc.id === 'gateway' ? cmdCount : (svcCounts[svc.id] || 0)
          return (
            <div key={svc.id} style={{ display:'flex', alignItems:'center', gap:8 }}>
              <PipeNode svc={svc} count={count} chaotic={chaotic} on={on} />
              {i < PIPE.length-1 && (
                <div style={{ display:'flex', alignItems:'center', flexShrink:0, opacity: on?1:0.3, animation: on?'pipeFlow 2s ease-in-out infinite':'none' }}>
                  <div style={{ width:20, height:2, background: on?'#2d5a8e':P.sep, borderRadius:1 }} />
                  <div style={{ width:0, height:0, borderTop:'5px solid transparent', borderBottom:'5px solid transparent', borderLeft:'7px solid '+(on?'#2d5a8e':P.sep) }} />
                </div>
              )}
            </div>
          )
        })}
      </div>
    </>
  )
}

// ─── Inspector ────────────────────────────────────────────────────────────────
function Inspector({ event, onClose }) {
  if (!event) return null
  let payload = null
  try { payload = JSON.parse(event.payload != null ? event.payload : 'null') } catch (_) { payload = event.payload }
  const c = TC[event.type] || P.tm
  return (
    <Box style={{ marginBottom:16 }} accent={c}>
      <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:10, flexWrap:'wrap' }}>
        <span style={{ fontSize:9, color:P.tm, fontWeight:700, textTransform:'uppercase', letterSpacing:1.2 }}>Inspector</span>
        <Tag col={c}>{event.type}</Tag>
        <span style={{ fontSize:11, color:P.tm }}>{event.source}</span>
        <button type="button" onClick={onClose} style={{ marginLeft:'auto', background:'none', border:'none', color:P.tm, cursor:'pointer', fontSize:16, padding:'0 4px', lineHeight:1 }}>✕</button>
      </div>
      <div style={{ display:'flex', gap:12, fontSize:10, marginBottom:10, color:P.tb, fontFamily:'monospace', flexWrap:'wrap' }}>
        <span>id: {(event.id||'').slice(0,12)}…</span>
        <span>corr: {event.correlation_id}</span>
        <span>ts: {new Date(event.timestamp).toISOString()}</span>
      </div>
      <pre style={{ background:P.input, borderRadius:6, padding:12, fontSize:11, color:P.th, margin:0, fontFamily:'monospace', whiteSpace:'pre-wrap', wordBreak:'break-all', maxHeight:180, overflowY:'auto' }}>
        {JSON.stringify(payload, null, 2)}
      </pre>
    </Box>
  )
}

// ─── Scenarios ────────────────────────────────────────────────────────────────
function ScenarioPanel() {
  const [running, setRunning] = useState(null)
  const timers = useRef([])
  const clearT = () => { timers.current.forEach(clearTimeout); timers.current = [] }
  const stopAll = useCallback(async () => {
    clearT(); setRunning(null)
    try { await apiPost('/simulator/control', { enabled:false, rate:1 }); await apiDel('/chaos') } catch (_) {}
  }, [])
  const run = useCallback(async (s) => {
    await stopAll()
    setRunning({ id:s.id, msg:'Starting…', step:0, total:s.steps.length })
    s.steps.forEach((step, idx) => {
      const tid = setTimeout(async () => {
        setRunning(prev => prev ? { ...prev, msg:step.msg, step:idx+1 } : null)
        try {
          if (step.sim)        await apiPost('/simulator/control', step.sim)
          if (step.chaos)      await apiPost('/chaos', step.chaos)
          if (step.chaosReset) await apiDel('/chaos')
        } catch (_) {}
        if (idx === s.steps.length-1) setTimeout(() => setRunning(null), 3000)
      }, step.t)
      timers.current.push(tid)
    })
  }, [stopAll])
  useEffect(() => () => clearT(), [])

  return (
    <Box title="Demo Scenarios — showcase Watermill capabilities">
      <div className="scenario-grid">
        {SCENARIOS.map(s => {
          const isOn = running?.id === s.id
          return (
            <div key={s.id} style={{
              background:P.card,
              borderLeft:'4px solid '+(isOn ? s.col : s.col+'50'),
              border:'1px solid '+(isOn ? s.col+'60' : P.border),
              borderRadius:'0 8px 8px 0',
              padding:'14px 14px',
              boxShadow: isOn ? '0 0 20px '+s.col+'22' : 'none',
              transition:'all 0.25s',
            }}>
              <div style={{ display:'flex', alignItems:'flex-start', gap:8, marginBottom:8 }}>
                <div style={{ flex:1, minWidth:0 }}>
                  <Tag col={s.col}>{s.tag}</Tag>
                  <div style={{ fontSize:14, fontWeight:700, color:P.th, marginTop:5, lineHeight:1.2 }}>{s.label}</div>
                </div>
                <Btn sm col={s.col} active={isOn} onClick={() => isOn ? stopAll() : run(s)}>
                  {isOn ? 'Stop' : 'Run ▶'}
                </Btn>
              </div>
              <div style={{ fontSize:11, color:P.tm, lineHeight:1.55 }}>{s.desc}</div>
              {isOn && (
                <div style={{ marginTop:10, paddingTop:10, borderTop:'1px solid '+P.border, display:'flex', alignItems:'center', gap:8 }}>
                  <span style={{ width:7, height:7, borderRadius:'50%', background:s.col, flexShrink:0, display:'inline-block', animation:'wmPulse 1.2s ease-in-out infinite' }} />
                  <span style={{ fontSize:11, color:s.col, flex:1, lineHeight:1.3 }}>{running.msg}</span>
                  <span style={{ fontSize:9, color:P.tm, flexShrink:0 }}>{running.step}/{running.total}</span>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </Box>
  )
}

// ─── Simulator ────────────────────────────────────────────────────────────────
function SimPanel() {
  const [status, setSt] = useState({ enabled:false, rate:1 })
  const [rate, setRate]  = useState(1)
  const [busy, setBusy]  = useState(false)
  const [msg,  setMsg]   = useState(null)
  const flash = m => { setMsg(m); setTimeout(() => setMsg(null), 2500) }
  const refr = useCallback(async () => {
    try { const d = await (await fetch(GW+'/simulator/status')).json(); setSt(d); setRate(d.rate) } catch (_) {}
  }, [])
  useEffect(() => { refr(); const id = setInterval(refr, 3000); return () => clearInterval(id) }, [refr])
  const apply = async (enabled, r) => {
    setBusy(true)
    try { const d = await apiPost('/simulator/control', { enabled, rate:r }); setSt(d); flash(enabled ? r.toFixed(1)+' evt/s' : 'Stopped') }
    catch (e) { flash('Error: '+e.message) } finally { setBusy(false) }
  }
  return (
    <Box title="Traffic Generator">
      <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:14 }}>
        <div style={{ width:9, height:9, borderRadius:'50%', background: status.enabled ? '#34d399' : P.tm, boxShadow: status.enabled ? '0 0 8px #34d39988' : 'none', transition:'all 0.3s', flexShrink:0 }} />
        <span style={{ fontSize:13, fontWeight:700, color: status.enabled ? '#34d399' : P.tb }}>
          {status.enabled ? status.rate.toFixed(1)+' evt/s' : 'Idle'}
        </span>
        {msg && <span style={{ fontSize:11, color:'#fbbf24', marginLeft:4 }}>{msg}</span>}
      </div>
      <RangeRow label="Rate (evt/s)" value={rate} min={0.1} max={20} step={0.1} onChange={setRate} fmt={v => v.toFixed(1)+'/s'} />
      <div style={{ display:'flex', gap:6, flexWrap:'wrap', marginTop:8 }}>
        <Btn col="#34d399" onClick={() => apply(true,  rate)} disabled={busy}>Start</Btn>
        <Btn col={P.tb}    onClick={() => apply(false, rate)} disabled={busy}>Stop</Btn>
        <Btn sm col="#fbbf24" onClick={() => apply(true, 5)}  disabled={busy}>5/s</Btn>
        <Btn sm col="#fb923c" onClick={() => apply(true, 10)} disabled={busy}>10/s</Btn>
        <Btn sm col="#f87171" onClick={() => apply(true, 20)} disabled={busy}>20/s</Btn>
      </div>
    </Box>
  )
}

// ─── Chaos ────────────────────────────────────────────────────────────────────
function ChaosPanel() {
  const [chaos, setCh] = useState({})
  const [busy, setBusy] = useState(false)
  const [msg,  setMsg]  = useState(null)
  const flash = m => { setMsg(m); setTimeout(() => setMsg(null), 2500) }
  const refr = useCallback(async () => {
    try { setCh(await (await fetch(GW+'/chaos/status')).json()) } catch (_) {}
  }, [])
  useEffect(() => { refr(); const id = setInterval(refr, 3000); return () => clearInterval(id) }, [refr])
  const inject = async (svc, mode, er) => {
    setBusy(true)
    try { await apiPost('/chaos', { service:svc, mode, error_rate:er }); flash(svc+': '+(mode||'off')); await refr() }
    catch (_) {} finally { setBusy(false) }
  }
  const reset = async () => {
    setBusy(true)
    try { await apiDel('/chaos'); flash('All faults cleared'); await refr() }
    catch (_) {} finally { setBusy(false) }
  }
  const anyOn = CHAOS_SVCS.some(s => chaos[s] && chaos[s].mode && chaos[s].mode !== '')
  return (
    <Box title="Chaos Engineering" accent={anyOn ? '#f87171' : undefined}>
      <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:12, flexWrap:'wrap' }}>
        <span style={{ fontSize:11, color: anyOn ? '#f87171' : '#34d399', fontWeight:600 }}>
          {anyOn ? '⚡ Fault injection active' : '✓ All services healthy'}
        </span>
        {msg && <span style={{ fontSize:11, color:'#fbbf24', marginLeft:4 }}>{msg}</span>}
        <div style={{ marginLeft:'auto' }}>
          <Btn sm col="#f87171" onClick={reset} disabled={busy}>Reset all</Btn>
        </div>
      </div>
      <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
        {CHAOS_SVCS.map(svc => {
          const s = chaos[svc] || { mode:'', error_rate:0 }
          const on = !!(s.mode && s.mode !== '')
          return (
            <div key={svc} style={{
              display:'flex', alignItems:'center', gap:8, flexWrap:'wrap',
              background: on ? '#f8717110' : P.card,
              border:'1px solid '+(on ? '#f8717140' : P.border),
              borderRadius:6, padding:'7px 10px', transition:'all 0.2s',
            }}>
              <div style={{ width:7, height:7, borderRadius:'50%', flexShrink:0, background: on ? '#f87171' : P.td, boxShadow: on ? '0 0 8px #f8717188' : 'none' }} />
              <span style={{ fontSize:10, color: on ? '#f87171' : P.tm, minWidth:140, fontFamily:'monospace', flexShrink:0 }}>{svc}</span>
              {on && <Tag col="#f87171">{s.mode} {Math.round(s.error_rate*100)}%</Tag>}
              <div style={{ marginLeft:'auto', display:'flex', gap:4 }}>
                <Btn sm col="#f87171" onClick={() => inject(svc,'poison',1.0)} disabled={busy}>Poison</Btn>
                <Btn sm col="#fbbf24" onClick={() => inject(svc,'drop',0.5)}   disabled={busy}>Drop50</Btn>
                <Btn sm col={P.tm}    onClick={() => inject(svc,'',0)}          disabled={busy}>Off</Btn>
              </div>
            </div>
          )
        })}
      </div>
    </Box>
  )
}

// ─── Injector ─────────────────────────────────────────────────────────────────
function Injector({ onResult }) {
  const [tab, setTab] = useState('admission')
  const [ap, setAp] = useState({ pid:'P1001', first:'Alice', last:'Smith', dob:'1980-06-15', ward:'ICU' })
  const [dp, setDp] = useState({ pid:'P1001', first:'Alice', last:'Smith', ward:'ICU', reason:'recovered' })
  const [tp, setTp] = useState({ pid:'P1001', first:'Alice', last:'Smith', from:'Emergency', to:'ICU', reason:'stabilized' })
  const [lp, setLp] = useState({ pid:'P1001', test:'Glucose', value:240 })
  const [alp, setAlp] = useState({ pid:'P1001', sev:'critical', cat:'vital', msg:'SpO2 dropped below 88%', val:86, thr:95 })
  const lt = LAB_TESTS.find(x => x.name===lp.test) || LAB_TESTS[3]
  const post = async (path, body) => {
    try {
      const r = await fetch(GW+path, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) })
      const d = await r.json()
      onResult(r.ok ? 'sent — corr: '+((d.correlation_id||'').slice(0,8)) : 'Error: '+d.error)
    } catch (e) { onResult('Error: '+e.message) }
  }
  const TABS = [
    { id:'admission', label:'Admission', col:'#38bdf8' },
    { id:'discharge', label:'Discharge', col:'#86efac' },
    { id:'transfer',  label:'Transfer',  col:'#fde68a' },
    { id:'lab',       label:'Lab Result',col:'#34d399' },
    { id:'alert',     label:'Alert',     col:'#f87171' },
  ]
  return (
    <Box title="Manual Event Injection">
      <div style={{ display:'flex', gap:4, marginBottom:16, flexWrap:'wrap' }}>
        {TABS.map(tb => (
          <button key={tb.id} type="button" onClick={() => setTab(tb.id)} style={{
            background: tab===tb.id ? tb.col+'20' : 'transparent',
            border:'1px solid '+(tab===tb.id ? tb.col+'60' : P.border),
            color: tab===tb.id ? tb.col : P.tm,
            borderRadius:5, padding:'7px 14px', fontSize:11, fontWeight:600, cursor:'pointer',
          }}>{tb.label}</button>
        ))}
      </div>
      {tab==='admission' && (
        <div>
          <Row>{[<TIn value={ap.pid}   onChange={v=>setAp(p=>({...p,pid:v}))}   placeholder="Patient ID *" />,
                  <TIn value={ap.dob}   onChange={v=>setAp(p=>({...p,dob:v}))}   placeholder="Date of birth" />]}</Row>
          <Row>{[<TIn value={ap.first} onChange={v=>setAp(p=>({...p,first:v}))} placeholder="First name *" />,
                  <TIn value={ap.last}  onChange={v=>setAp(p=>({...p,last:v}))}  placeholder="Last name *"  />]}</Row>
          <Row>{[<Sel value={ap.ward}  onChange={v=>setAp(p=>({...p,ward:v}))}  options={WARDS} />,
                  <Btn block col="#38bdf8" onClick={() => post('/admissions', { patient_id:ap.pid, first_name:ap.first, last_name:ap.last, date_of_birth:ap.dob, ward:ap.ward })}>Send Admission</Btn>]}</Row>
        </div>
      )}
      {tab==='discharge' && (
        <div>
          <Row>{[<TIn value={dp.pid}   onChange={v=>setDp(p=>({...p,pid:v}))}   placeholder="Patient ID *" />,
                  <Sel value={dp.ward}  onChange={v=>setDp(p=>({...p,ward:v}))}  options={WARDS} />]}</Row>
          <Row>{[<TIn value={dp.first} onChange={v=>setDp(p=>({...p,first:v}))} placeholder="First name" />,
                  <TIn value={dp.last}  onChange={v=>setDp(p=>({...p,last:v}))}  placeholder="Last name"  />]}</Row>
          <Row>{[<Sel value={dp.reason} onChange={v=>setDp(p=>({...p,reason:v}))} options={['recovered','transferred','deceased','self-discharge']} />,
                  <Btn block col="#86efac" onClick={() => post('/discharges', { patient_id:dp.pid, first_name:dp.first, last_name:dp.last, ward:dp.ward, reason:dp.reason })}>Send Discharge</Btn>]}</Row>
        </div>
      )}
      {tab==='transfer' && (
        <div>
          <Row>{[<TIn value={tp.pid}    onChange={v=>setTp(p=>({...p,pid:v}))}    placeholder="Patient ID *" />,
                  <TIn value={tp.reason} onChange={v=>setTp(p=>({...p,reason:v}))} placeholder="Reason"       />]}</Row>
          <Row>{[<TIn value={tp.first}  onChange={v=>setTp(p=>({...p,first:v}))}  placeholder="First name"   />,
                  <TIn value={tp.last}   onChange={v=>setTp(p=>({...p,last:v}))}   placeholder="Last name"    />]}</Row>
          <Row>{[<Sel value={tp.from}   onChange={v=>setTp(p=>({...p,from:v}))}   options={WARDS} />,
                  <Sel value={tp.to}     onChange={v=>setTp(p=>({...p,to:v}))}     options={WARDS} />]}</Row>
          <Btn block col="#fde68a" onClick={() => post('/transfers', { patient_id:tp.pid, first_name:tp.first, last_name:tp.last, from_ward:tp.from, to_ward:tp.to, reason:tp.reason })}>Send Transfer</Btn>
        </div>
      )}
      {tab==='lab' && (
        <div>
          <Row>{[<TIn value={lp.pid} onChange={v=>setLp(p=>({...p,pid:v}))} placeholder="Patient ID *" />,
                  <Sel value={lp.test} onChange={v=>setLp(p=>({...p,test:v,value:(LAB_TESTS.find(x=>x.name===v)||LAB_TESTS[3]).lo}))} options={LAB_TESTS.map(x=>x.name)} />]}</Row>
          <RangeRow value={lp.value} min={0} max={lt.hi*2} step={0.1} onChange={v=>setLp(p=>({...p,value:v}))} fmt={v => v.toFixed(1)+' '+lt.unit} />
          <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:10 }}>
            <span style={{ fontSize:11, color:P.tm }}>Ref: {lt.lo}–{lt.hi} {lt.unit}</span>
            {(lp.value < lt.lo || lp.value > lt.hi) && <Tag col="#f87171">ABNORMAL</Tag>}
          </div>
          <Btn block col="#34d399" onClick={() => post('/lab-results', { patient_id:lp.pid, test_name:lp.test, value:lp.value, unit:lt.unit, reference_lo:lt.lo, reference_hi:lt.hi })}>Send Lab Result</Btn>
        </div>
      )}
      {tab==='alert' && (
        <div>
          <Row>{[<TIn value={alp.pid} onChange={v=>setAlp(p=>({...p,pid:v}))} placeholder="Patient ID *" />,
                  <Sel value={alp.sev} onChange={v=>setAlp(p=>({...p,sev:v}))} options={['low','medium','high','critical']} />]}</Row>
          <Row>{[<Sel value={alp.cat} onChange={v=>setAlp(p=>({...p,cat:v}))} options={['vital','lab','medication','system']} />,
                  <TIn value={alp.msg} onChange={v=>setAlp(p=>({...p,msg:v}))} placeholder="Message *" />]}</Row>
          <Row>{[<NIn value={alp.val} onChange={v=>setAlp(p=>({...p,val:v}))} placeholder="Current value" />,
                  <NIn value={alp.thr} onChange={v=>setAlp(p=>({...p,thr:v}))} placeholder="Threshold"     />]}</Row>
          <Btn block col="#f87171" onClick={() => post('/alerts', { patient_id:alp.pid, severity:alp.sev, category:alp.cat, message:alp.msg, value:Number(alp.val), threshold:Number(alp.thr) })}>Send Alert</Btn>
        </div>
      )}
    </Box>
  )
}

// ─── App ──────────────────────────────────────────────────────────────────────
export default function App() {
  const { events, connected, tickRef } = useSSE('/events/stream')
  const [tphHist, setTphHist] = useState([])
  const [selected, setSelected] = useState(null)
  const [tf,  setTf]  = useState('all')
  const [qf,  setQf]  = useState('')
  const [injMsg, setInjMsg] = useState(null)
  const [chaos, setChaos]   = useState({})
  const w = useWidth()

  useEffect(() => {
    const id = setInterval(() => {
      const snap = Object.assign({}, tickRef.current)
      tickRef.current = {}
      const tot = Object.values(snap).reduce((s,v) => s+v, 0)
      setTphHist(prev => prev.concat([{ s:new Date().toLocaleTimeString(), tot, ...snap }]).slice(-60))
    }, 1000)
    return () => clearInterval(id)
  }, [tickRef])

  useEffect(() => {
    const poll = async () => { try { setChaos(await (await fetch(GW+'/chaos/status')).json()) } catch (_) {} }
    poll(); const id = setInterval(poll, 4000); return () => clearInterval(id)
  }, [])

  useEffect(() => {
    if (!injMsg) return; const id = setTimeout(() => setInjMsg(null), 3000); return () => clearTimeout(id)
  }, [injMsg])

  const cnt   = events.reduce((a,e) => { a[e.type]=(a[e.type]||0)+1; return a }, {})
  const svcC  = events.reduce((a,e) => { a[e.source]=(a[e.source]||0)+1; return a }, {})
  const dlqEv = events.filter(e => e.type && e.type.endsWith('-dlq'))
  const altEv = events.filter(e => e.type === 'alert-created')
  const crit  = altEv.filter(e => { try { return JSON.parse(e.payload).severity==='critical' } catch(_){return false} })
  const cmd     = cnt['command-patient-admit'] || 0
  const down    = (cnt['patient-admitted']||0)+(cnt['lab-result-created']||0)+(cnt['fhir-document-created']||0)+(cnt['notification-sent']||0)
  const fanout  = cmd > 0 ? (down/cmd).toFixed(1) : '—'
  const curTph  = tphHist.length > 0 ? (tphHist[tphHist.length-1].tot || 0) : 0
  const anyOn   = CHAOS_SVCS.some(s => chaos[s] && chaos[s].mode && chaos[s].mode !== '')
  const pad     = w < 640 ? '12px' : '20px 24px'

  const typeOpts = [
    { value:'all', label:'All types' },
    ...Object.keys(TC).map(k => ({ value:k, label:k })),
    { value:'_dlq', label:'*.dlq (DLQ)' },
  ]
  const filtered = events.filter(e => {
    if (tf === '_dlq') return e.type && e.type.endsWith('-dlq')
    if (tf !== 'all' && e.type !== tf) return false
    if (qf) {
      const q = qf.toLowerCase()
      return (e.correlation_id && e.correlation_id.toLowerCase().includes(q))
          || (e.source && e.source.toLowerCase().includes(q))
          || (e.type && e.type.toLowerCase().includes(q))
    }
    return true
  }).slice(0, 100)

  const barD = Object.entries(cnt).filter(([t]) => !t.endsWith('-dlq')).map(([type, count]) => ({
    name: type.split('-').slice(0,2).join('-'), count, fill: TC[type]||'#60a5fa',
  }))

  return (
    <div style={{ minHeight:'100vh', fontFamily:'system-ui,-apple-system,sans-serif', background:P.page, backgroundImage:'radial-gradient(circle, rgba(26,58,106,0.14) 1px, transparent 1px)', backgroundSize:'28px 28px' }}>
      <style>{CSS}</style>

      {/* Header */}
      <div style={{ background:P.panel, borderBottom:'1px solid '+P.border, padding:'12px '+pad, display:'flex', alignItems:'center', gap:12, flexWrap:'wrap', position:'sticky', top:0, zIndex:100 }}>
        <div>
          <span style={{ fontSize:16, fontWeight:800, color:P.th, letterSpacing:-0.3 }}>Health ESB</span>
          {w >= 480 && <span style={{ fontSize:10, color:P.tm, marginLeft:8 }}>event-driven architecture demo</span>}
        </div>
        {w >= 480 && (
          <div style={{ display:'flex', alignItems:'center', gap:5 }}>
            <span style={{ fontSize:9, color:P.td }}>powered by</span>
            <span style={{ fontSize:11, fontWeight:800, color:'#60a5fa', letterSpacing:1 }}>WATERMILL</span>
          </div>
        )}
        {injMsg && <div style={{ fontSize:11, color:'#34d399', background:'#34d39918', border:'1px solid #34d39928', borderRadius:4, padding:'3px 10px' }}>{injMsg}</div>}
        <div style={{ marginLeft:'auto', display:'flex', gap:8, alignItems:'center' }}>
          {anyOn      && <Tag col="#f87171">{w<480?'CHAOS':'CHAOS ACTIVE'}</Tag>}
          {crit.length > 0 && <Tag col="#ef4444">{crit.length}⚠</Tag>}
          <div style={{ display:'flex', alignItems:'center', gap:5 }}>
            <div style={{ width:8, height:8, borderRadius:'50%', background: connected?'#34d399':'#f87171', boxShadow: connected?'0 0 8px #34d39988':'none' }} />
            <span style={{ fontSize:11, color: connected?'#34d399':'#f87171', fontWeight:600 }}>{connected?'LIVE':'OFF'}</span>
          </div>
        </div>
      </div>

      <div style={{ padding:pad, maxWidth:1520, margin:'0 auto' }}>

        {/* KPI grid */}
        <div className="kpi-grid" style={{ marginBottom:16 }}>
          <Kpi label="Total"      value={events.length}                        col="#60a5fa" />
          <Kpi label="Admitted"   value={cnt['patient-admitted']      ||0}     col="#38bdf8" />
          <Kpi label="Discharged" value={cnt['patient-discharged']    ||0}     col="#86efac" />
          <Kpi label="Transferred"value={cnt['patient-transferred']   ||0}     col="#fde68a" />
          <Kpi label="Lab"        value={cnt['lab-result-created']    ||0}     col="#34d399" />
          <Kpi label="FHIR"       value={cnt['fhir-document-created'] ||0}     col="#a78bfa" />
          <Kpi label="Notify"     value={cnt['notification-sent']     ||0}     col="#fb923c" />
          <Kpi label="Alerts"     value={cnt['alert-created']         ||0}     col="#f87171" note={crit.length>0?crit.length+' crit':undefined} />
          <Kpi label="DLQ"        value={dlqEv.length}                         col="#f87171" note={dlqEv.length>0?'~'+(dlqEv.length*5)+' retries':undefined} />
          <Kpi label="Fan-out"    value={fanout}                               col="#a78bfa" note="downstream/cmd" />
          <Kpi label="Evt/s"      value={curTph}                               col="#fbbf24" />
        </div>

        {/* Pipeline */}
        <Box title="Message Pipeline — Watermill fan-out router" style={{ marginBottom:16 }}>
          <Pipeline svcCounts={svcC} cmdCount={cmd} chaos={chaos} tph={curTph} />
        </Box>

        {/* Charts row */}
        <div className="charts-row" style={{ marginBottom:16 }}>
          <Box title="Throughput — events per second" className="chart-main" style={{ flex:'3 1 300px', minWidth:260 }}>
            {tphHist.length < 2 ? (
              <div style={{ height:160, display:'flex', alignItems:'center', justifyContent:'center', color:P.tm, fontSize:12 }}>Waiting for events…</div>
            ) : (
              <ResponsiveContainer width="100%" height={160}>
                <AreaChart data={tphHist} margin={{ top:4, right:4, bottom:0, left:-14 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                  <XAxis dataKey="s" tick={{ fill:P.tm, fontSize:8 }} interval="preserveStartEnd" />
                  <YAxis allowDecimals={false} tick={{ fill:P.tm, fontSize:9 }} />
                  <Tooltip contentStyle={{ background:P.panel, border:'1px solid '+P.border, color:P.th, fontSize:11 }} labelStyle={{ color:P.tb }} />
                  {Object.entries(TC).map(([k,v]) => (
                    <Area key={k} type="monotone" dataKey={k} stroke={v} fill={v+'18'} stackId="s" isAnimationActive={false} dot={false} />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            )}
          </Box>

          <Box title="Events by Type" style={{ flex:'1 1 180px', minWidth:160 }}>
            {barD.length === 0 ? (
              <div style={{ height:160, display:'flex', alignItems:'center', justifyContent:'center', color:P.tm, fontSize:12 }}>No data</div>
            ) : (
              <ResponsiveContainer width="100%" height={160}>
                <BarChart data={barD} margin={{ top:4, right:4, bottom:24, left:-22 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                  <XAxis dataKey="name" tick={{ fill:P.tm, fontSize:7 }} angle={-35} textAnchor="end" />
                  <YAxis tick={{ fill:P.tm, fontSize:9 }} />
                  <Tooltip contentStyle={{ background:P.panel, border:'1px solid '+P.border, color:P.th, fontSize:11 }} />
                  <Bar dataKey="count" radius={[3,3,0,0]} isAnimationActive={false}>
                    {barD.map((e,i) => <Cell key={i} fill={e.fill} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </Box>

          <Box title="DLQ & Alerts" style={{ flex:'1 1 160px', minWidth:140 }}>
            {dlqEv.length===0 && altEv.length===0 ? (
              <div style={{ height:160, display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', gap:6 }}>
                <div style={{ fontSize:24, color:'#34d399', fontWeight:800 }}>OK</div>
                <div style={{ fontSize:11, color:P.tm }}>No failures</div>
              </div>
            ) : (
              <div style={{ overflowY:'auto', maxHeight:160 }}>
                {dlqEv.slice(0,15).map((e,i) => (
                  <div key={'d'+i} onClick={() => setSelected(prev=>prev===e?null:e)}
                    style={{ background:'#f8717112', borderRadius:4, padding:'4px 8px', marginBottom:3, fontSize:10, cursor:'pointer', display:'flex', gap:6, alignItems:'center' }}>
                    <Tag col="#f87171">DLQ</Tag>
                    <span style={{ color:P.tm, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{e.type?e.type.replace('-dlq',''):''}</span>
                  </div>
                ))}
                {altEv.slice(0,10).map((e,i) => {
                  let sev='low'; try { sev=JSON.parse(e.payload).severity } catch (_) {}
                  return (
                    <div key={'a'+i} onClick={() => setSelected(prev=>prev===e?null:e)}
                      style={{ background:(SEV[sev]||'#34d399')+'12', borderRadius:4, padding:'4px 8px', marginBottom:3, fontSize:10, cursor:'pointer', display:'flex', gap:6, alignItems:'center' }}>
                      <Tag col={SEV[sev]||'#34d399'}>{sev}</Tag>
                      <span style={{ color:P.tm }}>alert</span>
                    </div>
                  )
                })}
              </div>
            )}
          </Box>
        </div>

        {/* Scenarios */}
        <div style={{ marginBottom:16 }}><ScenarioPanel /></div>

        {/* Simulator + Chaos */}
        <div className="two-col" style={{ marginBottom:16 }}>
          <SimPanel />
          <ChaosPanel />
        </div>

        {/* Injector */}
        <div style={{ marginBottom:16 }}><Injector onResult={setInjMsg} /></div>

        {/* Inspector */}
        {selected && <Inspector event={selected} onClose={() => setSelected(null)} />}

        {/* Live stream */}
        <Box>
          <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:14, flexWrap:'wrap' }}>
            <span style={{ fontSize:9, fontWeight:700, color:P.tm, textTransform:'uppercase', letterSpacing:1.2 }}>Live Event Stream</span>
            <span style={{ fontSize:10, color:P.td }}>tap a row to inspect</span>
            <div style={{ marginLeft:'auto', display:'flex', gap:6, flexWrap:'wrap' }}>
              <Sel value={tf} onChange={setTf} options={typeOpts} style={{ width: w<640 ? '100%' : 200 }} />
              <TIn value={qf} onChange={setQf} placeholder="Search corr-id / source…" style={{ width: w<640 ? '100%' : 210 }} />
              {qf && <Btn sm col={P.tm} onClick={() => setQf('')}>✕</Btn>}
            </div>
          </div>
          <div style={{ overflowY:'auto', maxHeight:400, fontFamily:'monospace' }}>
            {filtered.map((e, i) => (
              <div key={i} onClick={() => setSelected(prev=>prev===e?null:e)}
                style={{
                  display:'flex', alignItems:'center', gap:6, flexWrap:'wrap',
                  padding:'6px 6px', borderBottom:'1px solid '+P.sep,
                  fontSize:11, cursor:'pointer', borderRadius:4,
                  background: selected===e ? P.card : 'transparent',
                }}>
                <span style={{ color:P.td, fontSize:9, flexShrink:0, minWidth:56 }}>{new Date(e.timestamp).toLocaleTimeString()}</span>
                <Tag col={TC[e.type]||P.tm}>{e.type}</Tag>
                {w >= 480 && <span style={{ color:P.tm, minWidth:90, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', fontSize:10 }}>{e.source}</span>}
                {w >= 640 && <span style={{ color:P.td, fontSize:9 }}>{e.correlation_id?(e.correlation_id.slice(0,8)):''}</span>}
                <div style={{ marginLeft:'auto', display:'flex', gap:4, flexShrink:0 }}>
                  {e.type==='alert-created'&&(()=>{try{const p=JSON.parse(e.payload);return<Tag col={SEV[p.severity]||P.tm}>{p.severity}</Tag>}catch(_){return null}})()}
                  {e.type==='lab-result-created'&&(()=>{try{const p=JSON.parse(e.payload);return p.abnormal?<Tag col="#f87171">!</Tag>:null}catch(_){return null}})()}
                  {e.type&&e.type.endsWith('-dlq')&&<Tag col="#f87171">DLQ</Tag>}
                  {w >= 480 && <Btn sm col={P.border} onClick={ev=>{ev.stopPropagation();setQf(e.correlation_id||'');setTf('all')}}>Trace</Btn>}
                </div>
              </div>
            ))}
            {filtered.length===0 && (
              <div style={{ color:P.tm, textAlign:'center', padding:32, fontSize:12 }}>
                {events.length===0 ? 'Waiting for events…' : 'No events match the filter'}
              </div>
            )}
          </div>
          <div style={{ marginTop:6, fontSize:9, color:P.td, textAlign:'right' }}>{filtered.length}/{events.length} shown</div>
        </Box>

      </div>
    </div>
  )
}
