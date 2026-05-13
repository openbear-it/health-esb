import { useState, useEffect, useRef, useCallback } from 'react'
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, Cell,
} from 'recharts'

const GW = ''

// ── Palette ──────────────────────────────────────────────────────────────────
const P = {
  page:   '#05111f',
  panel:  '#0a1c32',
  card:   '#0d2240',
  input:  '#061426',
  border: '#1a3554',
  sep:    '#0f2035',
  th:  '#ddeeff',
  tb:  '#6a9abf',
  tm:  '#3a6080',
  td:  '#1e3a52',
}

// ── Event topic colours ───────────────────────────────────────────────────────
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

// ── Service pipeline definition ───────────────────────────────────────────────
const PIPE = [
  { id:'gateway',              name:'Gateway',    sub:'REST + sim',    col:'#60a5fa' },
  { id:'adt-service',          name:'ADT',        sub:'Admit/DC/TR',   col:'#38bdf8' },
  { id:'lab-service',          name:'Lab',        sub:'Results',       col:'#34d399' },
  { id:'fhir-bridge',          name:'FHIR',       sub:'HL7 docs',      col:'#a78bfa' },
  { id:'notification-service', name:'Notify',     sub:'Alerts & SMS',  col:'#fb923c' },
  { id:'audit-service',        name:'Audit',      sub:'Compliance',    col:'#f9a8d4' },
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

// ── Demo scenarios ────────────────────────────────────────────────────────────
const SCENARIOS = [
  {
    id:'fanout', label:'Fan-out Demo', col:'#38bdf8', tag:'Routing',
    desc:'One patient admission triggers ADT, Lab, FHIR, Notifications and Audit in parallel — all wired by the Watermill router with zero boilerplate.',
    steps:[
      { t:0,     msg:'Simulator at 2 evt/s — watch the fan-out ratio grow!', sim:{ enabled:true,  rate:2 } },
      { t:30000, msg:'Done.',                                                 sim:{ enabled:false, rate:2 } },
    ],
  },
  {
    id:'scale', label:'Scale-Up', col:'#fbbf24', tag:'Throughput',
    desc:'Ramp from 1 to 20 events/sec with zero code changes, zero restarts and zero message loss.',
    steps:[
      { t:0,     msg:'1 evt/s baseline...',             sim:{ enabled:true,  rate:1  } },
      { t:4000,  msg:'Ramping to 5 evt/s...',           sim:{ enabled:true,  rate:5  } },
      { t:8000,  msg:'Ramping to 10 evt/s...',          sim:{ enabled:true,  rate:10 } },
      { t:12000, msg:'20 evt/s — full throughput!',     sim:{ enabled:true,  rate:20 } },
      { t:17000, msg:'Slowing back down...',            sim:{ enabled:true,  rate:3  } },
      { t:21000, msg:'Done.',                           sim:{ enabled:false, rate:1  } },
    ],
  },
  {
    id:'dlq', label:'Retry + DLQ', col:'#f87171', tag:'Resilience',
    desc:'Poison lab-service: Watermill retries 5x with exponential backoff then routes failed messages to the Dead Letter Queue automatically.',
    steps:[
      { t:0,     msg:'System healthy at 3 evt/s...',               sim:{ enabled:true,  rate:3 } },
      { t:4000,  msg:'lab-service poisoned — watch the DLQ grow!', chaos:{ service:'lab-service', mode:'poison', error_rate:1.0 } },
      { t:18000, msg:'Chaos reset — Watermill auto-recovers...',    chaosReset:true },
      { t:22000, msg:'Fully recovered. Done.',                      sim:{ enabled:false, rate:1 } },
    ],
  },
  {
    id:'cascade', label:'Cascade Failure', col:'#fb923c', tag:'Fault Isolation',
    desc:'Poison 3 services simultaneously: each fails and recovers independently — no domino effect, no data loss.',
    steps:[
      { t:0,     msg:'All services healthy at 4 evt/s...',          sim:{ enabled:true, rate:4 } },
      { t:4000,  msg:'Poisoning lab-service...',                     chaos:{ service:'lab-service',          mode:'poison', error_rate:1.0 } },
      { t:7000,  msg:'Poisoning fhir-bridge...',                     chaos:{ service:'fhir-bridge',          mode:'poison', error_rate:1.0 } },
      { t:10000, msg:'Dropping 50% in notification-service...',     chaos:{ service:'notification-service', mode:'drop',   error_rate:0.5 } },
      { t:16000, msg:'Resetting ALL faults — watch recovery!',       chaosReset:true },
      { t:21000, msg:'Done.',                                        sim:{ enabled:false, rate:1 } },
    ],
  },
]

// ── API helpers ───────────────────────────────────────────────────────────────
async function apiPost(path, body) {
  const r = await fetch(GW+path, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) })
  return r.json()
}
async function apiDel(path) { await fetch(GW+path, { method:'DELETE' }) }

// ── SSE hook ──────────────────────────────────────────────────────────────────
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

// ── Primitives ────────────────────────────────────────────────────────────────

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
      borderRadius:'0 0 8px 8px', padding:'10px 14px',
      flex:'1 1 85px', minWidth:85,
    }}>
      <div style={{ fontSize:9, color:P.tm, textTransform:'uppercase', letterSpacing:0.6, marginBottom:5 }}>{label}</div>
      <div style={{ fontSize:22, fontWeight:800, color:col, lineHeight:1, fontVariantNumeric:'tabular-nums' }}>{value}</div>
      {note && <div style={{ fontSize:9, color:P.tm, marginTop:3 }}>{note}</div>}
    </div>
  )
}

function Box({ title, children, accent, style }) {
  return (
    <div style={{
      background:P.panel, borderRadius:10,
      border:'1px solid '+(accent ? accent+'44' : P.border),
      padding:'16px 20px', ...style,
    }}>
      {title && (
        <div style={{ fontSize:9, color:P.tm, fontWeight:700, textTransform:'uppercase', letterSpacing:1.5, marginBottom:14 }}>
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
      display:block ? 'block' : 'inline-flex', width:block ? '100%' : undefined,
      alignItems:'center', justifyContent:'center',
      background: active ? c+'30' : c+'18',
      border:'1px solid '+(active ? c+'90' : c+'40'),
      color: active ? c : c+'cc',
      borderRadius:6, padding: sm ? '4px 10px' : '7px 16px',
      fontSize: sm ? 10 : 12, fontWeight:600,
      cursor: disabled ? 'not-allowed' : 'pointer',
      opacity: disabled ? 0.4 : 1,
      whiteSpace:'nowrap', lineHeight:1.4,
      boxSizing:'border-box', userSelect:'none',
      transition:'background 0.15s, border-color 0.15s',
    }}>{children}</button>
  )
}

function TIn({ value, onChange, placeholder, style }) {
  return (
    <input type="text" value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder}
      style={{ background:P.input, border:'1px solid '+P.border, color:P.th, borderRadius:6, padding:'7px 11px', fontSize:12, width:'100%', boxSizing:'border-box', outline:'none', fontFamily:'inherit', ...style }} />
  )
}

function NIn({ value, onChange, placeholder }) {
  return (
    <input type="number" value={value} onChange={e => onChange(e.target.value==='' ? 0 : Number(e.target.value))} placeholder={placeholder}
      style={{ background:P.input, border:'1px solid '+P.border, color:P.th, borderRadius:6, padding:'7px 11px', fontSize:12, width:'100%', boxSizing:'border-box', outline:'none', fontFamily:'inherit' }} />
  )
}

function Sel({ value, onChange, options, style }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ background:P.input, border:'1px solid '+P.border, color:P.th, borderRadius:6, padding:'7px 11px', fontSize:12, width:'100%', boxSizing:'border-box', outline:'none', fontFamily:'inherit', ...style }}>
      {options.map(o => <option key={o.value!=null?o.value:o} value={o.value!=null?o.value:o}>{o.label!=null?o.label:o}</option>)}
    </select>
  )
}

function Range({ label, value, min, max, step, onChange, fmt }) {
  return (
    <div style={{ display:'flex', alignItems:'center', gap:12 }}>
      {label && <span style={{ fontSize:11, color:P.tb, minWidth:95, flexShrink:0 }}>{label}</span>}
      <input type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(Number(e.target.value))}
        style={{ flex:1, accentColor:'#60a5fa', cursor:'pointer', minWidth:0 }} />
      <span style={{ fontSize:12, color:P.th, minWidth:62, textAlign:'right', flexShrink:0, fontVariantNumeric:'tabular-nums' }}>
        {fmt ? fmt(value) : value}
      </span>
    </div>
  )
}

// ── Pipeline ──────────────────────────────────────────────────────────────────
function PipeArrow({ on }) {
  const c = on ? '#2d5a8e' : P.sep
  return (
    <div style={{ display:'flex', alignItems:'center', flexShrink:0, opacity: on ? 1 : 0.3, animation: on ? 'pipeflow 2s ease-in-out infinite' : 'none' }}>
      <div style={{ width:24, height:2, background:c, borderRadius:1 }} />
      <div style={{ width:0, height:0, borderTop:'5px solid transparent', borderBottom:'5px solid transparent', borderLeft:'7px solid '+c }} />
    </div>
  )
}

function PipeNode({ svc, count, chaotic, on }) {
  const c = svc.col
  return (
    <div style={{
      background:P.card,
      border:'1.5px solid '+(chaotic ? '#f87171' : on ? c+'80' : P.border),
      borderRadius:10, padding:'14px 16px', width:112, flexShrink:0, textAlign:'center',
      position:'relative',
      boxShadow: chaotic ? '0 0 16px #f8717128' : on ? '0 0 22px '+c+'28, 0 0 8px '+c+'18' : 'none',
      transition:'border-color 0.4s, box-shadow 0.4s',
    }}>
      {chaotic && (
        <div style={{ position:'absolute', top:-9, right:-9, background:'#ef4444', color:'#fff', borderRadius:'50%', width:20, height:20, display:'flex', alignItems:'center', justifyContent:'center', fontSize:12, fontWeight:800, lineHeight:1 }}>!</div>
      )}
      <div style={{ fontSize:11, fontWeight:700, color: chaotic ? '#f87171' : c, marginBottom:2, lineHeight:1.2 }}>{svc.name}</div>
      <div style={{ fontSize:9, color:P.tm, marginBottom:10 }}>{svc.sub}</div>
      <div style={{ fontSize:28, fontWeight:800, color: chaotic ? '#f87171cc' : P.th, lineHeight:1, fontVariantNumeric:'tabular-nums' }}>
        {count.toLocaleString()}
      </div>
      <div style={{ fontSize:9, color:P.td, marginTop:3 }}>events</div>
    </div>
  )
}

function Pipeline({ svcCounts, cmdCount, chaos, tph }) {
  const on = tph > 0
  return (
    <div>
      {on && (
        <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:14 }}>
          <div style={{ height:1, flex:1, background:'linear-gradient(90deg,transparent,'+P.border+')' }} />
          <span style={{ fontSize:11, color:P.tm, whiteSpace:'nowrap' }}>
            <span style={{ color:'#60a5fa', fontWeight:700 }}>{tph}</span> evt/s in flight
          </span>
          <div style={{ height:1, flex:1, background:'linear-gradient(90deg,'+P.border+',transparent)' }} />
        </div>
      )}
      <div style={{ display:'flex', alignItems:'center', gap:8, overflowX:'auto', paddingBottom:4 }}>
        {PIPE.map((svc, i) => {
          const cs = chaos[svc.id]
          const chaotic = !!(cs && cs.mode && cs.mode !== '')
          const count = svc.id === 'gateway' ? cmdCount : (svcCounts[svc.id] || 0)
          return (
            <div key={svc.id} style={{ display:'flex', alignItems:'center', gap:8 }}>
              <PipeNode svc={svc} count={count} chaotic={chaotic} on={on} />
              {i < PIPE.length-1 && <PipeArrow on={on && !chaotic} />}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Message inspector ─────────────────────────────────────────────────────────
function Inspector({ event, onClose }) {
  if (!event) return null
  let payload = null
  try { payload = JSON.parse(event.payload != null ? event.payload : 'null') } catch (_) { payload = event.payload }
  const c = TC[event.type] || P.tm
  return (
    <Box style={{ marginBottom:16 }} accent={c}>
      <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:12, flexWrap:'wrap' }}>
        <span style={{ fontSize:9, color:P.tm, fontWeight:700, textTransform:'uppercase', letterSpacing:1.5 }}>Message Inspector</span>
        <Tag col={c}>{event.type}</Tag>
        <span style={{ fontSize:11, color:P.tm }}>{event.source}</span>
        <button type="button" onClick={onClose} style={{ marginLeft:'auto', background:'none', border:'none', color:P.tm, cursor:'pointer', fontSize:18, padding:'0 4px', lineHeight:1 }}>x</button>
      </div>
      <div style={{ display:'flex', gap:16, fontSize:10, marginBottom:12, color:P.tb, fontFamily:'monospace', flexWrap:'wrap' }}>
        <span>id: {(event.id||'').slice(0,16)}...</span>
        <span>corr: {event.correlation_id}</span>
        <span>ts: {new Date(event.timestamp).toISOString()}</span>
      </div>
      <pre style={{ background:P.input, borderRadius:6, padding:12, fontSize:11, color:P.th, margin:0, fontFamily:'monospace', whiteSpace:'pre-wrap', wordBreak:'break-all', maxHeight:200, overflowY:'auto', overflowX:'auto' }}>
        {JSON.stringify(payload, null, 2)}
      </pre>
    </Box>
  )
}

// ── Scenario panel ────────────────────────────────────────────────────────────
function ScenarioPanel() {
  const [running, setRunning] = useState(null)
  const timers = useRef([])
  const clearT = useCallback(() => { timers.current.forEach(clearTimeout); timers.current = [] }, [])
  const stopAll = useCallback(async () => {
    clearT(); setRunning(null)
    try { await apiPost('/simulator/control', { enabled:false, rate:1 }); await apiDel('/chaos') } catch (_) {}
  }, [clearT])
  const run = useCallback(async (s) => {
    await stopAll()
    setRunning({ id:s.id, msg:'Starting...', step:0, total:s.steps.length })
    s.steps.forEach((step, idx) => {
      const tid = setTimeout(async () => {
        setRunning(prev => prev ? { ...prev, msg:step.msg, step:idx+1 } : null)
        try {
          if (step.sim)        await apiPost('/simulator/control', step.sim)
          if (step.chaos)      await apiPost('/chaos', step.chaos)
          if (step.chaosReset) await apiDel('/chaos')
        } catch (_) {}
        if (idx === s.steps.length-1) setTimeout(() => setRunning(null), 2500)
      }, step.t)
      timers.current.push(tid)
    })
  }, [stopAll])
  useEffect(() => () => clearT(), [clearT])

  return (
    <Box title="Demo Scenarios — showcase Watermill capabilities">
      <div style={{ display:'flex', gap:12, flexWrap:'wrap' }}>
        {SCENARIOS.map(s => {
          const isOn = running != null && running.id === s.id
          return (
            <div key={s.id} style={{
              flex:'1 1 200px', minWidth:200, background:P.card,
              borderLeft:'4px solid '+(isOn ? s.col : s.col+'50'),
              border:'1px solid '+(isOn ? s.col+'60' : P.border),
              borderRadius:'0 8px 8px 0',
              padding:'14px 16px',
              boxShadow: isOn ? '0 0 22px '+s.col+'22' : 'none',
              transition:'all 0.25s',
            }}>
              <div style={{ display:'flex', alignItems:'flex-start', gap:8, marginBottom:8 }}>
                <div style={{ flex:1, minWidth:0 }}>
                  <Tag col={s.col}>{s.tag}</Tag>
                  <div style={{ fontSize:14, fontWeight:700, color:P.th, marginTop:5 }}>{s.label}</div>
                </div>
                <Btn sm col={s.col} active={isOn} onClick={() => isOn ? stopAll() : run(s)}>
                  {isOn ? 'Stop' : 'Run'}
                </Btn>
              </div>
              <div style={{ fontSize:11, color:P.tm, lineHeight:1.55 }}>{s.desc}</div>
              {isOn && (
                <div style={{ marginTop:10, paddingTop:10, borderTop:'1px solid '+P.border, display:'flex', alignItems:'center', gap:8 }}>
                  <span style={{ width:7, height:7, borderRadius:'50%', background:s.col, flexShrink:0, display:'inline-block', animation:'wmPulse 1.2s ease-in-out infinite' }} />
                  <span style={{ fontSize:11, color:s.col, flex:1 }}>{running.msg}</span>
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

// ── Traffic generator ─────────────────────────────────────────────────────────
function SimPanel() {
  const [status, setSt] = useState({ enabled:false, rate:1 })
  const [rate, setRate] = useState(1)
  const [busy, setBusy] = useState(false)
  const [msg,  setMsg]  = useState(null)
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
      <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:16 }}>
        <div style={{ display:'flex', alignItems:'center', gap:7 }}>
          <div style={{ width:9, height:9, borderRadius:'50%', background: status.enabled ? '#34d399' : P.tm, boxShadow: status.enabled ? '0 0 8px #34d39988' : 'none', transition:'all 0.3s' }} />
          <span style={{ fontSize:12, color:P.tb }}>{status.enabled ? status.rate.toFixed(1)+' evt/s' : 'Idle'}</span>
        </div>
        {msg && <span style={{ fontSize:11, color:'#fbbf24', marginLeft:6 }}>{msg}</span>}
      </div>
      <div style={{ marginBottom:16 }}>
        <Range label="Rate (evt/s)" value={rate} min={0.1} max={20} step={0.1} onChange={setRate} fmt={v => v.toFixed(1)+'/s'} />
      </div>
      <div style={{ display:'flex', gap:6, flexWrap:'wrap' }}>
        <Btn col="#34d399" onClick={() => apply(true,  rate)} disabled={busy}>Start</Btn>
        <Btn col={P.tm}    onClick={() => apply(false, rate)} disabled={busy}>Stop</Btn>
        <Btn sm col="#fbbf24" onClick={() => apply(true, 5)}  disabled={busy}>5/s</Btn>
        <Btn sm col="#fb923c" onClick={() => apply(true, 10)} disabled={busy}>10/s</Btn>
        <Btn sm col="#f87171" onClick={() => apply(true, 20)} disabled={busy}>20/s</Btn>
      </div>
    </Box>
  )
}

// ── Chaos engineering ─────────────────────────────────────────────────────────
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
      <div style={{ display:'flex', alignItems:'center', marginBottom:12 }}>
        <span style={{ fontSize:11, color: anyOn ? '#f87171' : P.tm }}>
          {anyOn ? 'Fault injection active' : 'All services healthy'}
        </span>
        {msg && <span style={{ fontSize:11, color:'#fbbf24', marginLeft:12 }}>{msg}</span>}
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
              display:'flex', alignItems:'center', gap:8,
              background: on ? '#f8717110' : P.card,
              border:'1px solid '+(on ? '#f8717140' : P.border),
              borderRadius:6, padding:'7px 12px', transition:'all 0.2s',
            }}>
              <div style={{ width:7, height:7, borderRadius:'50%', flexShrink:0, background: on ? '#f87171' : P.td, boxShadow: on ? '0 0 8px #f8717188' : 'none' }} />
              <span style={{ fontSize:11, color: on ? '#f87171' : P.tm, minWidth:148, fontFamily:'monospace', flexShrink:0 }}>{svc}</span>
              {on && <Tag col="#f87171">{s.mode} {Math.round(s.error_rate*100)}%</Tag>}
              <div style={{ marginLeft:'auto', display:'flex', gap:4 }}>
                <Btn sm col="#f87171" onClick={() => inject(svc,'poison',1.0)} disabled={busy}>Poison</Btn>
                <Btn sm col="#fbbf24" onClick={() => inject(svc,'drop',0.5)}   disabled={busy}>Drop 50%</Btn>
                <Btn sm col={P.tm}    onClick={() => inject(svc,'',0)}          disabled={busy}>Off</Btn>
              </div>
            </div>
          )
        })}
      </div>
    </Box>
  )
}

// ── Event injector ────────────────────────────────────────────────────────────
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
  const Row = ({ a, b }) => (
    <div style={{ display:'flex', gap:8, marginBottom:8 }}>
      <div style={{ flex:1, minWidth:0 }}>{a}</div>
      <div style={{ flex:1, minWidth:0 }}>{b}</div>
    </div>
  )
  return (
    <Box title="Manual Event Injection">
      <div style={{ display:'flex', gap:4, marginBottom:16, flexWrap:'wrap' }}>
        {TABS.map(tb => (
          <button key={tb.id} type="button" onClick={() => setTab(tb.id)} style={{
            background: tab===tb.id ? tb.col+'20' : 'transparent',
            border:'1px solid '+(tab===tb.id ? tb.col+'60' : P.border),
            color: tab===tb.id ? tb.col : P.tm,
            borderRadius:5, padding:'6px 14px', fontSize:11, fontWeight:600, cursor:'pointer',
          }}>{tb.label}</button>
        ))}
      </div>

      {tab==='admission' && (
        <div>
          <Row a={<TIn value={ap.pid}   onChange={v => setAp(p=>({...p,pid:v}))}   placeholder="Patient ID *" />}
               b={<TIn value={ap.dob}   onChange={v => setAp(p=>({...p,dob:v}))}   placeholder="Date of birth" />} />
          <Row a={<TIn value={ap.first} onChange={v => setAp(p=>({...p,first:v}))} placeholder="First name *" />}
               b={<TIn value={ap.last}  onChange={v => setAp(p=>({...p,last:v}))}  placeholder="Last name *" />} />
          <Row a={<Sel value={ap.ward}  onChange={v => setAp(p=>({...p,ward:v}))}  options={WARDS} />}
               b={<Btn block col="#38bdf8" onClick={() => post('/admissions', { patient_id:ap.pid, first_name:ap.first, last_name:ap.last, date_of_birth:ap.dob, ward:ap.ward })}>Send Admission</Btn>} />
        </div>
      )}
      {tab==='discharge' && (
        <div>
          <Row a={<TIn value={dp.pid}   onChange={v => setDp(p=>({...p,pid:v}))}    placeholder="Patient ID *" />}
               b={<Sel value={dp.ward}  onChange={v => setDp(p=>({...p,ward:v}))}   options={WARDS} />} />
          <Row a={<TIn value={dp.first} onChange={v => setDp(p=>({...p,first:v}))}  placeholder="First name" />}
               b={<TIn value={dp.last}  onChange={v => setDp(p=>({...p,last:v}))}   placeholder="Last name" />} />
          <Row a={<Sel value={dp.reason} onChange={v => setDp(p=>({...p,reason:v}))} options={['recovered','transferred','deceased','self-discharge']} />}
               b={<Btn block col="#86efac" onClick={() => post('/discharges', { patient_id:dp.pid, first_name:dp.first, last_name:dp.last, ward:dp.ward, reason:dp.reason })}>Send Discharge</Btn>} />
        </div>
      )}
      {tab==='transfer' && (
        <div>
          <Row a={<TIn value={tp.pid}    onChange={v => setTp(p=>({...p,pid:v}))}    placeholder="Patient ID *" />}
               b={<TIn value={tp.reason} onChange={v => setTp(p=>({...p,reason:v}))} placeholder="Reason" />} />
          <Row a={<TIn value={tp.first}  onChange={v => setTp(p=>({...p,first:v}))}  placeholder="First name" />}
               b={<TIn value={tp.last}   onChange={v => setTp(p=>({...p,last:v}))}   placeholder="Last name" />} />
          <Row a={<Sel value={tp.from}   onChange={v => setTp(p=>({...p,from:v}))}   options={WARDS} />}
               b={<Sel value={tp.to}     onChange={v => setTp(p=>({...p,to:v}))}     options={WARDS} />} />
          <Btn block col="#fde68a" onClick={() => post('/transfers', { patient_id:tp.pid, first_name:tp.first, last_name:tp.last, from_ward:tp.from, to_ward:tp.to, reason:tp.reason })}>Send Transfer</Btn>
        </div>
      )}
      {tab==='lab' && (
        <div>
          <Row a={<TIn value={lp.pid} onChange={v => setLp(p=>({...p,pid:v}))} placeholder="Patient ID *" />}
               b={<Sel value={lp.test} onChange={v => setLp(p=>({...p,test:v,value:(LAB_TESTS.find(x=>x.name===v)||LAB_TESTS[3]).lo}))} options={LAB_TESTS.map(x=>x.name)} />} />
          <div style={{ marginBottom:10 }}>
            <Range value={lp.value} min={0} max={lt.hi*2} step={0.1} onChange={v => setLp(p=>({...p,value:v}))} fmt={v => v.toFixed(1)+' '+lt.unit} />
          </div>
          <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:10 }}>
            <span style={{ fontSize:11, color:P.tm }}>Ref: {lt.lo}–{lt.hi} {lt.unit}</span>
            {(lp.value < lt.lo || lp.value > lt.hi) && <Tag col="#f87171">ABNORMAL</Tag>}
          </div>
          <Btn block col="#34d399" onClick={() => post('/lab-results', { patient_id:lp.pid, test_name:lp.test, value:lp.value, unit:lt.unit, reference_lo:lt.lo, reference_hi:lt.hi })}>Send Lab Result</Btn>
        </div>
      )}
      {tab==='alert' && (
        <div>
          <Row a={<TIn value={alp.pid} onChange={v => setAlp(p=>({...p,pid:v}))} placeholder="Patient ID *" />}
               b={<Sel value={alp.sev} onChange={v => setAlp(p=>({...p,sev:v}))} options={['low','medium','high','critical']} />} />
          <Row a={<Sel value={alp.cat} onChange={v => setAlp(p=>({...p,cat:v}))} options={['vital','lab','medication','system']} />}
               b={<TIn value={alp.msg} onChange={v => setAlp(p=>({...p,msg:v}))} placeholder="Message *" />} />
          <Row a={<NIn value={alp.val} onChange={v => setAlp(p=>({...p,val:v}))} placeholder="Current value" />}
               b={<NIn value={alp.thr} onChange={v => setAlp(p=>({...p,thr:v}))} placeholder="Threshold" />} />
          <Btn block col="#f87171" onClick={() => post('/alerts', { patient_id:alp.pid, severity:alp.sev, category:alp.cat, message:alp.msg, value:Number(alp.val), threshold:Number(alp.thr) })}>Send Alert</Btn>
        </div>
      )}
    </Box>
  )
}

// ── App ───────────────────────────────────────────────────────────────────────
export default function App() {
  const { events, connected, tickRef } = useSSE('/events/stream')
  const [tph,      setTph]      = useState([])
  const [selected, setSelected] = useState(null)
  const [tf,       setTf]       = useState('all')
  const [qf,       setQf]       = useState('')
  const [injMsg,   setInjMsg]   = useState(null)
  const [chaos,    setChaos]    = useState({})

  // throughput tick
  useEffect(() => {
    const id = setInterval(() => {
      const snap = Object.assign({}, tickRef.current)
      tickRef.current = {}
      const tot = Object.values(snap).reduce((s,v) => s+v, 0)
      setTph(prev => prev.concat([{ s:new Date().toLocaleTimeString(), tot, ...snap }]).slice(-60))
    }, 1000)
    return () => clearInterval(id)
  }, [tickRef])

  // chaos poll
  useEffect(() => {
    const poll = async () => { try { setChaos(await (await fetch(GW+'/chaos/status')).json()) } catch (_) {} }
    poll(); const id = setInterval(poll, 4000); return () => clearInterval(id)
  }, [])

  // dismiss inj feedback
  useEffect(() => {
    if (!injMsg) return; const id = setTimeout(() => setInjMsg(null), 3000); return () => clearTimeout(id)
  }, [injMsg])

  const cnt   = events.reduce((a,e) => { a[e.type]=(a[e.type]||0)+1; return a }, {})
  const svcC  = events.reduce((a,e) => { a[e.source]=(a[e.source]||0)+1; return a }, {})
  const dlqEv = events.filter(e => e.type && e.type.endsWith('-dlq'))
  const altEv = events.filter(e => e.type === 'alert-created')
  const crit  = altEv.filter(e => { try { return JSON.parse(e.payload).severity==='critical' } catch (_) { return false } })
  const cmd   = cnt['command-patient-admit'] || 0
  const down  = (cnt['patient-admitted']||0)+(cnt['lab-result-created']||0)+(cnt['fhir-document-created']||0)+(cnt['notification-sent']||0)
  const fanout  = cmd > 0 ? (down/cmd).toFixed(1) : '-'
  const curTph  = tph.length > 0 ? (tph[tph.length-1].tot || 0) : 0
  const retryEst = dlqEv.length * 5
  const anyOn   = CHAOS_SVCS.some(s => chaos[s] && chaos[s].mode && chaos[s].mode !== '')

  const typeOpts = [
    { value:'all', label:'All types' },
    ...Object.keys(TC).map(k => ({ value:k, label:k })),
    { value:'_dlq', label:'*.dlq  (DLQ)' },
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

  const barD = Object.entries(cnt).map(([type, count]) => ({
    name: type.split('-').slice(0,2).join('-'), count, fill: TC[type]||'#60a5fa',
  }))

  return (
    <div style={{
      minHeight:'100vh', fontFamily:'system-ui,-apple-system,sans-serif',
      background:P.page,
      backgroundImage:'radial-gradient(circle, rgba(26,58,106,0.14) 1px, transparent 1px)',
      backgroundSize:'28px 28px',
    }}>
      <style>{'@keyframes wmPulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.3;transform:scale(0.55)}} @keyframes pipeflow{0%,100%{opacity:0.5}50%{opacity:1}} *{box-sizing:border-box} input::placeholder{color:#3a6080} select option{background:#0a1c32} input[type=range]{height:4px}'}</style>

      {/* ── Header bar ── */}
      <div style={{ background:P.panel, borderBottom:'1px solid '+P.border, padding:'13px 28px', display:'flex', alignItems:'center', gap:16, flexWrap:'wrap', position:'sticky', top:0, zIndex:100 }}>
        <div>
          <span style={{ fontSize:17, fontWeight:800, color:P.th, letterSpacing:-0.3 }}>Health ESB</span>
          <span style={{ fontSize:10, color:P.tm, marginLeft:10 }}>event-driven architecture demo</span>
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:5 }}>
          <span style={{ fontSize:10, color:P.td }}>powered by</span>
          <span style={{ fontSize:11, fontWeight:800, color:'#60a5fa', letterSpacing:1 }}>WATERMILL</span>
        </div>
        {injMsg && (
          <div style={{ fontSize:11, color:'#34d399', background:'#34d39918', border:'1px solid #34d39928', borderRadius:4, padding:'4px 12px' }}>{injMsg}</div>
        )}
        <div style={{ marginLeft:'auto', display:'flex', gap:10, alignItems:'center', flexWrap:'wrap' }}>
          {anyOn      && <Tag col="#f87171">CHAOS ACTIVE</Tag>}
          {crit.length > 0 && <Tag col="#ef4444">{crit.length} critical alert{crit.length>1?'s':''}</Tag>}
          <div style={{ display:'flex', alignItems:'center', gap:6 }}>
            <div style={{ width:8, height:8, borderRadius:'50%', background: connected ? '#34d399' : '#f87171', boxShadow: connected ? '0 0 8px #34d39988' : 'none' }} />
            <span style={{ fontSize:11, color: connected ? '#34d399' : '#f87171', fontWeight:600 }}>
              {connected ? 'LIVE' : 'DISCONNECTED'}
            </span>
          </div>
        </div>
      </div>

      <div style={{ padding:'20px 28px', maxWidth:1520, margin:'0 auto' }}>

        {/* ── KPI strip ── */}
        <div style={{ display:'flex', gap:8, marginBottom:20, flexWrap:'wrap' }}>
          <Kpi label="Total Events"  value={events.length}                          col="#60a5fa" />
          <Kpi label="Admitted"      value={cnt['patient-admitted']      ||0}       col="#38bdf8" />
          <Kpi label="Discharged"    value={cnt['patient-discharged']    ||0}       col="#86efac" />
          <Kpi label="Transferred"   value={cnt['patient-transferred']   ||0}       col="#fde68a" />
          <Kpi label="Lab Results"   value={cnt['lab-result-created']    ||0}       col="#34d399" />
          <Kpi label="FHIR Docs"     value={cnt['fhir-document-created'] ||0}       col="#a78bfa" />
          <Kpi label="Notifications" value={cnt['notification-sent']     ||0}       col="#fb923c" />
          <Kpi label="Alerts"        value={cnt['alert-created']         ||0}       col="#f87171" note={crit.length>0 ? crit.length+' critical' : undefined} />
          <Kpi label="DLQ"           value={dlqEv.length}                           col="#f87171" note={dlqEv.length>0 ? '~'+retryEst+' retries' : undefined} />
          <Kpi label="Fan-out x"     value={fanout}                                 col="#a78bfa" note="downstream / command" />
          <Kpi label="Evt / sec"     value={curTph}                                 col="#fbbf24" />
        </div>

        {/* ── Pipeline ── */}
        <Box title="Message Pipeline — Watermill fan-out router" style={{ marginBottom:20 }}>
          <Pipeline svcCounts={svcC} cmdCount={cmd} chaos={chaos} tph={curTph} />
        </Box>

        {/* ── Charts row ── */}
        <div style={{ display:'flex', gap:16, marginBottom:20, flexWrap:'wrap' }}>
          <Box title="Throughput — events per second" style={{ flex:'3 1 380px', minWidth:280 }}>
            {tph.length < 2 ? (
              <div style={{ height:175, display:'flex', alignItems:'center', justifyContent:'center', color:P.tm, fontSize:12 }}>
                Waiting for events...
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={175}>
                <AreaChart data={tph} margin={{ top:4, right:4, bottom:0, left:-14 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                  <XAxis dataKey="s" tick={{ fill:P.tm, fontSize:9 }} interval="preserveStartEnd" />
                  <YAxis allowDecimals={false} tick={{ fill:P.tm, fontSize:10 }} />
                  <Tooltip contentStyle={{ background:P.panel, border:'1px solid '+P.border, color:P.th, fontSize:11 }} labelStyle={{ color:P.tb }} />
                  {Object.entries(TC).map(([k,v]) => (
                    <Area key={k} type="monotone" dataKey={k} stroke={v} fill={v+'18'} stackId="s" isAnimationActive={false} />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            )}
          </Box>

          <Box title="Events by Type" style={{ flex:'1 1 220px', minWidth:200 }}>
            {barD.length === 0 ? (
              <div style={{ height:175, display:'flex', alignItems:'center', justifyContent:'center', color:P.tm, fontSize:12 }}>No data yet</div>
            ) : (
              <ResponsiveContainer width="100%" height={175}>
                <BarChart data={barD} margin={{ top:4, right:4, bottom:22, left:-22 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                  <XAxis dataKey="name" tick={{ fill:P.tm, fontSize:8 }} angle={-35} textAnchor="end" />
                  <YAxis tick={{ fill:P.tm, fontSize:10 }} />
                  <Tooltip contentStyle={{ background:P.panel, border:'1px solid '+P.border, color:P.th, fontSize:11 }} />
                  <Bar dataKey="count" radius={[3,3,0,0]} isAnimationActive={false}>
                    {barD.map((e,i) => <Cell key={i} fill={e.fill} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </Box>

          <Box title="DLQ and Alerts" style={{ flex:'1 1 180px', minWidth:180 }}>
            {dlqEv.length===0 && altEv.length===0 ? (
              <div style={{ height:175, display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', gap:6 }}>
                <div style={{ fontSize:28, color:'#34d399', fontWeight:800 }}>OK</div>
                <div style={{ fontSize:11, color:P.tm }}>No failures</div>
              </div>
            ) : (
              <div style={{ overflowY:'auto', maxHeight:175 }}>
                {dlqEv.slice(0,20).map((e,i) => (
                  <div key={'d'+i} onClick={() => setSelected(prev => prev===e?null:e)}
                    style={{ background:'#f8717112', borderRadius:4, padding:'5px 8px', marginBottom:3, fontSize:11, cursor:'pointer', display:'flex', gap:6, alignItems:'center' }}>
                    <Tag col="#f87171">DLQ</Tag>
                    <span style={{ color:P.tm }}>{e.type?(e.type.replace('-dlq','')):''}</span>
                  </div>
                ))}
                {altEv.slice(0,12).map((e,i) => {
                  let sev='low'; try { sev=JSON.parse(e.payload).severity } catch (_) {}
                  return (
                    <div key={'a'+i} onClick={() => setSelected(prev => prev===e?null:e)}
                      style={{ background:(SEV[sev]||'#34d399')+'12', borderRadius:4, padding:'5px 8px', marginBottom:3, fontSize:11, cursor:'pointer', display:'flex', gap:6, alignItems:'center' }}>
                      <Tag col={SEV[sev]||'#34d399'}>{sev}</Tag>
                      <span style={{ color:P.tm, fontSize:10 }}>alert</span>
                    </div>
                  )
                })}
              </div>
            )}
          </Box>
        </div>

        {/* ── Demo scenarios ── */}
        <div style={{ marginBottom:20 }}><ScenarioPanel /></div>

        {/* ── Simulator + Chaos ── */}
        <div style={{ display:'flex', gap:16, marginBottom:20, flexWrap:'wrap' }}>
          <div style={{ flex:'1 1 240px', minWidth:240 }}><SimPanel /></div>
          <div style={{ flex:'2 1 360px', minWidth:320 }}><ChaosPanel /></div>
        </div>

        {/* ── Event injector ── */}
        <div style={{ marginBottom:20 }}><Injector onResult={setInjMsg} /></div>

        {/* ── Inspector (conditional) ── */}
        {selected && <Inspector event={selected} onClose={() => setSelected(null)} />}

        {/* ── Live stream ── */}
        <Box>
          <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:14, flexWrap:'wrap' }}>
            <span style={{ fontSize:9, fontWeight:700, color:P.tm, textTransform:'uppercase', letterSpacing:1.5 }}>Live Event Stream</span>
            <span style={{ fontSize:10, color:P.td }}>click a row to inspect — Trace filters by correlation-id</span>
            <div style={{ marginLeft:'auto', display:'flex', gap:8, flexWrap:'wrap' }}>
              <Sel value={tf} onChange={setTf} options={typeOpts} style={{ width:220 }} />
              <TIn value={qf} onChange={setQf} placeholder="Search corr-id / source / type" style={{ width:230 }} />
              {qf && <Btn sm col={P.tm} onClick={() => setQf('')}>clear</Btn>}
            </div>
          </div>
          <div style={{ overflowY:'auto', maxHeight:460, fontFamily:'monospace' }}>
            {filtered.map((e, i) => (
              <div key={i} onClick={() => setSelected(prev => prev===e?null:e)}
                style={{
                  display:'flex', alignItems:'center', gap:8,
                  padding:'6px 8px', borderBottom:'1px solid '+P.sep,
                  fontSize:11, cursor:'pointer', borderRadius:4,
                  background: selected===e ? P.card : 'transparent',
                  transition:'background 0.1s',
                }}>
                <span style={{ color:P.td, minWidth:66, fontSize:9, flexShrink:0 }}>{new Date(e.timestamp).toLocaleTimeString()}</span>
                <Tag col={TC[e.type]||P.tm}>{e.type}</Tag>
                <span style={{ color:P.tm, minWidth:95, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{e.source}</span>
                <span style={{ color:P.td, fontSize:9, flexShrink:0 }}>{e.correlation_id?(e.correlation_id.slice(0,8)):''}</span>
                <div style={{ marginLeft:'auto', display:'flex', gap:4, flexShrink:0 }}>
                  {e.type==='alert-created'&&(()=>{try{const p=JSON.parse(e.payload);return<Tag col={SEV[p.severity]||P.tm}>{p.severity}</Tag>}catch(_){return null}})()}
                  {e.type==='lab-result-created'&&(()=>{try{const p=JSON.parse(e.payload);return p.abnormal?<Tag col="#f87171">abnormal</Tag>:null}catch(_){return null}})()}
                  {e.type&&e.type.endsWith('-dlq')&&<Tag col="#f87171">DLQ</Tag>}
                  <Btn sm col={P.border} onClick={ev => { ev.stopPropagation(); setQf(e.correlation_id||''); setTf('all') }}>Trace</Btn>
                </div>
              </div>
            ))}
            {filtered.length===0 && (
              <div style={{ color:P.tm, textAlign:'center', padding:40, fontSize:12 }}>
                {events.length===0 ? 'Waiting for events...' : 'No events match the current filter'}
              </div>
            )}
          </div>
          <div style={{ marginTop:8, fontSize:9, color:P.td, textAlign:'right' }}>{filtered.length} / {events.length} shown</div>
        </Box>

      </div>
    </div>
  )
}
