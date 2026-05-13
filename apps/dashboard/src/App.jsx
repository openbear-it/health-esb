import { useState, useEffect, useRef, useCallback } from 'react'
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, Cell,
} from 'recharts'

const GW = ''

const COLORS = {
  'command-patient-admit':  '#7dd3fc',
  'patient-admitted':       '#38bdf8',
  'patient-discharged':     '#86efac',
  'patient-transferred':    '#fde68a',
  'lab-result-created':     '#34d399',
  'fhir-document-created':  '#a78bfa',
  'notification-sent':      '#fb923c',
  'alert-created':          '#f87171',
}

const SEV = { low: '#34d399', medium: '#fbbf24', high: '#fb923c', critical: '#f87171' }

const SERVICES = [
  { id: 'gateway',              label: 'Gateway', color: '#60a5fa' },
  { id: 'adt-service',          label: 'ADT',     color: '#38bdf8' },
  { id: 'lab-service',          label: 'Lab',     color: '#34d399' },
  { id: 'fhir-bridge',          label: 'FHIR',    color: '#a78bfa' },
  { id: 'notification-service', label: 'Notify',  color: '#fb923c' },
  { id: 'audit-service',        label: 'Audit',   color: '#f9a8d4' },
]

const CHAOS_SVCS = ['adt-service', 'lab-service', 'fhir-bridge', 'notification-service', 'audit-service']
const WARDS = ['ICU', 'Cardiology', 'Oncology', 'Pediatrics', 'Emergency', 'Surgery', 'Neurology']
const LAB_TESTS = [
  { name: 'Hemoglobin',        lo: 12.0, hi: 17.5, unit: 'g/dL'    },
  { name: 'White Blood Cells', lo: 4.5,  hi: 11.0, unit: '10^3/uL' },
  { name: 'Platelets',         lo: 150,  hi: 400,  unit: '10^3/uL' },
  { name: 'Glucose',           lo: 70,   hi: 100,  unit: 'mg/dL'   },
  { name: 'Creatinine',        lo: 0.6,  hi: 1.2,  unit: 'mg/dL'   },
]

// Demo Scenarios — each showcases a Watermill capability
const SCENARIOS = [
  {
    id: 'normal', label: 'Normal Flow', color: '#34d399',
    desc: 'Steady 2 evt/s — watch fan-out: 1 command generates many downstream events',
    steps: [
      { t: 0,     msg: 'Simulator at 2 evt/s...', sim: { enabled: true,  rate: 2 } },
      { t: 30000, msg: 'Done.',                   sim: { enabled: false, rate: 2 } },
    ],
  },
  {
    id: 'spike', label: 'Throughput Spike', color: '#fbbf24',
    desc: 'Ramp 1 to 20 evt/s — Watermill scales with zero code changes',
    steps: [
      { t: 0,     msg: '1 evt/s baseline...',                   sim: { enabled: true, rate: 1  } },
      { t: 4000,  msg: 'Ramping to 5 evt/s...',                 sim: { enabled: true, rate: 5  } },
      { t: 8000,  msg: 'Ramping to 10 evt/s...',                sim: { enabled: true, rate: 10 } },
      { t: 12000, msg: '20 evt/s — watch the throughput chart!',sim: { enabled: true, rate: 20 } },
      { t: 17000, msg: 'Ramping back down...',                  sim: { enabled: true, rate: 5  } },
      { t: 21000, msg: 'Done.',                                 sim: { enabled: false, rate: 1 } },
    ],
  },
  {
    id: 'dlq', label: 'Retry + DLQ Demo', color: '#f87171',
    desc: 'Poison lab-service → Watermill retries 5x then routes to Dead Letter Queue',
    steps: [
      { t: 0,     msg: '3 evt/s, system healthy...',              sim: { enabled: true, rate: 3 } },
      { t: 4000,  msg: 'Poisoning lab-service — watch DLQ grow!', chaos: { service: 'lab-service', mode: 'poison', error_rate: 1.0 } },
      { t: 18000, msg: 'Resetting chaos — Watermill auto-recovers...', chaosReset: true },
      { t: 22000, msg: 'System recovered. Done.',                 sim: { enabled: false, rate: 1 } },
    ],
  },
  {
    id: 'cascade', label: 'Cascade Failure', color: '#fb923c',
    desc: 'Poison multiple services — shows fault isolation and automatic recovery',
    steps: [
      { t: 0,     msg: '4 evt/s, all services healthy...',          sim: { enabled: true, rate: 4 } },
      { t: 4000,  msg: 'Poisoning lab-service...',                   chaos: { service: 'lab-service',          mode: 'poison', error_rate: 1.0 } },
      { t: 7000,  msg: 'Also poisoning fhir-bridge...',              chaos: { service: 'fhir-bridge',          mode: 'poison', error_rate: 1.0 } },
      { t: 10000, msg: 'Dropping 50% in notification-service...',    chaos: { service: 'notification-service', mode: 'drop',   error_rate: 0.5 } },
      { t: 16000, msg: 'Resetting ALL faults — watch recovery!',     chaosReset: true },
      { t: 21000, msg: 'Done.',                                       sim: { enabled: false, rate: 1 } },
    ],
  },
]

async function apiPost(path, body) {
  const r = await fetch(GW + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  return r.json()
}
async function apiDelete(path) { await fetch(GW + path, { method: 'DELETE' }) }

function useSSE(url) {
  const [events, setEvents] = useState([])
  const [connected, setConnected] = useState(false)
  const tickRef = useRef({})
  useEffect(() => {
    const es = new EventSource(url)
    es.onopen  = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.addEventListener('event', e => {
      try {
        const evt = JSON.parse(e.data)
        tickRef.current[evt.type] = (tickRef.current[evt.type] || 0) + 1
        setEvents(prev => [evt, ...prev].slice(0, 500))
      } catch (_) {}
    })
    return () => es.close()
  }, [url])
  return { events, connected, tickRef }
}

const C = {
  bg: '#0f172a', panel: '#1e293b', border: '#334155',
  muted: '#475569', subtle: '#64748b', dim: '#94a3b8', text: '#e2e8f0', bright: '#f1f5f9',
}

function Pill({ color, children }) {
  const col = color || C.subtle
  return <span style={{ background: col+'28', color: col, border: '1px solid '+col+'55', borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600, letterSpacing: 0.3, whiteSpace: 'nowrap', display: 'inline-block' }}>{children}</span>
}

function Card({ label, value, color, note }) {
  return (
    <div style={{ background: C.panel, border: '1px solid '+color+'44', borderRadius: 8, padding: '12px 16px', flex: '1 1 90px', minWidth: 90 }}>
      <div style={{ fontSize: 10, color: C.subtle, marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.5 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, color, lineHeight: 1 }}>{value}</div>
      {note && <div style={{ fontSize: 10, color: C.muted, marginTop: 3 }}>{note}</div>}
    </div>
  )
}

function Section({ title, children, style }) {
  return (
    <div style={{ background: C.panel, borderRadius: 10, padding: '16px 20px', ...style }}>
      {title && <div style={{ fontSize: 10, color: C.subtle, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1.2, marginBottom: 14 }}>{title}</div>}
      {children}
    </div>
  )
}

function Btn({ children, onClick, color, active, small, disabled, block }) {
  const col = color || '#60a5fa'
  return (
    <button type="button" onClick={disabled ? undefined : onClick}
      style={{
        display: block ? 'block' : 'inline-flex', width: block ? '100%' : undefined,
        alignItems: 'center', justifyContent: 'center',
        background: active ? col+'33' : col+'18',
        border: '1px solid '+(active ? col+'88' : col+'44'),
        color: active ? col : col+'cc',
        borderRadius: 6, padding: small ? '4px 10px' : '7px 16px',
        fontSize: small ? 11 : 12, fontWeight: 600,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.45 : 1,
        whiteSpace: 'nowrap', lineHeight: 1.4,
        transition: 'background 0.15s', boxSizing: 'border-box', userSelect: 'none',
      }}>
      {children}
    </button>
  )
}

function TxtIn({ value, onChange, placeholder, style }) {
  return <input type="text" value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder}
    style={{ background: C.bg, border: '1px solid '+C.border, color: C.text, borderRadius: 6, padding: '6px 10px', fontSize: 12, width: '100%', boxSizing: 'border-box', outline: 'none', fontFamily: 'inherit', ...style }} />
}

function NumIn({ value, onChange, placeholder }) {
  return <input type="number" value={value} onChange={e => onChange(e.target.value === '' ? 0 : Number(e.target.value))} placeholder={placeholder}
    style={{ background: C.bg, border: '1px solid '+C.border, color: C.text, borderRadius: 6, padding: '6px 10px', fontSize: 12, width: '100%', boxSizing: 'border-box', outline: 'none', fontFamily: 'inherit' }} />
}

function Combo({ value, onChange, options, style }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ background: C.bg, border: '1px solid '+C.border, color: C.text, borderRadius: 6, padding: '6px 10px', fontSize: 12, width: '100%', boxSizing: 'border-box', outline: 'none', fontFamily: 'inherit', ...style }}>
      {options.map(o => <option key={o.value != null ? o.value : o} value={o.value != null ? o.value : o}>{o.label != null ? o.label : o}</option>)}
    </select>
  )
}

function Slider({ label, value, min, max, step, onChange, fmt }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
      {label && <span style={{ fontSize: 11, color: C.dim, minWidth: 90, flexShrink: 0 }}>{label}</span>}
      <input type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(Number(e.target.value))}
        style={{ flex: 1, accentColor: '#60a5fa', minWidth: 0, cursor: 'pointer' }} />
      <span style={{ fontSize: 12, color: C.text, minWidth: 64, textAlign: 'right', flexShrink: 0 }}>
        {fmt ? fmt(value) : value}
      </span>
    </div>
  )
}

function Inspector({ event, onClose }) {
  if (!event) return null
  let payload = null
  try { payload = JSON.parse(event.payload != null ? event.payload : 'null') } catch (_) { payload = event.payload }
  const col = COLORS[event.type] || C.dim
  return (
    <div style={{ background: C.panel, borderRadius: 10, padding: 16, marginBottom: 16, border: '1px solid '+col+'55' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 10, color: C.subtle, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1.2 }}>Message Inspector</span>
        <Pill color={col}>{event.type}</Pill>
        <span style={{ fontSize: 11, color: C.muted }}>{event.source}</span>
        <button type="button" onClick={onClose} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: C.subtle, cursor: 'pointer', fontSize: 18, padding: '0 4px', lineHeight: 1 }}>x</button>
      </div>
      <div style={{ display: 'flex', gap: 16, fontSize: 11, marginBottom: 12, flexWrap: 'wrap', color: C.dim, fontFamily: 'monospace' }}>
        <span><span style={{ color: C.muted }}>id </span>{(event.id||'').slice(0,16)}...</span>
        <span><span style={{ color: C.muted }}>corr </span>{event.correlation_id}</span>
        <span><span style={{ color: C.muted }}>ts </span>{new Date(event.timestamp).toISOString()}</span>
      </div>
      <pre style={{ background: C.bg, borderRadius: 6, padding: 12, fontSize: 12, color: C.text, overflowX: 'auto', margin: 0, fontFamily: 'monospace', whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 220, overflowY: 'auto' }}>
        {JSON.stringify(payload, null, 2)}
      </pre>
    </div>
  )
}

function Pipeline({ svcCounts, chaos, tph }) {
  const flowing = tph > 0
  return (
    <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 6 }}>
      {SERVICES.map((svc, i) => {
        const cs = chaos[svc.id]
        const disrupted = cs && cs.mode && cs.mode !== ''
        const count = svcCounts[svc.id] || 0
        const borderCol = disrupted ? '#f87171' : flowing ? svc.color : svc.color+'44'
        return (
          <div key={svc.id} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <div style={{ background: C.bg, border: '2px solid '+borderCol, borderRadius: 8, padding: '8px 14px', textAlign: 'center', minWidth: 76, position: 'relative', transition: 'border-color 0.4s', boxShadow: flowing && !disrupted ? '0 0 10px '+svc.color+'30' : 'none' }}>
              {disrupted && <div style={{ position: 'absolute', top: -10, right: -8, fontSize: 14, color: '#f87171' }}>!</div>}
              <div style={{ fontSize: 10, fontWeight: 700, color: disrupted ? '#f87171' : svc.color, marginBottom: 3 }}>{svc.label}</div>
              <div style={{ fontSize: 18, fontWeight: 800, color: C.bright, lineHeight: 1 }}>{count}</div>
              <div style={{ fontSize: 9, color: C.muted, marginTop: 2 }}>events</div>
            </div>
            {i < SERVICES.length - 1 && (
              <div style={{ color: flowing ? C.border : '#1e293b', fontSize: 18, transition: 'color 0.4s', userSelect: 'none' }}>-&gt;</div>
            )}
          </div>
        )
      })}
    </div>
  )
}

function ScenarioPanel() {
  const [running, setRunning] = useState(null)
  const timersRef = useRef([])

  const clearTimers = useCallback(() => {
    timersRef.current.forEach(id => clearTimeout(id))
    timersRef.current = []
  }, [])

  const stopAll = useCallback(async () => {
    clearTimers()
    setRunning(null)
    try {
      await apiPost('/simulator/control', { enabled: false, rate: 1 })
      await apiDelete('/chaos')
    } catch (_) {}
  }, [clearTimers])

  const runScenario = useCallback(async (scenario) => {
    await stopAll()
    setRunning({ id: scenario.id, msg: 'Starting...', step: 0, total: scenario.steps.length })
    scenario.steps.forEach((step, idx) => {
      const tid = setTimeout(async () => {
        setRunning(prev => prev ? { ...prev, msg: step.msg, step: idx + 1 } : null)
        try {
          if (step.sim)        await apiPost('/simulator/control', step.sim)
          if (step.chaos)      await apiPost('/chaos', step.chaos)
          if (step.chaosReset) await apiDelete('/chaos')
        } catch (_) {}
        if (idx === scenario.steps.length - 1) {
          setTimeout(() => setRunning(null), 2500)
        }
      }, step.t)
      timersRef.current.push(tid)
    })
  }, [stopAll])

  useEffect(() => () => clearTimers(), [clearTimers])

  return (
    <Section title="Demo Scenarios — showcase Watermill capabilities">
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 14 }}>
        {SCENARIOS.map(s => {
          const isActive = running != null && running.id === s.id
          return (
            <div key={s.id} style={{ flex: '1 1 160px', minWidth: 160 }}>
              <Btn color={s.color} active={isActive} block onClick={() => isActive ? stopAll() : runScenario(s)}>
                {isActive ? 'Stop' : '> '+s.label}
              </Btn>
              <div style={{ fontSize: 10, color: C.muted, marginTop: 5, textAlign: 'center', lineHeight: 1.4 }}>{s.desc}</div>
            </div>
          )
        })}
      </div>
      {running != null ? (
        <div style={{ background: C.bg, borderRadius: 7, padding: '10px 14px', border: '1px solid '+((SCENARIOS.find(s => s.id === running.id)||{}).color||'#60a5fa')+'44', display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: '50%', flexShrink: 0, background: (SCENARIOS.find(s => s.id === running.id)||{}).color||'#60a5fa', animation: 'wmPulse 1.2s ease-in-out infinite' }} />
          <span style={{ fontSize: 12, color: C.text, flex: 1 }}>{running.msg}</span>
          <span style={{ fontSize: 10, color: C.subtle, flexShrink: 0 }}>step {running.step}/{running.total}</span>
          <Btn small color={C.subtle} onClick={stopAll}>Stop</Btn>
        </div>
      ) : (
        <div style={{ fontSize: 11, color: C.muted, paddingTop: 4 }}>
          Click a scenario to start. Click again to stop early. Each auto-resets the simulator and chaos state.
        </div>
      )}
    </Section>
  )
}

function SimulatorPanel() {
  const [status, setStatus] = useState({ enabled: false, rate: 1 })
  const [rate, setRate] = useState(1)
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState(null)
  const flash = m => { setMsg(m); setTimeout(() => setMsg(null), 2500) }
  const refresh = useCallback(async () => {
    try { const d = await (await fetch(GW+'/simulator/status')).json(); setStatus(d); setRate(d.rate) } catch (_) {}
  }, [])
  useEffect(() => { refresh(); const id = setInterval(refresh, 3000); return () => clearInterval(id) }, [refresh])
  const apply = async (enabled, r) => {
    setBusy(true)
    try { const d = await apiPost('/simulator/control', { enabled, rate: r }); setStatus(d); flash(enabled ? r.toFixed(1)+' evt/s' : 'Stopped') }
    catch (e) { flash('Error: '+e.message) }
    finally { setBusy(false) }
  }
  return (
    <Section title="Traffic Generator" style={{ height: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <Pill color={status.enabled ? '#34d399' : C.subtle}>{status.enabled ? status.rate.toFixed(1)+' evt/s' : 'idle'}</Pill>
        {msg && <span style={{ fontSize: 11, color: '#fbbf24' }}>{msg}</span>}
      </div>
      <div style={{ marginBottom: 14 }}>
        <Slider label="Rate (evt/s)" value={rate} min={0.1} max={20} step={0.1} onChange={setRate} fmt={v => v.toFixed(1)+'/s'} />
      </div>
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
        <Btn color="#34d399" onClick={() => apply(true, rate)} disabled={busy}>Start</Btn>
        <Btn color={C.subtle} onClick={() => apply(false, rate)} disabled={busy}>Stop</Btn>
        <Btn small color="#fbbf24" onClick={() => apply(true, 5)}  disabled={busy}>x5</Btn>
        <Btn small color="#fb923c" onClick={() => apply(true, 10)} disabled={busy}>x10</Btn>
        <Btn small color="#f87171" onClick={() => apply(true, 20)} disabled={busy}>x20</Btn>
      </div>
    </Section>
  )
}

function ChaosPanel() {
  const [chaos, setChaos] = useState({})
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState(null)
  const flash = m => { setMsg(m); setTimeout(() => setMsg(null), 2500) }
  const refresh = useCallback(async () => {
    try { setChaos(await (await fetch(GW+'/chaos/status')).json()) } catch (_) {}
  }, [])
  useEffect(() => { refresh(); const id = setInterval(refresh, 3000); return () => clearInterval(id) }, [refresh])
  const inject = async (service, mode, error_rate) => {
    setBusy(true)
    try { await apiPost('/chaos', { service, mode, error_rate }); flash(service+': '+(mode||'off')); await refresh() }
    catch (_) {} finally { setBusy(false) }
  }
  const resetAll = async () => {
    setBusy(true)
    try { await apiDelete('/chaos'); flash('All faults cleared'); await refresh() }
    catch (_) {} finally { setBusy(false) }
  }
  return (
    <Section title="Chaos Engineering">
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: 10 }}>
        {msg && <span style={{ fontSize: 11, color: '#fbbf24' }}>{msg}</span>}
        <div style={{ marginLeft: 'auto' }}><Btn small color="#f87171" onClick={resetAll} disabled={busy}>Reset all faults</Btn></div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
        {CHAOS_SVCS.map(svc => {
          const s = chaos[svc] || { mode: '', error_rate: 0 }
          const on = s.mode && s.mode !== ''
          return (
            <div key={svc} style={{ display: 'flex', alignItems: 'center', gap: 8, background: on ? '#f8717110' : C.bg, border: '1px solid '+(on ? '#f8717140' : C.border), borderRadius: 6, padding: '7px 10px', transition: 'background 0.2s' }}>
              <span style={{ fontSize: 11, color: on ? '#f87171' : C.dim, minWidth: 140, fontFamily: 'monospace', flexShrink: 0 }}>
                {on ? '[!] ' : '    '}{svc}
              </span>
              <Pill color={on ? '#f87171' : C.muted}>{s.mode||'ok'}{on ? ' '+Math.round(s.error_rate*100)+'%' : ''}</Pill>
              <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
                <Btn small color="#f87171" onClick={() => inject(svc,'poison',1.0)} disabled={busy}>Poison</Btn>
                <Btn small color="#fbbf24" onClick={() => inject(svc,'drop',0.5)}   disabled={busy}>Drop 50%</Btn>
                <Btn small color={C.subtle} onClick={() => inject(svc,'',0)}        disabled={busy}>Off</Btn>
              </div>
            </div>
          )
        })}
      </div>
    </Section>
  )
}

function EventInjector({ onResult }) {
  const [tab, setTab] = useState('admission')
  const [ap, setAp] = useState({ pid: 'P1001', first: 'Alice', last: 'Smith', dob: '1980-06-15', ward: 'ICU' })
  const [dp, setDp] = useState({ pid: 'P1001', first: 'Alice', last: 'Smith', ward: 'ICU', reason: 'recovered' })
  const [tp, setTp] = useState({ pid: 'P1001', first: 'Alice', last: 'Smith', from: 'Emergency', to: 'ICU', reason: 'stabilized' })
  const [lp, setLp] = useState({ pid: 'P1001', test: 'Glucose', value: 240 })
  const [alp, setAlp] = useState({ pid: 'P1001', sev: 'critical', cat: 'vital', msg: 'SpO2 dropped below 88%', val: 86, thr: 95 })
  const labTest = LAB_TESTS.find(lt => lt.name === lp.test) || LAB_TESTS[3]
  const post = async (path, body) => {
    try {
      const r = await fetch(GW+path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      const d = await r.json()
      onResult(r.ok ? 'sent corr:'+((d.correlation_id||'').slice(0,8)) : 'Error: '+d.error)
    } catch (e) { onResult('Error: '+e.message) }
  }
  const TABS = [
    { id: 'admission', label: 'Admission' }, { id: 'discharge', label: 'Discharge' },
    { id: 'transfer', label: 'Transfer' }, { id: 'lab', label: 'Lab Result' }, { id: 'alert', label: 'Alert' },
  ]
  const Row = ({ ch }) => <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>{ch}</div>
  const F = ({ ch }) => <div style={{ flex: 1, minWidth: 0 }}>{ch}</div>
  return (
    <Section title="Manual Event Injection">
      <div style={{ display: 'flex', gap: 4, marginBottom: 14, flexWrap: 'wrap' }}>
        {TABS.map(tb => (
          <button key={tb.id} type="button" onClick={() => setTab(tb.id)} style={{ background: tab===tb.id ? '#60a5fa22' : 'transparent', border: '1px solid '+(tab===tb.id ? '#60a5fa66' : C.border), color: tab===tb.id ? '#60a5fa' : C.subtle, borderRadius: 5, padding: '5px 14px', fontSize: 11, fontWeight: 600, cursor: 'pointer' }}>{tb.label}</button>
        ))}
      </div>
      {tab === 'admission' && (
        <div>
          <Row ch={[<F key="a" ch={<TxtIn value={ap.pid}   onChange={v => setAp(p => ({...p,pid:v}))   } placeholder="Patient ID *" />} />, <F key="b" ch={<TxtIn value={ap.dob}   onChange={v => setAp(p => ({...p,dob:v}))   } placeholder="Date of birth" />} />]} />
          <Row ch={[<F key="a" ch={<TxtIn value={ap.first} onChange={v => setAp(p => ({...p,first:v}))} placeholder="First name *"  />} />, <F key="b" ch={<TxtIn value={ap.last}  onChange={v => setAp(p => ({...p,last:v}))  } placeholder="Last name *"  />} />]} />
          <div style={{ display: 'flex', gap: 8 }}>
            <div style={{ flex: 1, minWidth: 0 }}><Combo value={ap.ward} onChange={v => setAp(p => ({...p,ward:v}))} options={WARDS} /></div>
            <div style={{ flex: 1, minWidth: 0 }}><Btn block color="#38bdf8" onClick={() => post('/admissions', { patient_id: ap.pid, first_name: ap.first, last_name: ap.last, date_of_birth: ap.dob, ward: ap.ward })}>Send Admission</Btn></div>
          </div>
        </div>
      )}
      {tab === 'discharge' && (
        <div>
          <Row ch={[<F key="a" ch={<TxtIn value={dp.pid}   onChange={v => setDp(p => ({...p,pid:v}))   } placeholder="Patient ID *" />} />, <F key="b" ch={<Combo value={dp.ward}   onChange={v => setDp(p => ({...p,ward:v}))  } options={WARDS} />} />]} />
          <Row ch={[<F key="a" ch={<TxtIn value={dp.first} onChange={v => setDp(p => ({...p,first:v}))} placeholder="First name"   />} />, <F key="b" ch={<TxtIn value={dp.last}   onChange={v => setDp(p => ({...p,last:v}))  } placeholder="Last name"   />} />]} />
          <div style={{ display: 'flex', gap: 8 }}>
            <div style={{ flex: 1, minWidth: 0 }}><Combo value={dp.reason} onChange={v => setDp(p => ({...p,reason:v}))} options={['recovered','transferred','deceased','self-discharge']} /></div>
            <div style={{ flex: 1, minWidth: 0 }}><Btn block color="#86efac" onClick={() => post('/discharges', { patient_id: dp.pid, first_name: dp.first, last_name: dp.last, ward: dp.ward, reason: dp.reason })}>Send Discharge</Btn></div>
          </div>
        </div>
      )}
      {tab === 'transfer' && (
        <div>
          <Row ch={[<F key="a" ch={<TxtIn value={tp.pid}    onChange={v => setTp(p => ({...p,pid:v}))   } placeholder="Patient ID *" />} />, <F key="b" ch={<TxtIn value={tp.reason} onChange={v => setTp(p => ({...p,reason:v}))} placeholder="Reason"       />} />]} />
          <Row ch={[<F key="a" ch={<TxtIn value={tp.first}  onChange={v => setTp(p => ({...p,first:v}))} placeholder="First name"   />} />, <F key="b" ch={<TxtIn value={tp.last}   onChange={v => setTp(p => ({...p,last:v}))  } placeholder="Last name"   />} />]} />
          <Row ch={[<F key="a" ch={<Combo value={tp.from}   onChange={v => setTp(p => ({...p,from:v}))} options={WARDS} />} />, <F key="b" ch={<Combo value={tp.to} onChange={v => setTp(p => ({...p,to:v}))} options={WARDS} />} />]} />
          <Btn block color="#fde68a" onClick={() => post('/transfers', { patient_id: tp.pid, first_name: tp.first, last_name: tp.last, from_ward: tp.from, to_ward: tp.to, reason: tp.reason })}>Send Transfer</Btn>
        </div>
      )}
      {tab === 'lab' && (
        <div>
          <Row ch={[<F key="a" ch={<TxtIn value={lp.pid} onChange={v => setLp(p => ({...p,pid:v}))} placeholder="Patient ID *" />} />, <F key="b" ch={<Combo value={lp.test} onChange={v => setLp(p => ({...p,test:v,value:(LAB_TESTS.find(lt=>lt.name===v)||LAB_TESTS[3]).lo}))} options={LAB_TESTS.map(lt=>lt.name)} />} />]} />
          <div style={{ marginBottom: 8 }}><Slider value={lp.value} min={0} max={labTest.hi*2} step={0.1} onChange={v => setLp(p => ({...p,value:v}))} fmt={v => v.toFixed(1)+' '+labTest.unit} /></div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
            <span style={{ fontSize: 11, color: C.subtle }}>Normal: {labTest.lo}-{labTest.hi} {labTest.unit}</span>
            {(lp.value < labTest.lo || lp.value > labTest.hi) && <Pill color="#f87171">ABNORMAL</Pill>}
          </div>
          <Btn block color="#34d399" onClick={() => post('/lab-results', { patient_id: lp.pid, test_name: lp.test, value: lp.value, unit: labTest.unit, reference_lo: labTest.lo, reference_hi: labTest.hi })}>Send Lab Result</Btn>
        </div>
      )}
      {tab === 'alert' && (
        <div>
          <Row ch={[<F key="a" ch={<TxtIn value={alp.pid} onChange={v => setAlp(p => ({...p,pid:v}))} placeholder="Patient ID *" />} />, <F key="b" ch={<Combo value={alp.sev} onChange={v => setAlp(p => ({...p,sev:v}))} options={['low','medium','high','critical']} />} />]} />
          <Row ch={[<F key="a" ch={<Combo value={alp.cat} onChange={v => setAlp(p => ({...p,cat:v}))} options={['vital','lab','medication','system']} />} />, <F key="b" ch={<TxtIn value={alp.msg} onChange={v => setAlp(p => ({...p,msg:v}))} placeholder="Message *" />} />]} />
          <Row ch={[<F key="a" ch={<NumIn value={alp.val} onChange={v => setAlp(p => ({...p,val:v}))} placeholder="Current value" />} />, <F key="b" ch={<NumIn value={alp.thr} onChange={v => setAlp(p => ({...p,thr:v}))} placeholder="Threshold"     />} />]} />
          <Btn block color="#f87171" onClick={() => post('/alerts', { patient_id: alp.pid, severity: alp.sev, category: alp.cat, message: alp.msg, value: Number(alp.val), threshold: Number(alp.thr) })}>Send Alert</Btn>
        </div>
      )}
    </Section>
  )
}

export default function App() {
  const { events, connected, tickRef } = useSSE('/events/stream')
  const [tphData, setTphData]       = useState([])
  const [selected, setSelected]     = useState(null)
  const [typeFilter, setTypeFilter] = useState('all')
  const [textFilter, setTextFilter] = useState('')
  const [injMsg, setInjMsg]         = useState(null)
  const [chaos, setChaos]           = useState({})

  useEffect(() => {
    const id = setInterval(() => {
      const snap = Object.assign({}, tickRef.current)
      tickRef.current = {}
      const total = Object.values(snap).reduce((s, v) => s + v, 0)
      setTphData(prev => prev.concat([{ s: new Date().toLocaleTimeString(), total, ...snap }]).slice(-60))
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

  const counts    = events.reduce((a, e) => { a[e.type] = (a[e.type]||0)+1; return a }, {})
  const svcCounts = events.reduce((a, e) => { a[e.source] = (a[e.source]||0)+1; return a }, {})
  const dlqEvents   = events.filter(e => e.type && e.type.endsWith('-dlq'))
  const alertEvents = events.filter(e => e.type === 'alert-created')
  const criticals   = alertEvents.filter(e => { try { return JSON.parse(e.payload).severity === 'critical' } catch (_) { return false } })
  const commands    = counts['command-patient-admit'] || 0
  const downstream  = (counts['patient-admitted']||0) + (counts['lab-result-created']||0) + (counts['fhir-document-created']||0) + (counts['notification-sent']||0)
  const multiplier  = commands > 0 ? (downstream/commands).toFixed(1) : '-'
  const currentTph  = tphData.length > 0 ? (tphData[tphData.length-1].total||0) : 0
  const retryEst    = dlqEvents.length * 5

  const typeOpts = [
    { value: 'all', label: 'All types' },
    ...Object.keys(COLORS).map(k => ({ value: k, label: k })),
    { value: '_dlq', label: '*.dlq (failed msgs)' },
  ]

  const filteredEvents = events.filter(e => {
    if (typeFilter === '_dlq') return e.type && e.type.endsWith('-dlq')
    if (typeFilter !== 'all' && e.type !== typeFilter) return false
    if (textFilter) {
      const q = textFilter.toLowerCase()
      return (e.correlation_id && e.correlation_id.toLowerCase().includes(q))
          || (e.source && e.source.toLowerCase().includes(q))
          || (e.type && e.type.toLowerCase().includes(q))
    }
    return true
  }).slice(0, 100)

  const barData = Object.entries(counts).map(([type, count]) => ({
    name: type.split('-').slice(0,2).join('-'), count, fill: COLORS[type]||'#60a5fa',
  }))

  return (
    <div style={{ minHeight: '100vh', background: C.bg, padding: '20px 24px', fontFamily: 'system-ui,-apple-system,sans-serif' }}>
      <style>{'@keyframes wmPulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.35;transform:scale(0.65)}} *{box-sizing:border-box} input::placeholder{color:#475569} select option{background:#1e293b}'}</style>
      <div style={{ maxWidth: 1440, margin: '0 auto' }}>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: 20, fontWeight: 800, color: C.bright, lineHeight: 1 }}>Health ESB</div>
            <div style={{ fontSize: 11, color: C.subtle, marginTop: 2 }}>powered by <span style={{ color: '#60a5fa', fontWeight: 600 }}>Watermill</span></div>
          </div>
          {injMsg && <div style={{ fontSize: 12, color: '#34d399', background: '#34d39918', border: '1px solid #34d39940', borderRadius: 5, padding: '4px 12px' }}>{injMsg}</div>}
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
            {criticals.length > 0 && <Pill color="#f87171">{criticals.length} critical alert{criticals.length>1?'s':''}</Pill>}
            <Pill color={connected ? '#34d399' : '#f87171'}>{connected ? 'LIVE' : 'DISCONNECTED'}</Pill>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
          <Card label="Total Events"  value={events.length}                          color="#60a5fa" />
          <Card label="Admitted"      value={counts['patient-admitted']     ||0}     color="#38bdf8" />
          <Card label="Discharged"    value={counts['patient-discharged']   ||0}     color="#86efac" />
          <Card label="Transferred"   value={counts['patient-transferred']  ||0}     color="#fde68a" />
          <Card label="Lab Results"   value={counts['lab-result-created']   ||0}     color="#34d399" />
          <Card label="FHIR Docs"     value={counts['fhir-document-created']||0}     color="#a78bfa" />
          <Card label="Notifications" value={counts['notification-sent']    ||0}     color="#fb923c" />
          <Card label="Alerts"        value={counts['alert-created']        ||0}     color="#f87171" note={criticals.length>0?criticals.length+' critical':undefined} />
          <Card label="DLQ"           value={dlqEvents.length}                       color="#f87171" note={dlqEvents.length>0?'~'+retryEst+' retries':undefined} />
          <Card label="Fan-out x"     value={multiplier}                             color="#a78bfa" note="downstream/command" />
          <Card label="Evt/s"         value={currentTph}                             color="#fbbf24" />
        </div>

        <Section title="Message Pipeline" style={{ marginBottom: 16, overflowX: 'auto' }}>
          <Pipeline svcCounts={svcCounts} chaos={chaos} tph={currentTph} />
        </Section>

        <div style={{ display: 'flex', gap: 16, marginBottom: 16, flexWrap: 'wrap' }}>
          <Section title="Throughput (events / sec)" style={{ flex: '2 1 380px', minWidth: 280 }}>
            {tphData.length < 2 ? (
              <div style={{ height: 160, display: 'flex', alignItems: 'center', justifyContent: 'center', color: C.muted, fontSize: 12 }}>Waiting for events...</div>
            ) : (
              <ResponsiveContainer width="100%" height={160}>
                <AreaChart data={tphData} margin={{ top: 4, right: 4, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                  <XAxis dataKey="s" tick={{ fill: C.subtle, fontSize: 9 }} interval="preserveStartEnd" />
                  <YAxis allowDecimals={false} tick={{ fill: C.subtle, fontSize: 10 }} />
                  <Tooltip contentStyle={{ background: C.panel, border: '1px solid '+C.border, color: C.text, fontSize: 11 }} />
                  {Object.entries(COLORS).map(([topic, color]) => (
                    <Area key={topic} type="monotone" dataKey={topic} stroke={color} fill={color+'20'} stackId="s" isAnimationActive={false} />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            )}
          </Section>

          <Section title="Events by Type" style={{ flex: '1 1 220px', minWidth: 200 }}>
            {barData.length === 0 ? (
              <div style={{ height: 160, display: 'flex', alignItems: 'center', justifyContent: 'center', color: C.muted, fontSize: 12 }}>No data yet</div>
            ) : (
              <ResponsiveContainer width="100%" height={160}>
                <BarChart data={barData} margin={{ top: 4, right: 4, bottom: 18, left: -20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                  <XAxis dataKey="name" tick={{ fill: C.subtle, fontSize: 8 }} angle={-35} textAnchor="end" />
                  <YAxis tick={{ fill: C.subtle, fontSize: 10 }} />
                  <Tooltip contentStyle={{ background: C.panel, border: '1px solid '+C.border, color: C.text, fontSize: 11 }} />
                  <Bar dataKey="count" radius={[3,3,0,0]} isAnimationActive={false}>
                    {barData.map((entry, i) => <Cell key={i} fill={entry.fill} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </Section>

          <Section title="DLQ and Alerts" style={{ flex: '1 1 180px', minWidth: 180 }}>
            {dlqEvents.length === 0 && alertEvents.length === 0 ? (
              <div style={{ height: 160, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#34d399', fontSize: 13 }}>All clear</div>
            ) : (
              <div style={{ overflowY: 'auto', maxHeight: 160 }}>
                {dlqEvents.slice(0,20).map((e, i) => (
                  <div key={'dlq'+i} onClick={() => setSelected(prev => prev===e?null:e)}
                    style={{ background: '#f8717115', borderRadius: 4, padding: '4px 8px', marginBottom: 3, fontSize: 11, cursor: 'pointer', display: 'flex', gap: 6, alignItems: 'center' }}>
                    <span style={{ color: '#f87171', fontWeight: 700 }}>DLQ</span>
                    <span style={{ color: C.subtle }}>{e.type?(e.type.replace('-dlq','')):''}</span>
                  </div>
                ))}
                {alertEvents.slice(0,15).map((e, i) => {
                  let sev = 'low'; try { sev = JSON.parse(e.payload).severity } catch (_) {}
                  return (
                    <div key={'al'+i} onClick={() => setSelected(prev => prev===e?null:e)}
                      style={{ background: (SEV[sev]||'#34d399')+'18', borderRadius: 4, padding: '4px 8px', marginBottom: 3, fontSize: 11, cursor: 'pointer', display: 'flex', gap: 6, alignItems: 'center' }}>
                      <span style={{ color: SEV[sev]||'#34d399', fontWeight: 700 }}>ALERT</span>
                      <Pill color={SEV[sev]}>{sev}</Pill>
                    </div>
                  )
                })}
              </div>
            )}
          </Section>
        </div>

        <div style={{ marginBottom: 16 }}><ScenarioPanel /></div>

        <div style={{ display: 'flex', gap: 16, marginBottom: 16, flexWrap: 'wrap' }}>
          <div style={{ flex: '1 1 240px', minWidth: 240 }}><SimulatorPanel /></div>
          <div style={{ flex: '2 1 340px', minWidth: 300 }}><ChaosPanel /></div>
        </div>

        <div style={{ marginBottom: 16 }}><EventInjector onResult={setInjMsg} /></div>

        {selected && <Inspector event={selected} onClose={() => setSelected(null)} />}

        <Section>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 10, fontWeight: 700, color: C.subtle, textTransform: 'uppercase', letterSpacing: 1.2 }}>Live Event Stream</span>
            <span style={{ fontSize: 11, color: C.muted }}>click row to inspect — Trace filters by correlation-id</span>
            <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <Combo value={typeFilter} onChange={setTypeFilter} options={typeOpts} style={{ width: 210 }} />
              <TxtIn value={textFilter} onChange={setTextFilter} placeholder="Filter corr-id / source / type" style={{ width: 220 }} />
              {textFilter && <Btn small color={C.subtle} onClick={() => setTextFilter('')}>clear</Btn>}
            </div>
          </div>
          <div style={{ overflowY: 'auto', maxHeight: 420, fontFamily: 'monospace' }}>
            {filteredEvents.map((e, i) => (
              <div key={i} onClick={() => setSelected(prev => prev===e?null:e)}
                style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 6px', borderBottom: '1px solid '+C.bg, fontSize: 11, cursor: 'pointer', borderRadius: 4, background: selected===e?C.bg:'transparent', transition: 'background 0.1s' }}>
                <span style={{ color: C.muted, minWidth: 64, fontSize: 9, flexShrink: 0 }}>{new Date(e.timestamp).toLocaleTimeString()}</span>
                <Pill color={COLORS[e.type]||C.subtle}>{e.type}</Pill>
                <span style={{ color: C.dim, minWidth: 90, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{e.source}</span>
                <span style={{ color: C.muted, fontSize: 9, flexShrink: 0 }}>{e.correlation_id?(e.correlation_id.slice(0,8)):''}</span>
                <div style={{ marginLeft: 'auto', display: 'flex', gap: 4, flexShrink: 0 }}>
                  {e.type==='alert-created'&&(()=>{try{const p=JSON.parse(e.payload);return<Pill color={SEV[p.severity]}>{p.severity}</Pill>}catch(_){return null}})()}
                  {e.type==='lab-result-created'&&(()=>{try{const p=JSON.parse(e.payload);return p.abnormal?<Pill color="#f87171">abnormal</Pill>:null}catch(_){return null}})()}
                  {e.type&&e.type.endsWith('-dlq')&&<Pill color="#f87171">DLQ</Pill>}
                  <Btn small color={C.border} onClick={ev => { ev.stopPropagation(); setTextFilter(e.correlation_id||''); setTypeFilter('all') }}>Trace</Btn>
                </div>
              </div>
            ))}
            {filteredEvents.length === 0 && (
              <div style={{ color: C.muted, textAlign: 'center', padding: 36, fontSize: 12 }}>
                {events.length === 0 ? 'Waiting for events...' : 'No events match the current filter'}
              </div>
            )}
          </div>
          <div style={{ marginTop: 8, fontSize: 10, color: C.muted, textAlign: 'right' }}>{filteredEvents.length} / {events.length} shown</div>
        </Section>

      </div>
    </div>
  )
}
