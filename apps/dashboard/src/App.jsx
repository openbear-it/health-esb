import { useState, useEffect, useRef, useCallback } from 'react'
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, PieChart, Pie, Cell, Legend,
} from 'recharts'

// ─── Constants ────────────────────────────────────────────────────────────────

const TOPIC_COLORS = {
  'command-patient-admit':  '#7dd3fc',
  'patient-admitted':       '#38bdf8',
  'patient-discharged':     '#86efac',
  'patient-transferred':    '#fde68a',
  'lab-result-created':     '#34d399',
  'fhir-document-created':  '#a78bfa',
  'notification-sent':      '#fb923c',
  'alert-created':          '#f87171',
}

const SEVERITY_COLORS = {
  low:      '#34d399',
  medium:   '#fbbf24',
  high:     '#fb923c',
  critical: '#f87171',
}

const SERVICE_CHAIN = [
  { id: 'gateway',              label: 'Gateway',       color: '#60a5fa' },
  { id: 'gateway-sim',          label: 'Sim',           color: '#7dd3fc' },
  { id: 'adt-service',          label: 'ADT Service',   color: '#38bdf8' },
  { id: 'lab-service',          label: 'Lab Service',   color: '#34d399' },
  { id: 'fhir-bridge',          label: 'FHIR Bridge',   color: '#a78bfa' },
  { id: 'notification-service', label: 'Notification',  color: '#fb923c' },
  { id: 'audit-service',        label: 'Audit',         color: '#f9a8d4' },
]

const CHAOS_SERVICES = [
  'adt-service', 'lab-service', 'fhir-bridge', 'notification-service', 'audit-service',
]

const WARDS = ['ICU', 'Cardiology', 'Oncology', 'Pediatrics', 'Emergency', 'Surgery', 'Neurology', 'Orthopedics']
const LAB_TESTS = [
  { name: 'Hemoglobin', lo: 12.0, hi: 17.5, unit: 'g/dL' },
  { name: 'White Blood Cells', lo: 4.5, hi: 11.0, unit: '10³/uL' },
  { name: 'Platelets', lo: 150, hi: 400, unit: '10³/uL' },
  { name: 'Glucose', lo: 70, hi: 100, unit: 'mg/dL' },
  { name: 'Creatinine', lo: 0.6, hi: 1.2, unit: 'mg/dL' },
]

const API = ''   // same origin; change to http://localhost:8080 for dev

// ─── SSE hook ────────────────────────────────────────────────────────────────

function useSSE(url) {
  const [events, setEvents] = useState([])
  const [connected, setConnected] = useState(false)
  const tickCountsRef = useRef({})

  useEffect(() => {
    const es = new EventSource(url)
    es.onopen  = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.addEventListener('event', (e) => {
      try {
        const evt = JSON.parse(e.data)
        tickCountsRef.current[evt.type] = (tickCountsRef.current[evt.type] || 0) + 1
        setEvents(prev => [evt, ...prev].slice(0, 500))
      } catch (_) {}
    })
    return () => es.close()
  }, [url])

  return { events, connected, tickCountsRef }
}

// ─── Primitive UI components ──────────────────────────────────────────────────

function Badge({ color, children, style }) {
  return (
    <span style={{
      background: color + '22', color, border: `1px solid ${color}55`,
      borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600,
      letterSpacing: 0.5, ...style,
    }}>
      {children}
    </span>
  )
}

function StatCard({ label, value, color, sub }) {
  return (
    <div style={{
      background: '#1e293b', border: `1px solid ${color}44`,
      borderRadius: 8, padding: '14px 18px', flex: 1, minWidth: 110,
    }}>
      <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: '#64748b', marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

function Panel({ title, children, style }) {
  return (
    <div style={{ background: '#1e293b', borderRadius: 8, padding: 16, ...style }}>
      {title && (
        <div style={{ fontSize: 11, color: '#64748b', marginBottom: 12,
          fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
          {title}
        </div>
      )}
      {children}
    </div>
  )
}

function Btn({ children, onClick, color = '#60a5fa', danger, disabled, small }) {
  const bg = danger ? '#f8717133' : color + '22'
  const border = danger ? '#f87171' : color
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        background: bg, border: `1px solid ${border}55`, color: danger ? '#f87171' : color,
        borderRadius: 6, padding: small ? '4px 10px' : '6px 14px',
        fontSize: small ? 11 : 12, fontWeight: 600, cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.5 : 1,
      }}
    >
      {children}
    </button>
  )
}

function Input({ value, onChange, placeholder, style }) {
  return (
    <input
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      style={{
        background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0',
        borderRadius: 6, padding: '6px 10px', fontSize: 12, width: '100%',
        boxSizing: 'border-box', outline: 'none', ...style,
      }}
    />
  )
}

function Select({ value, onChange, options, style }) {
  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      style={{
        background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0',
        borderRadius: 6, padding: '6px 10px', fontSize: 12, width: '100%',
        boxSizing: 'border-box', outline: 'none', ...style,
      }}
    >
      {options.map(o => (
        <option key={o.value ?? o} value={o.value ?? o}>{o.label ?? o}</option>
      ))}
    </select>
  )
}

function SliderRow({ label, value, min, max, step, onChange, fmt }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
      <span style={{ fontSize: 11, color: '#94a3b8', minWidth: 100 }}>{label}</span>
      <input
        type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(Number(e.target.value))}
        style={{ flex: 1, accentColor: '#60a5fa' }}
      />
      <span style={{ fontSize: 12, color: '#e2e8f0', minWidth: 40, textAlign: 'right' }}>
        {fmt ? fmt(value) : value}
      </span>
    </div>
  )
}

// ─── Message inspector ────────────────────────────────────────────────────────

function MessageInspector({ event, onClose }) {
  if (!event) return null
  let payload = null
  try { payload = JSON.parse(event.payload ?? 'null') } catch (_) { payload = event.payload }
  const color = TOPIC_COLORS[event.type] || '#94a3b8'

  return (
    <Panel style={{ marginBottom: 24, border: `1px solid ${color}55` }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 }}>
        <span style={{ fontSize: 11, color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
          Message Inspector
        </span>
        <Badge color={color}>{event.type}</Badge>
        <span style={{ color: '#64748b', fontSize: 11 }}>{event.source}</span>
        <button onClick={onClose} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: '#64748b', cursor: 'pointer', fontSize: 16 }}>✕</button>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 12, fontSize: 12 }}>
        <div><span style={{ color: '#475569' }}>ID </span><span style={{ color: '#94a3b8', fontFamily: 'monospace' }}>{event.id}</span></div>
        <div><span style={{ color: '#475569' }}>Correlation </span><span style={{ color: '#94a3b8', fontFamily: 'monospace' }}>{event.correlation_id}</span></div>
        <div><span style={{ color: '#475569' }}>Timestamp </span><span style={{ color: '#94a3b8' }}>{new Date(event.timestamp).toISOString()}</span></div>
        <div><span style={{ color: '#475569' }}>Source </span><span style={{ color: '#94a3b8' }}>{event.source}</span></div>
      </div>
      <div style={{ fontSize: 11, color: '#64748b', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.5 }}>Payload</div>
      <pre style={{
        background: '#0f172a', borderRadius: 6, padding: 12, fontSize: 12,
        color: '#e2e8f0', overflowX: 'auto', margin: 0, fontFamily: 'monospace',
        whiteSpace: 'pre-wrap', wordBreak: 'break-all',
      }}>
        {JSON.stringify(payload, null, 2)}
      </pre>
    </Panel>
  )
}

// ─── Service node ─────────────────────────────────────────────────────────────

function ServiceNode({ service, count, chaotic }) {
  return (
    <div style={{
      background: '#1e293b',
      border: `2px solid ${chaotic ? '#f8717166' : service.color + '66'}`,
      borderRadius: 8, padding: '8px 14px', textAlign: 'center', minWidth: 90,
      position: 'relative',
    }}>
      {chaotic && (
        <div style={{ position: 'absolute', top: -8, right: -8, fontSize: 14 }}>⚡</div>
      )}
      <div style={{ fontSize: 10, color: chaotic ? '#f87171' : service.color, fontWeight: 700 }}>{service.label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: '#f1f5f9', marginTop: 2 }}>{count}</div>
      <div style={{ fontSize: 9, color: '#64748b' }}>events</div>
    </div>
  )
}

// ─── Simulator control panel ──────────────────────────────────────────────────

function SimulatorPanel() {
  const [status, setStatus] = useState({ enabled: false, rate: 1 })
  const [rate, setRate] = useState(1)
  const [loading, setLoading] = useState(false)
  const [feedback, setFeedback] = useState(null)

  const refresh = useCallback(async () => {
    try {
      const r = await fetch(`${API}/simulator/status`)
      const d = await r.json()
      setStatus(d)
      setRate(d.rate)
    } catch (_) {}
  }, [])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 3000)
    return () => clearInterval(id)
  }, [refresh])

  const apply = async (enabled, newRate) => {
    setLoading(true)
    try {
      const r = await fetch(`${API}/simulator/control`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled, rate: newRate }),
      })
      const d = await r.json()
      setStatus(d)
      setFeedback(enabled ? `Running at ${newRate} evt/s` : 'Stopped')
      setTimeout(() => setFeedback(null), 2000)
    } catch (e) {
      setFeedback('Error: ' + e.message)
    }
    setLoading(false)
  }

  const burst = async (n) => {
    setLoading(true)
    try {
      const res = await fetch(`${API}/simulator/control`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: true, rate: n }),
      })
      await res.json()
      setFeedback(`Burst: ${n} evt/s`)
      setTimeout(() => setFeedback(null), 2000)
      await refresh()
    } catch (_) {}
    setLoading(false)
  }

  return (
    <Panel title="Built-in Simulator" style={{ flex: 1 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <Badge color={status.enabled ? '#34d399' : '#64748b'}>
          {status.enabled ? `● ON — ${status.rate} evt/s` : '○ OFF'}
        </Badge>
        {feedback && <span style={{ fontSize: 11, color: '#fbbf24' }}>{feedback}</span>}
      </div>
      <SliderRow
        label="Rate (evt/s)"
        value={rate} min={0.1} max={20} step={0.1}
        onChange={setRate}
        fmt={v => `${v.toFixed(1)}/s`}
      />
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 }}>
        <Btn onClick={() => apply(true, rate)} disabled={loading} color="#34d399">▶ Start</Btn>
        <Btn onClick={() => apply(false, rate)} disabled={loading} color="#64748b">■ Stop</Btn>
        <Btn onClick={() => burst(5)} disabled={loading} small>Burst ×5</Btn>
        <Btn onClick={() => burst(10)} disabled={loading} small>Burst ×10</Btn>
        <Btn onClick={() => burst(20)} disabled={loading} small>Burst ×20</Btn>
      </div>
    </Panel>
  )
}

// ─── Chaos engineering panel ──────────────────────────────────────────────────

function ChaosPanel() {
  const [chaos, setChaos] = useState({})
  const [loading, setLoading] = useState(false)
  const [feedback, setFeedback] = useState(null)

  const refresh = useCallback(async () => {
    try {
      const r = await fetch(`${API}/chaos/status`)
      setChaos(await r.json())
    } catch (_) {}
  }, [])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 3000)
    return () => clearInterval(id)
  }, [refresh])

  const setMode = async (service, mode, errorRate) => {
    setLoading(true)
    try {
      await fetch(`${API}/chaos`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ service, mode, error_rate: errorRate }),
      })
      setFeedback(`${service}: ${mode || 'off'}`)
      setTimeout(() => setFeedback(null), 2000)
      await refresh()
    } catch (_) {}
    setLoading(false)
  }

  const resetAll = async () => {
    setLoading(true)
    try {
      await fetch(`${API}/chaos`, { method: 'DELETE' })
      setFeedback('All chaos reset')
      setTimeout(() => setFeedback(null), 2000)
      await refresh()
    } catch (_) {}
    setLoading(false)
  }

  return (
    <Panel title="⚡ Chaos Engineering" style={{ flex: 1 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        {feedback && <span style={{ fontSize: 11, color: '#fbbf24' }}>{feedback}</span>}
        <div style={{ marginLeft: 'auto' }}>
          <Btn onClick={resetAll} disabled={loading} danger small>Reset all</Btn>
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {CHAOS_SERVICES.map(svc => {
          const s = chaos[svc] || { mode: '', error_rate: 0 }
          const active = s.mode && s.mode !== ''
          return (
            <div key={svc} style={{
              display: 'flex', alignItems: 'center', gap: 8,
              background: active ? '#f8717111' : '#0f172a',
              borderRadius: 6, padding: '6px 10px',
              border: `1px solid ${active ? '#f8717144' : '#1e293b'}`,
            }}>
              <span style={{ fontSize: 11, color: active ? '#f87171' : '#94a3b8', minWidth: 140 }}>
                {active ? '⚡ ' : ''}{svc}
              </span>
              <Badge color={active ? '#f87171' : '#64748b'}>
                {s.mode || 'ok'} {active ? `${Math.round(s.error_rate * 100)}%` : ''}
              </Badge>
              <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
                <Btn small onClick={() => setMode(svc, 'poison', 1)} disabled={loading} danger>Poison</Btn>
                <Btn small onClick={() => setMode(svc, 'drop', 0.5)} disabled={loading} color="#fbbf24">Drop 50%</Btn>
                <Btn small onClick={() => setMode(svc, '', 0)} disabled={loading} color="#64748b">Off</Btn>
              </div>
            </div>
          )
        })}
      </div>
    </Panel>
  )
}

// ─── Manual event injection ───────────────────────────────────────────────────

function EventInjector({ onFeedback }) {
  const [tab, setTab] = useState('admission')
  // Admission form
  const [admPid, setAdmPid] = useState('P1234')
  const [admFirst, setAdmFirst] = useState('John')
  const [admLast, setAdmLast] = useState('Doe')
  const [admDob, setAdmDob] = useState('1980-01-01')
  const [admWard, setAdmWard] = useState('ICU')
  // Discharge form
  const [disPid, setDisPid] = useState('P1234')
  const [disFirst, setDisFirst] = useState('John')
  const [disLast, setDisLast] = useState('Doe')
  const [disWard, setDisWard] = useState('ICU')
  const [disReason, setDisReason] = useState('recovered')
  // Transfer form
  const [trPid, setTrPid] = useState('P1234')
  const [trFirst, setTrFirst] = useState('John')
  const [trLast, setTrLast] = useState('Doe')
  const [trFrom, setTrFrom] = useState('Emergency')
  const [trTo, setTrTo] = useState('ICU')
  const [trReason, setTrReason] = useState('stabilized')
  // Lab result form
  const [labPid, setLabPid] = useState('P1234')
  const [labTest, setLabTest] = useState(LAB_TESTS[0].name)
  const [labValue, setLabValue] = useState(8.0)
  // Alert form
  const [alPid, setAlPid] = useState('P1234')
  const [alSev, setAlSev] = useState('high')
  const [alCat, setAlCat] = useState('vital')
  const [alMsg, setAlMsg] = useState('Heart rate > 130 bpm')
  const [alVal, setAlVal] = useState(135)
  const [alThr, setAlThr] = useState(100)

  const post = async (path, body) => {
    try {
      const r = await fetch(`${API}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const d = await r.json()
      if (r.ok) {
        onFeedback(`✓ sent — corr: ${d.correlation_id?.slice(0, 8)}`)
      } else {
        onFeedback(`✗ ${d.error}`)
      }
    } catch (e) {
      onFeedback(`✗ ${e.message}`)
    }
  }

  const selectedTest = LAB_TESTS.find(t => t.name === labTest) || LAB_TESTS[0]

  const tabs = [
    { id: 'admission',  label: 'Admission' },
    { id: 'discharge',  label: 'Discharge' },
    { id: 'transfer',   label: 'Transfer' },
    { id: 'lab',        label: 'Lab Result' },
    { id: 'alert',      label: 'Alert' },
  ]

  return (
    <Panel title="Manual Event Injection">
      <div style={{ display: 'flex', gap: 4, marginBottom: 12 }}>
        {tabs.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            background: tab === t.id ? '#60a5fa22' : 'transparent',
            border: `1px solid ${tab === t.id ? '#60a5fa55' : '#334155'}`,
            color: tab === t.id ? '#60a5fa' : '#64748b',
            borderRadius: 4, padding: '4px 10px', fontSize: 11, cursor: 'pointer',
          }}>
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'admission' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <Input value={admPid} onChange={setAdmPid} placeholder="Patient ID *" />
          <Input value={admDob} onChange={setAdmDob} placeholder="Date of birth" />
          <Input value={admFirst} onChange={setAdmFirst} placeholder="First name *" />
          <Input value={admLast} onChange={setAdmLast} placeholder="Last name *" />
          <Select value={admWard} onChange={setAdmWard} options={WARDS} />
          <Btn onClick={() => post('/admissions', { patient_id: admPid, first_name: admFirst, last_name: admLast, date_of_birth: admDob, ward: admWard })} color="#38bdf8">
            Send Admission
          </Btn>
        </div>
      )}

      {tab === 'discharge' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <Input value={disPid} onChange={setDisPid} placeholder="Patient ID *" />
          <Select value={disWard} onChange={setDisWard} options={WARDS} />
          <Input value={disFirst} onChange={setDisFirst} placeholder="First name" />
          <Input value={disLast} onChange={setDisLast} placeholder="Last name" />
          <Select value={disReason} onChange={setDisReason} options={['recovered', 'transferred', 'deceased', 'self-discharge']} />
          <Btn onClick={() => post('/discharges', { patient_id: disPid, first_name: disFirst, last_name: disLast, ward: disWard, reason: disReason })} color="#86efac">
            Send Discharge
          </Btn>
        </div>
      )}

      {tab === 'transfer' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <Input value={trPid} onChange={setTrPid} placeholder="Patient ID *" />
          <Input value={trReason} onChange={setTrReason} placeholder="Reason" />
          <Input value={trFirst} onChange={setTrFirst} placeholder="First name" />
          <Input value={trLast} onChange={setTrLast} placeholder="Last name" />
          <Select value={trFrom} onChange={setTrFrom} options={WARDS} />
          <Select value={trTo} onChange={setTrTo} options={WARDS} />
          <div style={{ gridColumn: '1 / -1' }}>
            <Btn onClick={() => post('/transfers', { patient_id: trPid, first_name: trFirst, last_name: trLast, from_ward: trFrom, to_ward: trTo, reason: trReason })} color="#fde68a">
              Send Transfer
            </Btn>
          </div>
        </div>
      )}

      {tab === 'lab' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <Input value={labPid} onChange={setLabPid} placeholder="Patient ID *" />
          <Select value={labTest} onChange={setLabTest} options={LAB_TESTS.map(t => t.name)} />
          <SliderRow
            label="Value"
            value={labValue}
            min={0} max={selectedTest.hi * 2} step={0.1}
            onChange={setLabValue}
            fmt={v => `${v.toFixed(1)} ${selectedTest.unit}`}
          />
          <div style={{ fontSize: 11, color: '#64748b', padding: '6px 0' }}>
            Ref: {selectedTest.lo}–{selectedTest.hi} {selectedTest.unit}
            {(labValue < selectedTest.lo || labValue > selectedTest.hi) && (
              <Badge color="#f87171" style={{ marginLeft: 6 }}>ABNORMAL</Badge>
            )}
          </div>
          <div style={{ gridColumn: '1 / -1' }}>
            <Btn onClick={() => post('/lab-results', {
              patient_id: labPid, test_name: labTest, value: labValue,
              unit: selectedTest.unit, reference_lo: selectedTest.lo, reference_hi: selectedTest.hi,
            })} color="#34d399">
              Send Lab Result
            </Btn>
          </div>
        </div>
      )}

      {tab === 'alert' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          <Input value={alPid} onChange={setAlPid} placeholder="Patient ID *" />
          <Select value={alSev} onChange={setAlSev} options={['low', 'medium', 'high', 'critical']} />
          <Select value={alCat} onChange={setAlCat} options={['vital', 'lab', 'medication', 'system']} />
          <Input value={alMsg} onChange={setAlMsg} placeholder="Message *" />
          <Input value={alVal} onChange={v => setAlVal(Number(v))} placeholder="Value" />
          <Input value={alThr} onChange={v => setAlThr(Number(v))} placeholder="Threshold" />
          <div style={{ gridColumn: '1 / -1' }}>
            <Btn onClick={() => post('/alerts', {
              patient_id: alPid, severity: alSev, category: alCat,
              message: alMsg, value: alVal, threshold: alThr,
            })} color="#f87171">
              Send Alert
            </Btn>
          </div>
        </div>
      )}
    </Panel>
  )
}

// ─── App ──────────────────────────────────────────────────────────────────────

export default function App() {
  const { events, connected, tickCountsRef } = useSSE('/events/stream')
  const [throughputData, setThroughputData] = useState([])
  const [selectedEvent, setSelectedEvent] = useState(null)
  const [filter, setFilter] = useState('')
  const [typeFilter, setTypeFilter] = useState('all')
  const [injectorFeedback, setInjectorFeedback] = useState(null)
  const [chaosStatusForPipeline, setChaosStatusForPipeline] = useState({})

  // Throughput tick — runs every second, snapshots the counter ref
  useEffect(() => {
    const id = setInterval(() => {
      const snapshot = { ...tickCountsRef.current }
      tickCountsRef.current = {}
      const total = Object.values(snapshot).reduce((s, v) => s + v, 0)
      setThroughputData(prev => [
        ...prev,
        { time: new Date().toLocaleTimeString(), total, ...snapshot },
      ].slice(-60))
    }, 1000)
    return () => clearInterval(id)
  }, [tickCountsRef])

  // Poll chaos status for pipeline visualization
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`${API}/chaos/status`)
        setChaosStatusForPipeline(await r.json())
      } catch (_) {}
    }
    poll()
    const id = setInterval(poll, 4000)
    return () => clearInterval(id)
  }, [])

  // Feedback auto-dismiss
  useEffect(() => {
    if (!injectorFeedback) return
    const t = setTimeout(() => setInjectorFeedback(null), 3000)
    return () => clearTimeout(t)
  }, [injectorFeedback])

  const topicCounts = events.reduce((acc, e) => {
    acc[e.type] = (acc[e.type] || 0) + 1
    return acc
  }, {})

  const serviceCounts = events.reduce((acc, e) => {
    acc[e.source] = (acc[e.source] || 0) + 1
    return acc
  }, {})

  const dlqEvents = events.filter(e => e.type && e.type.endsWith('-dlq'))
  const alertEvents = events.filter(e => e.type === 'alert-created')
  const criticalAlerts = alertEvents.filter(e => {
    try { return JSON.parse(e.payload).severity === 'critical' } catch { return false }
  })

  const typeOptions = [{ value: 'all', label: 'All types' }, ...Object.keys(TOPIC_COLORS).map(t => ({ value: t, label: t }))]

  const filteredEvents = events.filter(e => {
    const matchType = typeFilter === 'all' || e.type === typeFilter
    const matchText = !filter || e.correlation_id?.includes(filter) || e.source?.includes(filter) || e.type?.includes(filter)
    return matchType && matchText
  }).slice(0, 100)

  const pieData = Object.entries(topicCounts).map(([name, value]) => ({ name, value }))

  return (
    <div style={{ minHeight: '100vh', padding: 24, background: '#0f172a', fontFamily: 'system-ui, sans-serif' }}>

      {/* ── Header ── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <div style={{ fontSize: 22, fontWeight: 800, color: '#f1f5f9' }}>Health ESB</div>
        <div style={{ fontSize: 13, color: '#64748b' }}>Live Event Dashboard</div>
        {injectorFeedback && (
          <div style={{ fontSize: 12, color: '#34d399', background: '#34d39922', borderRadius: 4, padding: '3px 10px' }}>
            {injectorFeedback}
          </div>
        )}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center' }}>
          {criticalAlerts.length > 0 && (
            <Badge color="#f87171">⚠ {criticalAlerts.length} critical</Badge>
          )}
          <Badge color={connected ? '#34d399' : '#f87171'}>
            {connected ? '● LIVE' : '○ DISCONNECTED'}
          </Badge>
        </div>
      </div>

      {/* ── Stat cards ── */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 24, flexWrap: 'wrap' }}>
        <StatCard label="Total Events" value={events.length} color="#60a5fa" />
        <StatCard label="Admitted"     value={topicCounts['patient-admitted'] || 0}      color="#38bdf8" />
        <StatCard label="Discharged"   value={topicCounts['patient-discharged'] || 0}    color="#86efac" />
        <StatCard label="Transferred"  value={topicCounts['patient-transferred'] || 0}   color="#fde68a" />
        <StatCard label="Lab Results"  value={topicCounts['lab-result-created'] || 0}    color="#34d399" />
        <StatCard label="FHIR Docs"    value={topicCounts['fhir-document-created'] || 0} color="#a78bfa" />
        <StatCard label="Notifications" value={topicCounts['notification-sent'] || 0}   color="#fb923c" />
        <StatCard label="Alerts"       value={topicCounts['alert-created'] || 0}         color="#f87171"
          sub={criticalAlerts.length > 0 ? `${criticalAlerts.length} critical` : undefined} />
        <StatCard label="DLQ"          value={dlqEvents.length}                          color="#f87171" />
      </div>

      {/* ── Service pipeline ── */}
      <Panel title="Event Pipeline" style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          {SERVICE_CHAIN.map((svc, i) => {
            const s = chaosStatusForPipeline[svc.id]
            const chaotic = s && s.mode && s.mode !== ''
            return (
              <div key={svc.id} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <ServiceNode service={svc} count={serviceCounts[svc.id] || 0} chaotic={chaotic} />
                {i < SERVICE_CHAIN.length - 1 && <div style={{ color: '#334155', fontSize: 18 }}>→</div>}
              </div>
            )
          })}
        </div>
      </Panel>

      {/* ── Throughput chart ── */}
      <Panel title="Throughput (events/sec)" style={{ marginBottom: 24 }}>
        <ResponsiveContainer width="100%" height={180}>
          <AreaChart data={throughputData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 10 }} />
            <YAxis allowDecimals={false} tick={{ fill: '#64748b', fontSize: 10 }} />
            <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0' }} />
            {Object.entries(TOPIC_COLORS).map(([topic, color]) => (
              <Area key={topic} type="monotone" dataKey={topic} stroke={color} fill={color + '22'} stackId="1" />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </Panel>

      {/* ── Event distribution ── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginBottom: 24 }}>
        {/* By type bar */}
        <Panel title="Events by Type">
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={Object.entries(topicCounts).map(([type, count]) => ({ type: type.split('-')[0], count, full: type }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="#0f172a" />
              <XAxis dataKey="type" tick={{ fill: '#64748b', fontSize: 9 }} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} />
              <Tooltip
                contentStyle={{ background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0' }}
                formatter={(val, _, props) => [val, props.payload.full]}
              />
              <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                {Object.entries(topicCounts).map(([type], i) => (
                  <Cell key={i} fill={TOPIC_COLORS[type] || '#60a5fa'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Panel>

        {/* Pie chart */}
        <Panel title="Distribution">
          <ResponsiveContainer width="100%" height={180}>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={70} label={false}>
                {pieData.map((entry, i) => (
                  <Cell key={i} fill={TOPIC_COLORS[entry.name] || '#60a5fa'} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0' }} />
              <Legend formatter={(v) => <span style={{ fontSize: 10, color: '#94a3b8' }}>{v}</span>} />
            </PieChart>
          </ResponsiveContainer>
        </Panel>

        {/* DLQ + Alerts panel */}
        <Panel title="DLQ & Alerts">
          {dlqEvents.length === 0 && alertEvents.length === 0 ? (
            <div style={{ color: '#34d399', fontSize: 12, marginTop: 30, textAlign: 'center' }}>✓ All clear</div>
          ) : (
            <div style={{ overflowY: 'auto', maxHeight: 170 }}>
              {dlqEvents.map((e, i) => (
                <div key={`dlq-${i}`} style={{ background: '#f8717122', borderRadius: 4, padding: '4px 8px', marginBottom: 3, fontSize: 11, cursor: 'pointer' }}
                  onClick={() => setSelectedEvent(prev => prev === e ? null : e)}>
                  <span style={{ color: '#f87171' }}>DLQ </span>
                  <span style={{ color: '#64748b' }}>{e.type}</span>
                </div>
              ))}
              {alertEvents.slice(0, 10).map((e, i) => {
                let sev = 'low'
                try { sev = JSON.parse(e.payload).severity } catch (_) {}
                return (
                  <div key={`al-${i}`} style={{ background: SEVERITY_COLORS[sev] + '22', borderRadius: 4, padding: '4px 8px', marginBottom: 3, fontSize: 11, cursor: 'pointer' }}
                    onClick={() => setSelectedEvent(prev => prev === e ? null : e)}>
                    <span style={{ color: SEVERITY_COLORS[sev] }}>ALERT </span>
                    <Badge color={SEVERITY_COLORS[sev]}>{sev}</Badge>
                  </div>
                )
              })}
            </div>
          )}
        </Panel>
      </div>

      {/* ── Control panels ── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <SimulatorPanel />
        <ChaosPanel />
      </div>

      {/* ── Manual event injection ── */}
      <div style={{ marginBottom: 24 }}>
        <EventInjector onFeedback={setInjectorFeedback} />
      </div>

      {/* ── Message inspector ── */}
      <MessageInspector event={selectedEvent} onClose={() => setSelectedEvent(null)} />

      {/* ── Live event stream ── */}
      <Panel>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
          <span style={{ fontSize: 11, color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
            Live Event Stream
          </span>
          <span style={{ fontSize: 11, color: '#475569' }}>— click a row to inspect</span>
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
            <Select value={typeFilter} onChange={setTypeFilter} options={typeOptions} style={{ width: 180 }} />
            <Input value={filter} onChange={setFilter} placeholder="Filter by corr-id / source / type…" style={{ width: 240 }} />
          </div>
        </div>
        <div style={{ overflowY: 'auto', maxHeight: 380, fontFamily: 'monospace' }}>
          {filteredEvents.map((e, i) => (
            <div
              key={i}
              onClick={() => setSelectedEvent(prev => prev === e ? null : e)}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '5px 6px', borderBottom: '1px solid #0f172a',
                fontSize: 12, cursor: 'pointer', borderRadius: 4,
                background: selectedEvent === e ? '#0f172a' : 'transparent',
              }}
            >
              <span style={{ color: '#475569', minWidth: 72, fontSize: 10 }}>
                {new Date(e.timestamp).toLocaleTimeString()}
              </span>
              <Badge color={TOPIC_COLORS[e.type] || '#94a3b8'}>{e.type}</Badge>
              <span style={{ color: '#94a3b8', minWidth: 90 }}>{e.source}</span>
              <span style={{ color: '#475569', fontSize: 10 }}>{e.correlation_id?.slice(0, 8)}</span>
              {e.type === 'alert-created' && (() => {
                try {
                  const p = JSON.parse(e.payload)
                  return <Badge color={SEVERITY_COLORS[p.severity] || '#64748b'}>{p.severity}</Badge>
                } catch { return null }
              })()}
              {e.type === 'lab-result-created' && (() => {
                try {
                  const p = JSON.parse(e.payload)
                  return p.abnormal ? <Badge color="#f87171">ABNORMAL</Badge> : null
                } catch { return null }
              })()}
            </div>
          ))}
          {filteredEvents.length === 0 && (
            <div style={{ color: '#475569', fontSize: 13, textAlign: 'center', padding: 32 }}>
              {events.length === 0 ? 'Waiting for events…' : 'No events match the current filter'}
            </div>
          )}
        </div>
        <div style={{ marginTop: 8, fontSize: 10, color: '#475569', textAlign: 'right' }}>
          {filteredEvents.length} / {events.length} events shown
        </div>
      </Panel>
    </div>
  )
}


// Topic names match backend constants (hyphens, not dots)
const TOPIC_COLORS = {
  'patient-admitted': '#38bdf8',
  'lab-result-created': '#34d399',
  'fhir-document-created': '#a78bfa',
  'notification-sent': '#fb923c',
}

const SERVICE_CHAIN = [
  { id: 'gateway', label: 'Gateway', color: '#60a5fa' },
  { id: 'adt-service', label: 'ADT Service', color: '#38bdf8' },
  { id: 'lab-service', label: 'Lab Service', color: '#34d399' },
  { id: 'fhir-bridge', label: 'FHIR Bridge', color: '#a78bfa' },
  { id: 'notification-service', label: 'Notification', color: '#fb923c' },
  { id: 'audit-service', label: 'Audit', color: '#f9a8d4' },
]

function useSSE(url) {
  const [events, setEvents] = useState([])
  const [connected, setConnected] = useState(false)
  // Counts events received since last throughput tick, keyed by type.
  // Mutated directly so the interval always sees the latest value without
  // depending on React state (avoids stale-closure issues).
  const tickCountsRef = useRef({})

  useEffect(() => {
    const es = new EventSource(url)
    es.onopen = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.addEventListener('event', (e) => {
      try {
        const evt = JSON.parse(e.data)
        tickCountsRef.current[evt.type] = (tickCountsRef.current[evt.type] || 0) + 1
        setEvents(prev => [evt, ...prev].slice(0, 200))
      } catch (_) {}
    })
    return () => es.close()
  }, [url])

  return { events, connected, tickCountsRef }
}

function Badge({ color, children }) {
  return (
    <span style={{
      background: color + '22',
      color,
      border: `1px solid ${color}55`,
      borderRadius: 4,
      padding: '2px 8px',
      fontSize: 11,
      fontWeight: 600,
      letterSpacing: 0.5,
    }}>
      {children}
    </span>
  )
}

function StatCard({ label, value, color }) {
  return (
    <div style={{
      background: '#1e293b',
      border: `1px solid ${color}44`,
      borderRadius: 8,
      padding: '16px 20px',
      flex: 1,
      minWidth: 120,
    }}>
      <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700, color }}>{value}</div>
    </div>
  )
}

function ServiceNode({ service, count }) {
  return (
    <div style={{
      background: '#1e293b',
      border: `2px solid ${service.color}66`,
      borderRadius: 8,
      padding: '10px 16px',
      textAlign: 'center',
      minWidth: 100,
    }}>
      <div style={{ fontSize: 11, color: service.color, fontWeight: 700 }}>{service.label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: '#f1f5f9', marginTop: 4 }}>{count}</div>
      <div style={{ fontSize: 10, color: '#64748b' }}>events</div>
    </div>
  )
}

function MessageInspector({ event, onClose }) {
  if (!event) return null
  let payload = null
  try { payload = JSON.parse(event.payload ?? 'null') } catch (_) { payload = event.payload }
  const color = TOPIC_COLORS[event.type] || '#94a3b8'
  return (
    <div style={{ background: '#1e293b', borderRadius: 8, padding: 16, marginBottom: 24, border: `1px solid ${color}55` }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 }}>
        <div style={{ fontSize: 12, color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
          Message Inspector
        </div>
        <Badge color={color}>{event.type}</Badge>
        <span style={{ color: '#64748b', fontSize: 11, marginLeft: 4 }}>{event.source}</span>
        <button onClick={onClose} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: '#64748b', cursor: 'pointer', fontSize: 16 }}>✕</button>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 12, fontSize: 12 }}>
        <div><span style={{ color: '#475569' }}>ID </span><span style={{ color: '#94a3b8', fontFamily: 'monospace' }}>{event.id}</span></div>
        <div><span style={{ color: '#475569' }}>Correlation </span><span style={{ color: '#94a3b8', fontFamily: 'monospace' }}>{event.correlation_id}</span></div>
        <div><span style={{ color: '#475569' }}>Timestamp </span><span style={{ color: '#94a3b8' }}>{new Date(event.timestamp).toISOString()}</span></div>
      </div>
      <div style={{ fontSize: 11, color: '#64748b', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.5 }}>Payload</div>
      <pre style={{
        background: '#0f172a',
        borderRadius: 6,
        padding: 12,
        fontSize: 12,
        color: '#e2e8f0',
        overflowX: 'auto',
        margin: 0,
        fontFamily: 'monospace',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-all',
      }}>
        {JSON.stringify(payload, null, 2)}
      </pre>
    </div>
  )
}

export default function App() {
  const { events, connected, tickCountsRef } = useSSE('/events/stream')
  const [throughputData, setThroughputData] = useState([])
  const [selectedEvent, setSelectedEvent] = useState(null)

  // Throughput: every second, snapshot & reset the counter ref.
  // No dependency on `events` → interval is created once, never recreated.
  useEffect(() => {
    const id = setInterval(() => {
      const snapshot = { ...tickCountsRef.current }
      tickCountsRef.current = {}
      const total = Object.values(snapshot).reduce((s, v) => s + v, 0)
      setThroughputData(prev => [
        ...prev,
        { time: new Date().toLocaleTimeString(), total, ...snapshot },
      ].slice(-30))
    }, 1000)
    return () => clearInterval(id)
  }, [tickCountsRef])

  const topicCounts = events.reduce((acc, e) => {
    acc[e.type] = (acc[e.type] || 0) + 1
    return acc
  }, {})

  const serviceCounts = events.reduce((acc, e) => {
    acc[e.source] = (acc[e.source] || 0) + 1
    return acc
  }, {})

  const dlqEvents = events.filter(e => e.type && e.type.endsWith('-dlq'))

  return (
    <div style={{ minHeight: '100vh', padding: 24, background: '#0f172a' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <div style={{ fontSize: 22, fontWeight: 800, color: '#f1f5f9' }}>Health ESB</div>
        <div style={{ fontSize: 14, color: '#64748b' }}>Live Event Dashboard</div>
        <div style={{ marginLeft: 'auto' }}>
          <Badge color={connected ? '#34d399' : '#f87171'}>
            {connected ? '● LIVE' : '○ DISCONNECTED'}
          </Badge>
        </div>
      </div>

      {/* Stats Row */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 24, flexWrap: 'wrap' }}>
        <StatCard label="Total Events" value={events.length} color="#60a5fa" />
        <StatCard label="Admitted" value={topicCounts['patient-admitted'] || 0} color="#38bdf8" />
        <StatCard label="Lab Results" value={topicCounts['lab-result-created'] || 0} color="#34d399" />
        <StatCard label="FHIR Docs" value={topicCounts['fhir-document-created'] || 0} color="#a78bfa" />
        <StatCard label="Notifications" value={topicCounts['notification-sent'] || 0} color="#fb923c" />
        <StatCard label="DLQ" value={dlqEvents.length} color="#f87171" />
      </div>

      {/* Service pipeline */}
      <div style={{ background: '#1e293b', borderRadius: 8, padding: 16, marginBottom: 24 }}>
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
          Event Pipeline
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          {SERVICE_CHAIN.map((svc, i) => (
            <div key={svc.id} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <ServiceNode service={svc} count={serviceCounts[svc.id] || 0} />
              {i < SERVICE_CHAIN.length - 1 && (
                <div style={{ color: '#334155', fontSize: 20 }}>→</div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Throughput Chart */}
      <div style={{ background: '#1e293b', borderRadius: 8, padding: 16, marginBottom: 24 }}>
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
          Throughput (events/sec)
        </div>
        <ResponsiveContainer width="100%" height={180}>
          <AreaChart data={throughputData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 10 }} />
            <YAxis allowDecimals={false} tick={{ fill: '#64748b', fontSize: 10 }} />
            <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0' }} />
            {Object.entries(TOPIC_COLORS).map(([topic, color]) => (
              <Area key={topic} type="monotone" dataKey={topic} stroke={color} fill={color + '22'} stackId="1" />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Event Distribution */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <div style={{ background: '#1e293b', borderRadius: 8, padding: 16 }}>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
            Events by Type
          </div>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={Object.entries(topicCounts).map(([type, count]) => ({ type: type.split('.')[0], count, full: type }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="#0f172a" />
              <XAxis dataKey="type" tick={{ fill: '#64748b', fontSize: 10 }} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} />
              <Tooltip
                contentStyle={{ background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0' }}
                formatter={(val, _, props) => [val, props.payload.full]}
              />
              <Bar dataKey="count" fill="#60a5fa" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* DLQ Panel */}
        <div style={{ background: '#1e293b', borderRadius: 8, padding: 16 }}>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
            Dead-Letter Queue
          </div>
          {dlqEvents.length === 0 ? (
            <div style={{ color: '#34d399', fontSize: 13, marginTop: 40, textAlign: 'center' }}>
              ✓ No DLQ messages
            </div>
          ) : (
            <div style={{ overflowY: 'auto', maxHeight: 140 }}>
              {dlqEvents.map((e, i) => (
                <div key={i} style={{ background: '#f8717122', borderRadius: 4, padding: '4px 8px', marginBottom: 4, fontSize: 11 }}>
                  <span style={{ color: '#f87171' }}>{e.type}</span>
                  <span style={{ color: '#64748b', marginLeft: 8 }}>{e.correlation_id?.slice(0, 8)}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Message Inspector */}
      <MessageInspector event={selectedEvent} onClose={() => setSelectedEvent(null)} />

      {/* Live Event Stream */}
      <div style={{ background: '#1e293b', borderRadius: 8, padding: 16 }}>
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
          Live Event Stream <span style={{ fontWeight: 400, textTransform: 'none', fontSize: 11 }}> — click a row to inspect</span>
        </div>
        <div style={{ overflowY: 'auto', maxHeight: 320, fontFamily: 'monospace' }}>
          {events.slice(0, 50).map((e, i) => (
            <div
              key={i}
              onClick={() => setSelectedEvent(prev => prev === e ? null : e)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                padding: '5px 6px',
                borderBottom: '1px solid #0f172a',
                fontSize: 12,
                cursor: 'pointer',
                borderRadius: 4,
                background: selectedEvent === e ? '#0f172a' : 'transparent',
              }}
            >
              <span style={{ color: '#475569', minWidth: 80, fontSize: 10 }}>
                {new Date(e.timestamp).toLocaleTimeString()}
              </span>
              <Badge color={TOPIC_COLORS[e.type] || '#94a3b8'}>{e.type}</Badge>
              <span style={{ color: '#94a3b8' }}>{e.source}</span>
              <span style={{ color: '#475569', fontSize: 10 }}>{e.correlation_id?.slice(0, 8)}</span>
            </div>
          ))}
          {events.length === 0 && (
            <div style={{ color: '#475569', fontSize: 13, textAlign: 'center', padding: 32 }}>
              Waiting for events…
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
