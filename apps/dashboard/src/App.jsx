import { useState, useEffect, useRef } from 'react'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts'

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
  // Arrival ring-buffer: [{t: ms, type: string}] — mutated directly, no re-render
  const arrivalsRef = useRef([])

  useEffect(() => {
    const es = new EventSource(url)
    es.onopen = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.addEventListener('event', (e) => {
      try {
        const evt = JSON.parse(e.data)
        arrivalsRef.current = [{ t: Date.now(), type: evt.type }, ...arrivalsRef.current].slice(0, 1000)
        setEvents(prev => [evt, ...prev].slice(0, 200))
      } catch (_) {}
    })
    return () => es.close()
  }, [url])

  return { events, connected, arrivalsRef }
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
  const { events, connected, arrivalsRef } = useSSE('/events/stream')
  const [throughputData, setThroughputData] = useState([])
  const [selectedEvent, setSelectedEvent] = useState(null)

  // Throughput: read from arrivalsRef on a fixed 1s tick — no events dependency
  useEffect(() => {
    const id = setInterval(() => {
      const cutoff = Date.now() - 1000
      const recent = arrivalsRef.current.filter(a => a.t > cutoff)
      const counts = {}
      recent.forEach(a => { counts[a.type] = (counts[a.type] || 0) + 1 })
      setThroughputData(prev => [
        ...prev,
        { time: new Date().toLocaleTimeString(), total: recent.length, ...counts },
      ].slice(-30))
    }, 1000)
    return () => clearInterval(id)
  }, [arrivalsRef])

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
