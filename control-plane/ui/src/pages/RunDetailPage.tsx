import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'

import { api } from '../lib/api'
import type { RunEvent, RunRecord } from '../lib/types'

export default function RunDetailPage() {
  const { runId } = useParams()
  const [run, setRun] = useState<RunRecord | null>(null)
  const [events, setEvents] = useState<RunEvent[]>([])
  const [error, setError] = useState<string | null>(null)
  const [stopping, setStopping] = useState(false)

  const logEndRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!runId) return

    api.runs
      .get(runId)
      .then(setRun)
      .catch((e) => setError(String(e)))
  }, [runId])

  useEffect(() => {
    if (!runId) return

    const apiKey = localStorage.getItem('falkordb-control-plane-api-key')
    const sseUrl = apiKey
      ? `/api/runs/${runId}/events?api_key=${encodeURIComponent(apiKey)}`
      : `/api/runs/${runId}/events`

    const es = new EventSource(sseUrl)

    es.addEventListener('message', (msg) => {
      try {
        const ev = JSON.parse((msg as MessageEvent).data) as RunEvent
        setEvents((prev) => [...prev.slice(-2000), ev])

        if (ev.type === 'exit') {
          // Refresh run record to capture exit status.
          api.runs.get(runId).then(setRun).catch(() => {})
        }
      } catch {
        // ignore
      }
    })

    es.addEventListener('error', () => {
      // Browser will auto-retry; only show error if we have no run.
    })

    return () => {
      es.close()
    }
  }, [runId])

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [events.length])

  const lines = useMemo(() => {
    return events
      .filter((e) => e.type === 'log')
      .map((e) => e as Extract<RunEvent, { type: 'log' }>)
  }, [events])

  async function stop() {
    if (!runId) return
    setStopping(true)
    try {
      await api.runs.stop(runId)
      const updated = await api.runs.get(runId)
      setRun(updated)
    } catch (e) {
      setError(String(e))
    } finally {
      setStopping(false)
    }
  }

  if (error) return <div className="text-destructive">{error}</div>
  if (!run) return <div className="text-foreground/70">Loading…</div>

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-sm text-foreground/60">Run</div>
          <h1 className="text-2xl font-semibold">{run.id}</h1>
          <div className="text-sm text-foreground/70 mt-1">
            {run.tool_id} • {run.status} • {run.mode}
          </div>
          <div className="text-xs text-foreground/50 mt-1">
            Started: {new Date(run.started_at).toLocaleString()}
          </div>
          {run.ended_at ? (
            <div className="text-xs text-foreground/50">
              Ended: {new Date(run.ended_at).toLocaleString()}
            </div>
          ) : null}
        </div>

        <div className="flex items-center gap-2">
          <Link
            to="/runs"
            className="px-3 py-2 rounded-md text-sm border border-border hover:border-primary"
          >
            Back
          </Link>
          {run.status === 'running' ? (
            <button
              className="px-3 py-2 rounded-md text-sm border border-destructive text-destructive hover:bg-destructive/10 disabled:opacity-50"
              onClick={stop}
              disabled={stopping}
            >
              {stopping ? 'Stopping…' : 'Stop'}
            </button>
          ) : null}
        </div>
      </div>

      {run.error ? <div className="text-destructive">{run.error}</div> : null}

      <section className="Panel p-4">
        <div className="font-semibold mb-2">Logs</div>
        <div className="h-[520px] overflow-auto rounded-md border border-border bg-background p-3 font-mono text-xs">
          {lines.length === 0 ? (
            <div className="text-foreground/60">(no logs yet)</div>
          ) : null}
          {lines.map((l, idx) => (
            <div key={idx} className="whitespace-pre-wrap break-words">
              <span className="text-foreground/50">[{l.stream}]</span> {l.line}
            </div>
          ))}
          <div ref={logEndRef} />
        </div>
      </section>
    </div>
  )
}
