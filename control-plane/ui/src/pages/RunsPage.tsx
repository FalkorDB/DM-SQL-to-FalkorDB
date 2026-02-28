import { useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'

import { api } from '../lib/api'
import type { RunRecord, ToolSummary } from '../lib/types'

export default function RunsPage() {
  const [params] = useSearchParams()
  const toolId = params.get('tool_id') ?? undefined

  const [tools, setTools] = useState<ToolSummary[] | null>(null)
  const [runs, setRuns] = useState<RunRecord[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.tools
      .list()
      .then(setTools)
      .catch((e) => setError(String(e)))
  }, [])

  useEffect(() => {
    api.runs
      .list(toolId)
      .then(setRuns)
      .catch((e) => setError(String(e)))
  }, [toolId])

  if (error) return <div className="text-destructive">{error}</div>
  if (!tools || !runs) return <div className="text-foreground/70">Loading…</div>

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Runs</h1>

      <div className="Panel p-4">
        <div className="text-sm text-foreground/70 mb-2">Filter by tool</div>
        <div className="flex flex-wrap gap-2">
          <Link
            to="/runs"
            className={[
              'px-3 py-1.5 rounded-md text-sm border',
              toolId ? 'border-border' : 'border-primary text-primary',
            ].join(' ')}
          >
            All
          </Link>
          {tools.map((t) => (
            <Link
              key={t.id}
              to={`/runs?tool_id=${encodeURIComponent(t.id)}`}
              className={[
                'px-3 py-1.5 rounded-md text-sm border',
                toolId === t.id ? 'border-primary text-primary' : 'border-border',
              ].join(' ')}
            >
              {t.display_name}
            </Link>
          ))}
        </div>
      </div>

      <div className="space-y-2">
        {runs.map((r) => (
          <Link
            key={r.id}
            to={`/runs/${r.id}`}
            className="Panel p-4 block hover:border-primary transition-colors"
          >
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="font-semibold">{r.tool_id}</div>
                <div className="text-sm text-foreground/70">
                  {r.status} • {r.mode} • {new Date(r.started_at).toLocaleString()}
                </div>
                {r.error ? (
                  <div className="text-sm text-destructive mt-1">{r.error}</div>
                ) : null}
              </div>
              <div className="text-xs text-foreground/50">{r.id}</div>
            </div>
          </Link>
        ))}

        {runs.length === 0 ? (
          <div className="text-foreground/70">No runs yet.</div>
        ) : null}
      </div>
    </div>
  )
}
