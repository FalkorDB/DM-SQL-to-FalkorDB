import { useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'

import { api } from '../lib/api'
import type { ConfigRecord, ToolSummary } from '../lib/types'

export default function ConfigsPage() {
  const [params] = useSearchParams()
  const toolId = params.get('tool_id') ?? undefined

  const [tools, setTools] = useState<ToolSummary[] | null>(null)
  const [configs, setConfigs] = useState<ConfigRecord[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.tools
      .list()
      .then(setTools)
      .catch((e) => setError(String(e)))
  }, [])

  useEffect(() => {
    api.configs
      .list(toolId)
      .then(setConfigs)
      .catch((e) => setError(String(e)))
  }, [toolId])

  if (error) return <div className="text-destructive">{error}</div>
  if (!tools || !configs) return <div className="text-foreground/70">Loading…</div>

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Configs</h1>
        <Link
          to={toolId ? `/configs/new?tool_id=${encodeURIComponent(toolId)}` : '/configs/new'}
          className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
        >
          New config
        </Link>
      </div>

      <div className="Panel p-2">
        <div className="text-sm text-foreground/70 mb-2">Filter by tool</div>
        <div className="flex flex-wrap gap-2">
          <Link
            to="/configs"
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
              to={`/configs?tool_id=${encodeURIComponent(t.id)}`}
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
        {configs.map((c) => (
          <Link
            key={c.id}
            to={`/configs/${c.id}`}
            className="Panel p-2 block hover:border-primary transition-colors"
          >
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="font-semibold">{c.name}</div>
                <div className="text-sm text-foreground/60">tool: {c.tool_id}</div>
              </div>
              <div className="text-xs text-foreground/50">{c.id}</div>
            </div>
          </Link>
        ))}

        {configs.length === 0 ? (
          <div className="text-foreground/70">No configs yet.</div>
        ) : null}
      </div>
    </div>
  )
}
