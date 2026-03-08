import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'

import { api } from '../lib/api'
import type { ToolSummary } from '../lib/types'

export default function ToolsPage() {
  const [tools, setTools] = useState<ToolSummary[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.tools
      .list()
      .then(setTools)
      .catch((e) => setError(String(e)))
  }, [])

  if (error) {
    return <div className="text-destructive">{error}</div>
  }

  if (!tools) {
    return <div className="text-foreground/70">Loading tools…</div>
  }

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Tools</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
        {tools.map((t) => (
          <Link
            key={t.id}
            to={`/tools/${t.id}`}
            className="Panel p-2 hover:border-primary transition-colors"
          >
            <div className="flex items-center justify-between">
              <div>
                <div className="font-semibold">{t.display_name}</div>
                {t.description ? (
                  <div className="text-sm text-foreground/70 mt-1">{t.description}</div>
                ) : null}
              </div>
              <div className="text-xs text-foreground/60">{t.id}</div>
            </div>

            <div className="mt-3 flex flex-wrap gap-2 text-xs">
              {t.capabilities.supports_daemon ? (
                <span className="px-2 py-1 rounded bg-muted text-muted-foreground">daemon</span>
              ) : null}
              {t.capabilities.supports_purge_graph ? (
                <span className="px-2 py-1 rounded bg-muted text-muted-foreground">purge-graph</span>
              ) : null}
              {t.capabilities.supports_metrics ? (
                <span className="px-2 py-1 rounded bg-muted text-muted-foreground">metrics</span>
              ) : null}
            </div>
          </Link>
        ))}
      </div>
    </div>
  )
}
