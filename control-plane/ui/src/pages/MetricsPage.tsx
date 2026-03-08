import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'

import { api } from '../lib/api'
import type { ToolMetricsView, ToolSummary } from '../lib/types'

const PRIMARY_METRICS = [
  'runs',
  'failed_runs',
  'rows_fetched',
  'rows_written',
  'rows_deleted',
]

function formatMetricValue(v: number): string {
  if (!Number.isFinite(v)) return String(v)
  if (Math.abs(v - Math.round(v)) < 1e-9) {
    return new Intl.NumberFormat().format(Math.round(v))
  }
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(v)
}

export default function MetricsPage() {
  const [params, setParams] = useSearchParams()
  const selectedToolId = params.get('tool_id') ?? ''

  const [tools, setTools] = useState<ToolSummary[] | null>(null)
  const [metrics, setMetrics] = useState<ToolMetricsView | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)

  useEffect(() => {
    api.tools
      .list()
      .then((ts) => {
        setTools(ts)

        const hasCurrent = selectedToolId && ts.some((t) => t.id === selectedToolId)
        if (!hasCurrent && ts.length > 0) {
          const preferred = ts.find((t) => t.capabilities.supports_metrics) ?? ts[0]
          setParams({ tool_id: preferred.id }, { replace: true })
        }
      })
      .catch((e) => setError(String(e)))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const fetchMetrics = useCallback(async () => {
    if (!selectedToolId) return
    setRefreshing(true)
    setError(null)

    try {
      const data = await api.metrics.get(selectedToolId)
      setMetrics(data)
    } catch (e) {
      setError(String(e))
    } finally {
      setRefreshing(false)
    }
  }, [selectedToolId])

  useEffect(() => {
    if (!selectedToolId) {
      setMetrics(null)
      return
    }

    void fetchMetrics()
    const id = window.setInterval(() => {
      void fetchMetrics()
    }, 5000)
    return () => window.clearInterval(id)
  }, [fetchMetrics, selectedToolId])

  const hasMetricsSupport = metrics?.supports_metrics ?? false
  const snapshotTimestamp = metrics?.snapshot_timestamp ?? metrics?.fetched_at ?? null
  const snapshotSource = metrics?.snapshot_source ?? 'persisted'

  const overallRows = useMemo(() => {
    if (!metrics) return []
    const seen = new Set<string>()
    const out: Array<[string, number]> = []

    for (const key of PRIMARY_METRICS) {
      if (Object.prototype.hasOwnProperty.call(metrics.overall, key)) {
        out.push([key, metrics.overall[key]])
        seen.add(key)
      }
    }

    for (const [k, v] of Object.entries(metrics.overall).sort((a, b) => a[0].localeCompare(b[0]))) {
      if (seen.has(k)) continue
      out.push([k, v])
    }

    return out
  }, [metrics])

  const mappingRows = useMemo(() => {
    if (!metrics) return []
    return Object.entries(metrics.per_mapping).sort((a, b) => a[0].localeCompare(b[0]))
  }, [metrics])

  const mappingColumns = useMemo(() => {
    const cols = new Set<string>()
    for (const [, values] of mappingRows) {
      for (const key of Object.keys(values)) cols.add(key)
    }

    const ordered = Array.from(cols).sort((a, b) => a.localeCompare(b))
    ordered.sort((a, b) => {
      const ai = PRIMARY_METRICS.indexOf(a)
      const bi = PRIMARY_METRICS.indexOf(b)
      if (ai === -1 && bi === -1) return a.localeCompare(b)
      if (ai === -1) return 1
      if (bi === -1) return -1
      return ai - bi
    })

    return ordered
  }, [mappingRows])

  if (error) return <div className="text-destructive">{error}</div>
  if (!tools) return <div className="text-foreground/70">Loading…</div>

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-2">
        <h1 className="text-2xl font-semibold">Metrics</h1>
        <button
          className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary disabled:opacity-50"
          onClick={() => void fetchMetrics()}
          disabled={!selectedToolId || refreshing}
        >
          {refreshing ? 'Refreshing…' : 'Refresh now'}
        </button>
      </div>

      <div className="Panel p-2">
        <div className="text-sm text-foreground/70 mb-2">Select tool</div>
        <div className="flex flex-wrap gap-2">
          {tools.map((t) => (
            <button
              key={t.id}
              className={[
                'px-3 py-1.5 rounded-md text-sm border',
                selectedToolId === t.id ? 'border-primary text-primary' : 'border-border',
              ].join(' ')}
              onClick={() => setParams({ tool_id: t.id })}
            >
              {t.display_name}
            </button>
          ))}
        </div>
      </div>

      {!metrics ? (
        <div className="text-foreground/70">Loading metrics…</div>
      ) : (
        <>
          <section className="Panel p-2 space-y-2">
            <div className="flex items-center justify-between gap-2">
              <div>
                <div className="font-semibold">{metrics.display_name}</div>
                <div className="text-sm text-foreground/70">tool id: {metrics.tool_id}</div>
              </div>
              <div className="text-xs text-foreground/60 text-right">
                <div>
                  source: {snapshotSource}
                </div>
                <div>
                  snapshot: {snapshotTimestamp ? new Date(snapshotTimestamp).toLocaleString() : '—'}
                </div>
                <div>
                  run id: {metrics.snapshot_run_id || '—'}
                </div>
              </div>
            </div>

            <div className="text-sm text-foreground/70">
              Metrics are displayed from persisted control-plane snapshots.
            </div>

            {!hasMetricsSupport ? (
              <div className="text-sm text-foreground/70">
                This tool currently does not expose runtime metrics.
              </div>
            ) : null}

            {metrics.error ? (
              <div className="text-sm text-destructive">{metrics.error}</div>
            ) : null}

            {metrics.warnings.length > 0 ? (
              <ul className="list-disc pl-6 text-xs text-foreground/70 space-y-1">
                {metrics.warnings.map((w, idx) => (
                  <li key={idx}>{w}</li>
                ))}
              </ul>
            ) : null}
          </section>

          {hasMetricsSupport && !metrics.error ? (
            <>
              <section className="grid grid-cols-2 md:grid-cols-5 gap-2">
                {overallRows.map(([name, value]) => (
                  <div key={name} className="Panel p-2">
                    <div className="text-xs text-foreground/60">{name}</div>
                    <div className="text-xl font-semibold mt-1">{formatMetricValue(value)}</div>
                  </div>
                ))}
                {overallRows.length === 0 ? (
                  <div className="text-foreground/70 text-sm">No overall metrics yet.</div>
                ) : null}
              </section>

              <section className="Panel p-2">
                <div className="flex items-center justify-between mb-3">
                  <div className="font-semibold">Per-mapping metrics</div>
                  {selectedToolId ? (
                    <Link
                      to={`/tools/${selectedToolId}`}
                      className="px-3 py-1.5 rounded-md text-xs border border-border hover:border-primary"
                    >
                      Open tool
                    </Link>
                  ) : null}
                </div>

                {mappingRows.length === 0 ? (
                  <div className="text-sm text-foreground/70">No mapping metrics yet.</div>
                ) : (
                  <div className="overflow-auto rounded-md border border-border">
                    <table className="min-w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr>
                          <th className="text-left px-2 py-1 border-b border-border">mapping</th>
                          {mappingColumns.map((c) => (
                            <th key={c} className="text-left px-2 py-1 border-b border-border">
                              {c}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {mappingRows.map(([mapping, values]) => (
                          <tr key={mapping}>
                            <td className="px-2 py-1 border-b border-border">{mapping}</td>
                            {mappingColumns.map((c) => (
                              <td key={c} className="px-2 py-1 border-b border-border">
                                {values[c] === undefined ? '—' : formatMetricValue(values[c])}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </section>
            </>
          ) : null}
        </>
      )}
    </div>
  )
}
