import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'

import { api } from '../lib/api'
import type { ConfigRecord, ConfigStateInfo, RunMode, ToolManifest } from '../lib/types'

export default function ToolDetailPage() {
  const { toolId } = useParams()
  const navigate = useNavigate()
  const [tool, setTool] = useState<ToolManifest | null>(null)
  const [configs, setConfigs] = useState<ConfigRecord[] | null>(null)
  const [selectedConfigId, setSelectedConfigId] = useState<string>('')
  const [configState, setConfigState] = useState<ConfigStateInfo | null>(null)
  const [mode, setMode] = useState<RunMode>('one_shot')
  const [intervalSecs, setIntervalSecs] = useState<number>(60)
  const [purgeGraph, setPurgeGraph] = useState(false)
  const [purgeMappings, setPurgeMappings] = useState<string>('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [clearingState, setClearingState] = useState(false)

  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const [importFileName, setImportFileName] = useState<string>('')

  useEffect(() => {
    if (!toolId) return

    setError(null)
    api.tools
      .get(toolId)
      .then(setTool)
      .catch((e) => setError(String(e)))

    api.configs
      .list(toolId)
      .then((cs) => {
        setConfigs(cs)
        if (cs.length > 0) setSelectedConfigId(cs[0].id)
      })
      .catch((e) => setError(String(e)))
  }, [toolId])

  useEffect(() => {
    if (!selectedConfigId) {
      setConfigState(null)
      return
    }

    api.configs
      .state(selectedConfigId)
      .then(setConfigState)
      .catch(() => setConfigState(null))
  }, [selectedConfigId])

  const supportsDaemon = tool?.capabilities.supports_daemon ?? false
  const supportsPurgeGraph = tool?.capabilities.supports_purge_graph ?? false
  const supportsPurgeMapping = tool?.capabilities.supports_purge_mapping ?? false

  const purgeMappingList = useMemo(() => {
    return purgeMappings
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  }, [purgeMappings])

  async function importConfigFromFile(file: File) {
    if (!toolId) return

    setBusy(true)
    setError(null)

    try {
      const ext = file.name.split('.').pop()?.toLowerCase()
      if (ext && !['yaml', 'yml', 'json'].includes(ext)) {
        throw new Error('Only .yaml/.yml/.json files are supported')
      }

      const text = await file.text()
      const baseName = file.name.replace(/\.(yaml|yml|json)$/i, '')

      const cfg = await api.configs.create({
        tool_id: toolId,
        name: baseName,
        content: text,
      })

      setImportFileName(file.name)

      // Update local list and select the imported config.
      setConfigs((prev) => (prev ? [cfg, ...prev] : [cfg]))
      setSelectedConfigId(cfg.id)
    } catch (e) {
      setError(String(e))
    } finally {
      setBusy(false)
    }
  }

  async function clearState() {
    if (!selectedConfigId) return

    const ok = window.confirm(
      'Clear incremental state for this config?\n\nThis will delete the state file used for watermarks, so the next incremental run will behave like a full backfill.',
    )
    if (!ok) return

    setClearingState(true)
    setError(null)

    try {
      await api.configs.clearState(selectedConfigId)
      const next = await api.configs.state(selectedConfigId)
      setConfigState(next)
    } catch (e) {
      setError(String(e))
    } finally {
      setClearingState(false)
    }
  }

  async function startRun() {
    if (!toolId) return
    if (!selectedConfigId) {
      setError('Select a config first')
      return
    }

    setBusy(true)
    setError(null)

    try {
      const run = await api.runs.start({
        tool_id: toolId,
        config_id: selectedConfigId,
        mode,
        daemon_interval_secs: mode === 'daemon' ? intervalSecs : undefined,
        purge_graph: supportsPurgeGraph ? purgeGraph : undefined,
        purge_mappings: supportsPurgeMapping ? purgeMappingList : undefined,
      })

      navigate(`/runs/${run.id}`)
    } catch (e) {
      setError(String(e))
    } finally {
      setBusy(false)
    }
  }

  if (error) {
    return <div className="text-destructive">{error}</div>
  }

  if (!tool || !configs) {
    return <div className="text-foreground/70">Loading…</div>
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="text-sm text-foreground/60">Tool</div>
          <h1 className="text-2xl font-semibold">{tool.displayName}</h1>
          <div className="text-sm text-foreground/70 mt-1">{tool.description}</div>
          <div className="text-xs text-foreground/50 mt-2">Working dir: {tool.workingDir}</div>
        </div>

        <div className="flex items-center gap-2">
          <Link
            to={`/metrics?tool_id=${encodeURIComponent(tool.id)}`}
            className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
          >
            Metrics
          </Link>
          <Link
            to={`/configs/new?tool_id=${encodeURIComponent(tool.id)}`}
            className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
          >
            New config
          </Link>
        </div>
      </div>

      <section className="Panel p-2 space-y-4">
        <div className="font-semibold">Start run</div>

        <div className="flex items-center justify-between gap-2">
          <div>
            <div className="text-sm text-foreground/70">Import config from file</div>
            <div className="text-xs text-foreground/50">
              {importFileName
                ? `Imported: ${importFileName}`
                : 'Choose a local .yaml/.yml/.json to create a config and select it'}
            </div>
          </div>

          <div className="flex items-center gap-2">
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml,.json"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0]
                if (!f) return
                void importConfigFromFile(f)
                // Allow re-selecting the same file.
                e.currentTarget.value = ''
              }}
            />
            <button
              type="button"
              className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary disabled:opacity-50"
              onClick={() => fileInputRef.current?.click()}
              disabled={busy}
            >
              Choose file
            </button>
          </div>
        </div>

        <label className="block text-sm">
          <div className="text-foreground/70 mb-1">Config</div>
          <select
            className="w-full bg-background border border-border rounded-md px-2 py-1"
            value={selectedConfigId}
            onChange={(e) => setSelectedConfigId(e.target.value)}
          >
            {configs.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name}
              </option>
            ))}
          </select>
        </label>

        {configState ? (
          <div className="rounded-md border border-border bg-background p-2 text-sm">
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="font-semibold">Incremental state</div>
                <div className="text-foreground/70 mt-1">
                  Backend: {configState.backend || '(not configured)'}
                </div>
              </div>

              <button
                type="button"
                className="px-2 py-1 rounded-md text-xs border border-destructive text-destructive hover:bg-destructive/10 disabled:opacity-50"
                onClick={clearState}
                disabled={clearingState || configState.backend !== 'file'}
                title={
                  configState.backend !== 'file'
                    ? 'Clear state is available only for file-backed state'
                    : 'Delete the state file used for watermarks'
                }
              >
                {clearingState ? 'Clearing…' : 'Clear state'}
              </button>
            </div>

            {configState.backend === 'file' ? (
              <div className="space-y-1 mt-2">
                <div className="text-foreground/70">
                  State file:{' '}
                  <code className="text-xs">
                    {configState.resolved_path || configState.file_path || 'state.json'}
                  </code>{' '}
                  <span className="text-xs text-foreground/60">
                    ({configState.exists ? 'present' : 'missing'})
                  </span>
                </div>

                <div className="text-foreground/70">
                  Last watermark:{' '}
                  <span className="text-foreground">
                    {configState.last_watermark
                      ? new Date(configState.last_watermark).toLocaleString()
                      : '—'}
                  </span>
                </div>

                {configState.warning ? (
                  <div className="text-xs text-foreground/60">{configState.warning}</div>
                ) : null}
              </div>
            ) : (
              <div className="text-xs text-foreground/60 mt-2">
                Watermark inspection is currently shown only for file-backed state.
              </div>
            )}
          </div>
        ) : null}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          <label className="block text-sm">
            <div className="text-foreground/70 mb-1">Mode</div>
            <select
              className="w-full bg-background border border-border rounded-md px-2 py-1"
              value={mode}
              onChange={(e) => setMode(e.target.value as RunMode)}
            >
              <option value="one_shot">one-shot</option>
              {supportsDaemon ? <option value="daemon">daemon</option> : null}
            </select>
          </label>

          {supportsDaemon && mode === 'daemon' ? (
            <label className="block text-sm">
              <div className="text-foreground/70 mb-1">Interval (secs)</div>
              <input
                type="number"
                className="w-full bg-background border border-border rounded-md px-2 py-1"
                value={intervalSecs}
                onChange={(e) => setIntervalSecs(Number(e.target.value))}
                min={1}
              />
            </label>
          ) : null}
        </div>

        {supportsPurgeGraph ? (
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={purgeGraph}
              onChange={(e) => setPurgeGraph(e.target.checked)}
            />
            Purge entire graph before loading
          </label>
        ) : null}

        {supportsPurgeMapping ? (
          <label className="block text-sm">
            <div className="text-foreground/70 mb-1">
              Purge mappings (comma-separated)
            </div>
            <input
              className="w-full bg-background border border-border rounded-md px-2 py-1"
              placeholder="customers, customer_orders"
              value={purgeMappings}
              onChange={(e) => setPurgeMappings(e.target.value)}
            />
          </label>
        ) : null}

        <div className="flex items-center gap-2">
          <button
            className="px-2 py-1 rounded-md text-sm border border-primary text-primary hover:bg-primary/10 disabled:opacity-50"
            onClick={startRun}
            disabled={busy}
          >
            {busy ? 'Starting…' : 'Start'}
          </button>

          <Link
            to="/runs"
            className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
          >
            View runs
          </Link>
        </div>
      </section>

      <section className="Panel p-2">
        <div className="font-semibold mb-2">Example configs</div>
        {tool.config.examples.length === 0 ? (
          <div className="text-sm text-foreground/70">No examples listed.</div>
        ) : (
          <ul className="list-disc pl-6 text-sm text-foreground/80">
            {tool.config.examples.map((p) => (
              <li key={p}>
                <code className="text-xs">{p}</code>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  )
}
