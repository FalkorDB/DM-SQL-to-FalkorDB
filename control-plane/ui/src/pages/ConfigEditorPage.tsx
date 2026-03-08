import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import '@falkordb/canvas'

import CodeMirror from '@uiw/react-codemirror'
import { json } from '@codemirror/lang-json'
import { yaml } from '@codemirror/lang-yaml'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView, placeholder } from '@codemirror/view'

import { api } from '../lib/api'
import type { CanvasLink, CanvasNode, ToolSummary } from '../lib/types'

type ConfigSyntax = 'auto' | 'yaml' | 'json'
type ViewerTab = 'config' | 'schema' | 'template' | 'canvas'
type CanvasGraphData = { nodes: CanvasNode[]; links: CanvasLink[] }
type FalkorDBCanvasElement = HTMLElement & {
  setData: (data: CanvasGraphData) => void
  setConfig: (config: {
    backgroundColor?: string
    foregroundColor?: string
    captionsKeys?: string[]
    showPropertyKeyPrefix?: boolean
  }) => void
  zoomToFit?: (paddingMultiplier?: number) => void
}

function inferSyntax(text: string): Exclude<ConfigSyntax, 'auto'> {
  const t = text.trim()
  if (!t) return 'yaml'

  const first = t[0]
  if (first === '{' || first === '[' || '"0123456789tfn-'.includes(first)) {
    try {
      JSON.parse(t)
      return 'json'
    } catch {
      // ignore
    }
  }

  return 'yaml'
}

function supportsScaffold(toolId: string): boolean {
  return ['mysql', 'mariadb', 'sqlserver', 'postgres', 'snowflake', 'clickhouse', 'databricks'].includes(toolId)
}

export default function ConfigEditorPage() {
  const { configId } = useParams()
  const isNew = configId === 'new'

  const [params] = useSearchParams()
  const preToolId = params.get('tool_id') ?? ''

  const [tools, setTools] = useState<ToolSummary[] | null>(null)
  const [name, setName] = useState('')
  const [toolId, setToolId] = useState(preToolId)
  const [content, setContent] = useState('')
  const [syntax, setSyntax] = useState<ConfigSyntax>('auto')
  const [activeTab, setActiveTab] = useState<ViewerTab>('config')
  const [schemaSummary, setSchemaSummary] = useState('')
  const [generatedTemplate, setGeneratedTemplate] = useState('')
  const [canvasData, setCanvasData] = useState<CanvasGraphData | null>(null)
  const [canvasWarnings, setCanvasWarnings] = useState<string[]>([])
  const [canvasSource, setCanvasSource] = useState<'config' | 'template' | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [generatingTemplate, setGeneratingTemplate] = useState(false)
  const [previewingSchema, setPreviewingSchema] = useState(false)
  const [previewingGraph, setPreviewingGraph] = useState(false)

  const [isDark, setIsDark] = useState(() =>
    document.documentElement.classList.contains('dark'),
  )

  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const canvasRef = useRef<FalkorDBCanvasElement | null>(null)
  const nav = useNavigate()

  useEffect(() => {
    api.tools
      .list()
      .then((ts) => {
        setTools(ts)
        if (!toolId && ts.length > 0) setToolId(ts[0].id)
      })
      .catch((e) => setError(String(e)))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (!configId || isNew) return
    api.configs
      .get(configId)
      .then((cfg) => {
        setName(cfg.name)
        setToolId(cfg.tool_id)
        setContent(cfg.content)
        setSchemaSummary('')
        setGeneratedTemplate('')
        setCanvasData(null)
        setCanvasWarnings([])
        setCanvasSource(null)
        setActiveTab('config')
        setSyntax('auto')
      })
      .catch((e) => setError(String(e)))
  }, [configId, isNew])

  useEffect(() => {
    const root = document.documentElement
    const obs = new MutationObserver(() => {
      setIsDark(root.classList.contains('dark'))
    })
    obs.observe(root, { attributes: true, attributeFilter: ['class'] })
    return () => obs.disconnect()
  }, [])

  useEffect(() => {
    if (activeTab !== 'canvas' || !canvasData) return
    const canvas = canvasRef.current
    if (!canvas) return

    canvas.setData(canvasData)
    canvas.setConfig({
      backgroundColor: isDark ? '#0B1220' : '#FFFFFF',
      foregroundColor: isDark ? '#E5E7EB' : '#111827',
      captionsKeys: ['node_label', 'mapping_name'],
      showPropertyKeyPrefix: false,
    })

    if (typeof canvas.zoomToFit === 'function' && canvasData.nodes.length > 0) {
      window.setTimeout(() => canvas.zoomToFit?.(1.05), 50)
    }
  }, [activeTab, canvasData, isDark])

  const title = isNew ? 'New config' : 'Edit config'

  async function loadFromFile(file: File) {
    setError(null)
    const ext = file.name.split('.').pop()?.toLowerCase()
    if (ext && !['yaml', 'yml', 'json'].includes(ext)) {
      setError('Only .yaml/.yml/.json files are supported')
      return
    }

    try {
      const text = await file.text()
      setContent(text)
      setActiveTab('config')
      if (ext === 'json') setSyntax('json')
      else if (ext === 'yaml' || ext === 'yml') setSyntax('yaml')
      else setSyntax('auto')

      if (isNew && name.trim() === '') {
        const base = file.name.replace(/\.(yaml|yml|json)$/i, '')
        setName(base)
      }
    } catch (e) {
      setError(String(e))
    }
  }

  async function save() {
    setBusy(true)
    setError(null)
    try {
      if (!toolId) throw new Error('tool_id is required')
      if (!name) throw new Error('name is required')

      if (isNew) {
        const cfg = await api.configs.create({ tool_id: toolId, name, content })
        nav(`/configs/${cfg.id}`)
      } else if (configId) {
        const cfg = await api.configs.update(configId, { name, content })
        nav(`/configs/${cfg.id}`)
      }
    } catch (e) {
      setError(String(e))
    } finally {
      setBusy(false)
    }
  }

  async function generateTemplateFromSchema() {
    setError(null)
    if (!toolId) return setError('tool_id is required')
    if (!supportsScaffold(toolId)) {
      return setError(`Tool '${toolId}' does not support scaffold template generation`)
    }
    if (content.trim() === '') {
      return setError('Provide a base config with source connection details before generating template')
    }

    setGeneratingTemplate(true)
    try {
      const res = await api.tools.generateScaffoldTemplate(toolId, {
        config_content: content,
        include_schema_summary: false,
      })
      setGeneratedTemplate(res.template_yaml)
      setActiveTab('template')
      if (isNew && name.trim() === '') setName(`${toolId}_generated_template`)
    } catch (e) {
      setError(String(e))
    } finally {
      setGeneratingTemplate(false)
    }
  }

  async function previewExtractedSchema() {
    setError(null)
    if (!toolId) return setError('tool_id is required')
    if (!supportsScaffold(toolId)) return setError(`Tool '${toolId}' does not support schema preview`)
    if (content.trim() === '') {
      return setError('Provide a base config with source connection details before previewing schema')
    }

    setPreviewingSchema(true)
    try {
      const res = await api.tools.generateScaffoldTemplate(toolId, {
        config_content: content,
        include_schema_summary: true,
      })
      if (!res.schema_summary || res.schema_summary.trim() === '') {
        return setError('No schema summary was returned by the scaffold endpoint')
      }
      setSchemaSummary(res.schema_summary)
      setActiveTab('schema')
    } catch (e) {
      setError(String(e))
    } finally {
      setPreviewingSchema(false)
    }
  }

  async function previewGraphSchema() {
    setError(null)
    if (!toolId) return setError('tool_id is required')
    if (content.trim() === '') {
      return setError('Provide a config before previewing graph schema')
    }

    setPreviewingGraph(true)
    try {
      const res = await api.tools.generateSchemaGraphPreview(toolId, {
        config_content: content,
      })
      setCanvasData(res.canvas_data)
      setCanvasWarnings(res.warnings ?? [])
      setCanvasSource(res.source)
      setActiveTab('canvas')
    } catch (e) {
      setError(String(e))
    } finally {
      setPreviewingGraph(false)
    }
  }

  const effectiveSyntax = syntax === 'auto' ? inferSyntax(content) : syntax
  const editorExtensions = useMemo(
    () => [
      effectiveSyntax === 'json' ? json() : yaml(),
      EditorView.lineWrapping,
      placeholder('paste YAML/JSON here'),
      EditorView.theme({
        '&': { fontSize: '12px' },
        '.cm-content': {
          fontFamily:
            'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
        },
      }),
    ],
    [effectiveSyntax],
  )
  const schemaSyntax = useMemo(() => inferSyntax(schemaSummary), [schemaSummary])
  const schemaViewerExtensions = useMemo(
    () => [
      schemaSyntax === 'json' ? json() : yaml(),
      EditorView.lineWrapping,
      EditorView.editable.of(false),
      placeholder("No schema extracted yet. Click 'Preview schema'."),
      EditorView.theme({
        '&': { fontSize: '12px' },
        '.cm-content': {
          fontFamily:
            'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
        },
      }),
    ],
    [schemaSyntax],
  )
  const templateSyntax = useMemo(() => inferSyntax(generatedTemplate), [generatedTemplate])
  const templateViewerExtensions = useMemo(
    () => [
      templateSyntax === 'json' ? json() : yaml(),
      EditorView.lineWrapping,
      EditorView.editable.of(false),
      placeholder("No template generated yet. Click 'Generate template'."),
      EditorView.theme({
        '&': { fontSize: '12px' },
        '.cm-content': {
          fontFamily:
            'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
        },
      }),
    ],
    [templateSyntax],
  )


  if (error) return <div className="text-destructive">{error}</div>
  if (!tools) return <div className="text-foreground/70">Loading…</div>

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">{title}</h1>
        <div className="flex items-center gap-2">
          <Link
            to={toolId ? `/tools/${toolId}` : '/tools'}
            className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
          >
            Back
          </Link>
          <button
            className="px-2 py-1 rounded-md text-sm border border-primary text-primary hover:bg-primary/10 disabled:opacity-50"
            onClick={save}
            disabled={busy}
          >
            {busy ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>

      <div className="Panel p-2 space-y-4">
        <label className="block text-sm">
          <div className="text-foreground/70 mb-1">Tool</div>
          <select
            disabled={!isNew}
            className="w-full bg-background border border-border rounded-md px-2 py-1 disabled:opacity-60"
            value={toolId}
            onChange={(e) => setToolId(e.target.value)}
          >
            {tools.map((t) => (
              <option key={t.id} value={t.id}>
                {t.display_name}
              </option>
            ))}
          </select>
        </label>

        <div className="flex items-center justify-between gap-2">
          <div className="text-sm text-foreground/70">
            Load an existing config from your local filesystem.
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary disabled:opacity-50"
              onClick={() => void previewGraphSchema()}
              disabled={previewingGraph}
              title="Preview graph schema from current config mappings"
            >
              {previewingGraph ? 'Rendering graph…' : 'Preview graph'}
            </button>
            <button
              type="button"
              className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary disabled:opacity-50"
              onClick={() => void previewExtractedSchema()}
              disabled={previewingSchema || !supportsScaffold(toolId)}
              title={
                supportsScaffold(toolId)
                  ? 'Preview extracted source schema'
                  : 'Schema preview is supported for mysql/mariadb/sqlserver/postgres/snowflake/clickhouse/databricks'
              }
            >
              {previewingSchema ? 'Extracting schema…' : 'Preview schema'}
            </button>
            <button
              type="button"
              className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary disabled:opacity-50"
              onClick={() => void generateTemplateFromSchema()}
              disabled={generatingTemplate || !supportsScaffold(toolId)}
              title={
                supportsScaffold(toolId)
                  ? 'Generate template from source schema'
                  : 'Template generation is supported for mysql/mariadb/sqlserver/postgres/snowflake/clickhouse/databricks'
              }
            >
              {generatingTemplate ? 'Generating template…' : 'Generate template'}
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml,.json"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0]
                if (!f) return
                void loadFromFile(f)
                e.currentTarget.value = ''
              }}
            />
            <button
              type="button"
              className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
              onClick={() => fileInputRef.current?.click()}
            >
              Choose file
            </button>
          </div>
        </div>

        <div className="flex items-end justify-between gap-2">
          <div className="flex items-center gap-2">
            <button
              type="button"
              className={`px-2 py-1 rounded-md text-xs border ${
                activeTab === 'config'
                  ? 'border-primary text-primary'
                  : 'border-border hover:border-primary'
              }`}
              onClick={() => setActiveTab('config')}
            >
              Config file
            </button>
            <button
              type="button"
              className={`px-2 py-1 rounded-md text-xs border ${
                activeTab === 'schema'
                  ? 'border-primary text-primary'
                  : 'border-border hover:border-primary'
              }`}
              onClick={() => setActiveTab('schema')}
            >
              Extracted schema
            </button>
            <button
              type="button"
              className={`px-2 py-1 rounded-md text-xs border ${
                activeTab === 'template'
                  ? 'border-primary text-primary'
                  : 'border-border hover:border-primary'
              }`}
              onClick={() => setActiveTab('template')}
            >
              Generated template
            </button>
            <button
              type="button"
              className={`px-2 py-1 rounded-md text-xs border ${
                activeTab === 'canvas'
                  ? 'border-primary text-primary'
                  : 'border-border hover:border-primary'
              }`}
              onClick={() => setActiveTab('canvas')}
            >
              Graph visualization
            </button>
          </div>
          <label className="block text-sm min-w-80">
            <div className="text-foreground/70 mb-1">Name</div>
            <input
              className="w-full bg-background border border-border rounded-md px-2 py-1"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </label>
        </div>

        {activeTab === 'config' ? (
          <label className="block text-sm">
            <div className="flex items-center justify-between mb-1">
              <div className="text-foreground/70">Config file</div>
              <select
                className="bg-background border border-border rounded-md px-2 py-1 text-xs"
                value={syntax}
                onChange={(e) => setSyntax(e.target.value as ConfigSyntax)}
                title="Syntax highlighting"
              >
                <option value="auto">Auto</option>
                <option value="yaml">YAML</option>
                <option value="json">JSON</option>
              </select>
            </div>
            <div className="border border-border rounded-md overflow-hidden">
              <CodeMirror
                value={content}
                height="420px"
                theme={isDark ? oneDark : undefined}
                extensions={editorExtensions}
                onChange={(value) => setContent(value)}
                basicSetup={{
                  lineNumbers: true,
                  highlightActiveLine: false,
                  highlightActiveLineGutter: false,
                }}
              />
            </div>
          </label>
        ) : null}

        {activeTab === 'schema' ? (
          <label className="block text-sm">
            <div className="flex items-center justify-between mb-1">
              <div className="text-foreground/70">Extracted schema</div>
              <button
                type="button"
                className="px-2 py-1 rounded-md text-xs border border-border hover:border-primary"
                onClick={() => setSchemaSummary('')}
              >
                Clear
              </button>
            </div>
            <div className="border border-border rounded-md overflow-hidden">
              <CodeMirror
                value={schemaSummary}
                height="420px"
                theme={isDark ? oneDark : undefined}
                extensions={schemaViewerExtensions}
                editable={false}
                basicSetup={{
                  lineNumbers: true,
                  highlightActiveLine: false,
                  highlightActiveLineGutter: false,
                }}
              />
            </div>
          </label>
        ) : null}

        {activeTab === 'template' ? (
          <label className="block text-sm">
            <div className="flex items-center justify-between mb-1">
              <div className="text-foreground/70">Generated template</div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  className="px-2 py-1 rounded-md text-xs border border-border hover:border-primary"
                  onClick={() => {
                    if (!generatedTemplate) return
                    setContent(generatedTemplate)
                    setSyntax('yaml')
                    setActiveTab('config')
                  }}
                  disabled={!generatedTemplate}
                >
                  Use as config
                </button>
                <button
                  type="button"
                  className="px-2 py-1 rounded-md text-xs border border-border hover:border-primary"
                  onClick={() => setGeneratedTemplate('')}
                >
                  Clear
                </button>
              </div>
            </div>
            <div className="border border-border rounded-md overflow-hidden">
              <CodeMirror
                value={generatedTemplate}
                height="420px"
                theme={isDark ? oneDark : undefined}
                extensions={templateViewerExtensions}
                editable={false}
                basicSetup={{
                  lineNumbers: true,
                  highlightActiveLine: false,
                  highlightActiveLineGutter: false,
                }}
              />
            </div>
          </label>
        ) : null}

        {activeTab === 'canvas' ? (
          <label className="block text-sm">
            <div className="flex items-center justify-between mb-1">
              <div className="text-foreground/70">
                Graph visualization
                {canvasSource ? (
                  <span className="ml-2 text-[11px] text-foreground/60">Source: {canvasSource}</span>
                ) : null}
              </div>
              <button
                type="button"
                className="px-2 py-1 rounded-md text-xs border border-border hover:border-primary"
                onClick={() => {
                  setCanvasData(null)
                  setCanvasWarnings([])
                  setCanvasSource(null)
                }}
              >
                Clear
              </button>
            </div>
            {canvasWarnings.length > 0 ? (
              <div className="mb-2 p-2 rounded-md border border-border bg-background/40 text-xs space-y-1">
                {canvasWarnings.map((warning) => (
                  <div key={warning} className="text-foreground/80">
                    {warning}
                  </div>
                ))}
              </div>
            ) : null}
            <div className="border border-border rounded-md overflow-hidden h-[420px]">
              {canvasData && (canvasData.nodes.length > 0 || canvasData.links.length > 0) ? (
                <falkordb-canvas ref={canvasRef} style={{ width: '100%', height: '100%' }} />
              ) : (
                <div className="h-full w-full flex items-center justify-center text-foreground/60 text-sm px-4 text-center">
                  No graph visualization available yet. Click 'Preview graph'.
                </div>
              )}
            </div>
          </label>
        ) : null}
      </div>
    </div>
  )
}
