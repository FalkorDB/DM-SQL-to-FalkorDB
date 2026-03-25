import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
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
    onNodeClick?: (node: unknown, event: MouseEvent) => void
    onNodeRightClick?: (node: unknown, event: MouseEvent) => void
    onLinkClick?: (link: unknown, event: MouseEvent) => void
    onLinkRightClick?: (link: unknown, event: MouseEvent) => void
    onBackgroundClick?: (event: MouseEvent) => void
    onBackgroundRightClick?: (event: MouseEvent) => void
  }) => void
  zoomToFit?: (paddingMultiplier?: number) => void
}
type MappingPropertySpec = { name: string; column?: string | null; graphType?: string | null }
type MatchOnSpec = { column?: string | null; property?: string | null; graphType?: string | null }
type NodePropertyView = {
  id: number
  mappingName: string
  labels: string[]
  sourceTable?: string
  keyColumn?: string
  keyProperty?: string
  keyType?: string
  properties: MappingPropertySpec[]
}
type EdgePropertyView = {
  id: number
  mappingName: string
  relationship: string
  fromMapping?: string
  toMapping?: string
  sourceTable?: string
  properties: MappingPropertySpec[]
  fromMatchOn: MatchOnSpec[]
  toMatchOn: MatchOnSpec[]
}
type CanvasPopupState = {
  x: number
  y: number
  node?: NodePropertyView
  edge?: EdgePropertyView
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Record<string, unknown>
}

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

function mapSourceTypeToGraphType(sourceType?: string): string | undefined {
  if (!sourceType) return undefined
  const tokens = sourceType
    .toLowerCase()
    .replace(/["`[\]]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean)

  if (tokens.some((token) => token === 'vector' || token === 'array' || token.startsWith('vector'))) {
    return 'vector'
  }
  if (tokens.some((token) => token === 'bool' || token === 'boolean' || token === 'bit')) {
    return 'boolean'
  }
  if (
    tokens.some(
      (token) =>
        token === 'money' ||
        token === 'real' ||
        token === 'decimal' ||
        token === 'numeric' ||
        token === 'number' ||
        token.startsWith('int') ||
        token.endsWith('int') ||
        token.startsWith('uint') ||
        token.startsWith('float') ||
        token.startsWith('double') ||
        token.startsWith('serial'),
    )
  ) {
    return 'number'
  }
  return 'string'
}

function resolveGraphType(primaryType?: string, fallbackSourceType?: string): string | undefined {
  const normalizedPrimary = primaryType?.trim().toLowerCase()
  if (normalizedPrimary && ['string', 'number', 'boolean', 'vector'].includes(normalizedPrimary)) {
    return normalizedPrimary
  }
  return mapSourceTypeToGraphType(primaryType ?? fallbackSourceType)
}

function parsePropertySpecs(value: unknown): MappingPropertySpec[] {
  if (!Array.isArray(value)) return []
  const out: MappingPropertySpec[] = []
  for (const item of value) {
    const record = asRecord(item)
    if (!record) continue
    const name = asString(record.name)
    if (!name) continue
    out.push({
      name,
      column: asString(record.column),
      graphType: resolveGraphType(asString(record.graph_type), asString(record.sql_type)),
    })
  }
  return out
}

function parseMatchOnSpecs(value: unknown): MatchOnSpec[] {
  if (!Array.isArray(value)) return []
  const out: MatchOnSpec[] = []
  for (const item of value) {
    const record = asRecord(item)
    if (!record) continue
    out.push({
      column: asString(record.column),
      property: asString(record.property),
      graphType: resolveGraphType(asString(record.graph_type), asString(record.sql_type)),
    })
  }
  return out
}

function parseLabels(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.filter((item): item is string => typeof item === 'string')
}

function parseNumericId(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return null
}

function clampPopupPosition(clientX: number, clientY: number) {
  const POPUP_WIDTH = 380
  const POPUP_HEIGHT = 420
  const PADDING = 8
  const x = Math.max(
    PADDING,
    Math.min(clientX + 12, window.innerWidth - POPUP_WIDTH - PADDING),
  )
  const y = Math.max(
    PADDING,
    Math.min(clientY + 12, window.innerHeight - POPUP_HEIGHT - PADDING),
  )
  return { x, y }
}

function parseNodePropertyViewFromPayload(payload: unknown): NodePropertyView | null {
  const node = asRecord(payload)
  if (!node) return null
  const id = parseNumericId(node.id)
  if (id == null) return null
  const data = asRecord(node.data)
  return {
    id,
    mappingName: asString(data?.mapping_name) ?? `node_${id}`,
    labels: parseLabels(node.labels),
    sourceTable: asString(data?.source_table),
    keyColumn: asString(data?.key_column),
    keyProperty: asString(data?.key_property),
    keyType: resolveGraphType(asString(data?.key_type), asString(data?.key_sql_type)),
    properties: parsePropertySpecs(data?.properties),
  }
}

function parseEdgePropertyViewFromPayload(payload: unknown): EdgePropertyView | null {
  const edge = asRecord(payload)
  if (!edge) return null
  const id = parseNumericId(edge.id)
  if (id == null) return null
  const data = asRecord(edge.data)
  return {
    id,
    mappingName: asString(data?.mapping_name) ?? `edge_${id}`,
    relationship: asString(edge.relationship) ?? 'RELATES_TO',
    fromMapping: asString(data?.from_mapping),
    toMapping: asString(data?.to_mapping),
    sourceTable: asString(data?.source_table),
    properties: parsePropertySpecs(data?.properties),
    fromMatchOn: parseMatchOnSpecs(data?.from_match_on),
    toMatchOn: parseMatchOnSpecs(data?.to_match_on),
  }
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
  return ['mysql', 'mariadb', 'sqlserver', 'postgres', 'snowflake', 'clickhouse', 'databricks', 'bigquery', 'spark'].includes(toolId)
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
  const [canvasPopup, setCanvasPopup] = useState<CanvasPopupState | null>(null)

  const [isDark, setIsDark] = useState(() =>
    document.documentElement.classList.contains('dark'),
  )

  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const canvasRef = useRef<FalkorDBCanvasElement | null>(null)
  const canvasPopupRef = useRef<HTMLDivElement | null>(null)
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
        setCanvasPopup(null)
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
  const openNodePopup = useCallback((payload: unknown, event: MouseEvent) => {
    const node = parseNodePropertyViewFromPayload(payload)
    if (!node) return
    const { x, y } = clampPopupPosition(event.clientX, event.clientY)
    setCanvasPopup({ x, y, node })
  }, [])

  const openEdgePopup = useCallback((payload: unknown, event: MouseEvent) => {
    const edge = parseEdgePropertyViewFromPayload(payload)
    if (!edge) return
    const { x, y } = clampPopupPosition(event.clientX, event.clientY)
    setCanvasPopup({ x, y, edge })
  }, [])

  useEffect(() => {
    if (activeTab !== 'canvas') setCanvasPopup(null)
  }, [activeTab])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setCanvasPopup(null)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  useEffect(() => {
    if (!canvasPopup) return

    const onPointerDown = (event: MouseEvent) => {
      const popup = canvasPopupRef.current
      if (!popup) return
      const target = event.target as Node | null
      if (target && popup.contains(target)) return
      setCanvasPopup(null)
    }

    document.addEventListener('mousedown', onPointerDown)
    return () => document.removeEventListener('mousedown', onPointerDown)
  }, [canvasPopup])

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
      onNodeClick: (node, event) => {
        openNodePopup(node, event)
      },
      onNodeRightClick: (node, event) => {
        event.preventDefault()
        openNodePopup(node, event)
      },
      onLinkClick: (link, event) => {
        openEdgePopup(link, event)
      },
      onLinkRightClick: (link, event) => {
        event.preventDefault()
        openEdgePopup(link, event)
      },
      onBackgroundClick: () => setCanvasPopup(null),
      onBackgroundRightClick: (event) => {
        event.preventDefault()
        setCanvasPopup(null)
      },
    })

    if (typeof canvas.zoomToFit === 'function' && canvasData.nodes.length > 0) {
      window.setTimeout(() => canvas.zoomToFit?.(1.05), 50)
    }
  }, [activeTab, canvasData, isDark, openEdgePopup, openNodePopup])

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
      setCanvasPopup(null)
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
                  : 'Schema preview is supported for mysql/mariadb/sqlserver/postgres/snowflake/clickhouse/databricks/bigquery/spark'
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
                  : 'Template generation is supported for mysql/mariadb/sqlserver/postgres/snowflake/clickhouse/databricks/bigquery/spark'
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
          <div className="block text-sm">
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
                  setCanvasPopup(null)
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
            <div className="mt-2 rounded-md border border-border bg-background/40 px-3 py-2 text-xs text-foreground/80">
              <div className="font-medium text-foreground/90">Usability note</div>
              <div className="mt-1">
                Click <span className="font-medium">Preview graph</span> after YAML changes. Then
                left-click or right-click a node/edge in the canvas to open its extracted mapping
                properties. Click the background or press <span className="font-medium">Esc</span> to close.
              </div>
            </div>
            {canvasPopup ? (
              <div
                ref={canvasPopupRef}
                className="fixed z-50 w-[380px] max-h-[420px] overflow-auto rounded-md border border-border bg-background p-3 shadow-lg"
                style={{ left: `${canvasPopup.x}px`, top: `${canvasPopup.y}px` }}
                onContextMenu={(event) => event.preventDefault()}
              >
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs font-medium text-foreground/90">
                    {canvasPopup.node ? 'Node label properties' : 'Edge properties'}
                  </div>
                  <button
                    type="button"
                    className="rounded-md border border-border px-2 py-1 text-[11px] hover:border-primary"
                    onClick={() => setCanvasPopup(null)}
                  >
                    Close
                  </button>
                </div>
                {canvasPopup.node ? (
                  <div className="space-y-1 text-[11px] text-foreground/80">
                    <div className="font-medium text-foreground">{canvasPopup.node.mappingName}</div>
                    <div>Labels: {canvasPopup.node.labels.join(', ') || '—'}</div>
                    {canvasPopup.node.sourceTable ? (
                      <div>Source: <code>{canvasPopup.node.sourceTable}</code></div>
                    ) : null}
                    {canvasPopup.node.keyColumn || canvasPopup.node.keyProperty ? (
                      <div>
                        Key: <code>{canvasPopup.node.keyColumn ?? '?'}</code>
                        {' → '}
                        <code>{canvasPopup.node.keyProperty ?? '?'}</code>
                        {canvasPopup.node.keyType ? (
                          <>
                            {' '}
                            (<code>{canvasPopup.node.keyType}</code>)
                          </>
                        ) : null}
                      </div>
                    ) : null}
                    <div className="pt-1 text-foreground/70">Properties:</div>
                    {canvasPopup.node.properties.length > 0 ? (
                      <ul className="list-disc pl-4">
                        {canvasPopup.node.properties.map((property) => (
                          <li key={`${canvasPopup.node?.mappingName}-${property.name}`}>
                            <code>{property.name}</code>
                            {property.column ? (
                              <>
                                {' ← '}
                                <code>{property.column}</code>
                              </>
                            ) : null}
                            {property.graphType ? (
                              <>
                                {' '}
                                (<code>{property.graphType}</code>)
                              </>
                            ) : null}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <div className="text-foreground/60">No mapped properties.</div>
                    )}
                  </div>
                ) : null}
                {canvasPopup.edge ? (
                  <div className="space-y-1 text-[11px] text-foreground/80">
                    <div className="font-medium text-foreground">{canvasPopup.edge.mappingName}</div>
                    <div>Relationship: <code>{canvasPopup.edge.relationship}</code></div>
                    {canvasPopup.edge.fromMapping || canvasPopup.edge.toMapping ? (
                      <div>
                        Endpoints: <code>{canvasPopup.edge.fromMapping ?? '?'}</code>
                        {' → '}
                        <code>{canvasPopup.edge.toMapping ?? '?'}</code>
                      </div>
                    ) : null}
                    {canvasPopup.edge.sourceTable ? (
                      <div>Source: <code>{canvasPopup.edge.sourceTable}</code></div>
                    ) : null}
                    <div className="pt-1 text-foreground/70">Properties:</div>
                    {canvasPopup.edge.properties.length > 0 ? (
                      <ul className="list-disc pl-4">
                        {canvasPopup.edge.properties.map((property) => (
                          <li key={`${canvasPopup.edge?.mappingName}-${property.name}`}>
                            <code>{property.name}</code>
                            {property.column ? (
                              <>
                                {' ← '}
                                <code>{property.column}</code>
                              </>
                            ) : null}
                            {property.graphType ? (
                              <>
                                {' '}
                                (<code>{property.graphType}</code>)
                              </>
                            ) : null}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <div className="text-foreground/60">No mapped properties.</div>
                    )}
                    {(canvasPopup.edge.fromMatchOn.length > 0 || canvasPopup.edge.toMatchOn.length > 0) ? (
                      <div className="pt-1">
                        <div className="text-foreground/70">Match keys:</div>
                        <ul className="list-disc pl-4">
                          {canvasPopup.edge.fromMatchOn.map((entry, idx) => (
                            <li key={`${canvasPopup.edge?.mappingName}-from-${idx}`}>
                              from <code>{entry.column ?? '?'}</code>
                              {' → '}
                              <code>{entry.property ?? '?'}</code>
                              {entry.graphType ? (
                                <>
                                  {' '}
                                  (<code>{entry.graphType}</code>)
                                </>
                              ) : null}
                            </li>
                          ))}
                          {canvasPopup.edge.toMatchOn.map((entry, idx) => (
                            <li key={`${canvasPopup.edge?.mappingName}-to-${idx}`}>
                              to <code>{entry.column ?? '?'}</code>
                              {' → '}
                              <code>{entry.property ?? '?'}</code>
                              {entry.graphType ? (
                                <>
                                  {' '}
                                  (<code>{entry.graphType}</code>)
                                </>
                              ) : null}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  )
}
