import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'

import CodeMirror from '@uiw/react-codemirror'
import { json } from '@codemirror/lang-json'
import { yaml } from '@codemirror/lang-yaml'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView, placeholder } from '@codemirror/view'

import { api } from '../lib/api'
import type { ToolSummary } from '../lib/types'

type ConfigSyntax = 'auto' | 'yaml' | 'json'

function inferSyntax(text: string): Exclude<ConfigSyntax, 'auto'> {
  const t = text.trim()
  if (!t) return 'yaml'

  const first = t[0]
  // Cheap fast-path: JSON is common and starts with one of these.
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
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const [isDark, setIsDark] = useState(() =>
    document.documentElement.classList.contains('dark'),
  )

  const fileInputRef = useRef<HTMLInputElement | null>(null)

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

      if (ext === 'json') setSyntax('json')
      else if (ext === 'yaml' || ext === 'yml') setSyntax('yaml')
      else setSyntax('auto')

      if (isNew && name.trim() === '') {
        // Use the filename (without extension) as a reasonable default.
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

  const effectiveSyntax = syntax === 'auto' ? inferSyntax(content) : syntax

  const editorExtensions = useMemo(
    () => [
      effectiveSyntax === 'json' ? json() : yaml(),
      EditorView.lineWrapping,
      placeholder('paste YAML/JSON here'),
      EditorView.theme({
        '&': {
          fontSize: '12px',
        },
        '.cm-content': {
          fontFamily:
            'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
        },
      }),
    ],
    [effectiveSyntax],
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
            className="px-3 py-2 rounded-md text-sm border border-border hover:border-primary"
          >
            Back
          </Link>
          <button
            className="px-3 py-2 rounded-md text-sm border border-primary text-primary hover:bg-primary/10 disabled:opacity-50"
            onClick={save}
            disabled={busy}
          >
            {busy ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>

      <div className="Panel p-4 space-y-4">
        <label className="block text-sm">
          <div className="text-foreground/70 mb-1">Tool</div>
          <select
            disabled={!isNew}
            className="w-full bg-background border border-border rounded-md px-3 py-2 disabled:opacity-60"
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

        <div className="flex items-center justify-between gap-3">
          <div className="text-sm text-foreground/70">
            Load an existing config from your local filesystem.
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
                void loadFromFile(f)
                // Allow re-selecting the same file.
                e.currentTarget.value = ''
              }}
            />
            <button
              type="button"
              className="px-3 py-2 rounded-md text-sm border border-border hover:border-primary"
              onClick={() => fileInputRef.current?.click()}
            >
              Choose file
            </button>
          </div>
        </div>

        <label className="block text-sm">
          <div className="text-foreground/70 mb-1">Name</div>
          <input
            className="w-full bg-background border border-border rounded-md px-3 py-2"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </label>

        <label className="block text-sm">
          <div className="flex items-center justify-between mb-1">
            <div className="text-foreground/70">Config</div>
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
      </div>
    </div>
  )
}
