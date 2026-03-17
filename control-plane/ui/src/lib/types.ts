export type ToolCapabilities = {
  supports_daemon: boolean
  supports_purge_graph: boolean
  supports_purge_mapping: boolean
  supports_metrics: boolean
}

export type ToolSummary = {
  id: string
  display_name: string
  description?: string | null
  capabilities: ToolCapabilities
}

export type ToolManifest = {
  id: string
  displayName: string
  description?: string | null
  workingDir: string
  executable: unknown
  capabilities: ToolCapabilities
  config: {
    fileExtensions: string[]
    examples: string[]
  }
}

export type ConfigRecord = {
  id: string
  tool_id: string
  name: string
  content: string
  created_at: string
  updated_at: string
}

export type RunMode = 'one_shot' | 'daemon'
export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'stopped'

export type RunRecord = {
  id: string
  tool_id: string
  config_id: string
  mode: RunMode
  status: RunStatus
  started_at: string
  ended_at?: string | null
  exit_code?: number | null
  error?: string | null
}

export type GenerateScaffoldTemplateRequest = {
  config_content: string
  include_schema_summary?: boolean
}

export type GenerateScaffoldTemplateResponse = {
  template_yaml: string
  schema_summary?: string | null
}

export type CanvasNode = {
  id: number
  labels: string[]
  color: string
  visible: boolean
  caption?: string | null
  data: Record<string, unknown>
}

export type CanvasLink = {
  id: number
  relationship: string
  color: string
  source: number
  target: number
  visible: boolean
  data: Record<string, unknown>
}

export type GenerateSchemaGraphPreviewRequest = {
  config_content: string
}

export type GenerateSchemaGraphPreviewResponse = {
  canvas_data: {
    nodes: CanvasNode[]
    links: CanvasLink[]
  }
  warnings: string[]
  source: 'config' | 'template'
}

export type CreateConfigRequest = {
  tool_id: string
  name: string
  content: string
}

export type UpdateConfigRequest = {
  name: string
  content: string
}

export type ConfigStateInfo = {
  backend?: string | null
  file_path?: string | null
  resolved_path?: string | null
  exists: boolean
  last_watermark?: string | null
  watermarks?: Record<string, string> | null
  warning?: string | null
}

export type StartRunRequest = {
  tool_id: string
  config_id: string
  mode: RunMode
  daemon_interval_secs?: number
  purge_graph?: boolean
  purge_mappings?: string[]
}

export type RunEvent =
  | { type: 'state'; status: RunStatus }
  | { type: 'log'; stream: string; line: string }
  | { type: 'exit'; status: RunStatus; exit_code?: number | null; error?: string | null }

export type ToolMetricsView = {
  tool_id: string
  display_name: string
  supports_metrics: boolean
  endpoint?: string | null
  format?: string | null
  metric_prefix?: string | null
  mapping_label?: string | null
  fetched_at: string
  snapshot_timestamp?: string | null
  snapshot_run_id?: string | null
  snapshot_config_id?: string | null
  snapshot_source?: string | null
  overall: Record<string, number>
  per_mapping: Record<string, Record<string, number>>
  warnings: string[]
  error?: string | null
}
