import type {
  ConfigRecord,
  ConfigStateInfo,
  CreateConfigRequest,
  RunEvent,
  RunRecord,
  StartRunRequest,
  ToolManifest,
  ToolSummary,
  UpdateConfigRequest,
} from './types'

const API_KEY_STORAGE = 'falkordb-control-plane-api-key'

function getApiKey(): string | undefined {
  const stored = localStorage.getItem(API_KEY_STORAGE)
  if (stored) return stored

  // Optional build-time key.
  const envKey = import.meta.env.VITE_CONTROL_PLANE_API_KEY as string | undefined
  return envKey || undefined
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const apiKey = getApiKey()

  const res = await fetch(path, {
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}),
      ...(init?.headers ?? {}),
    },
  })

  const text = await res.text().catch(() => '')

  if (!res.ok) {
    throw new Error(text || `HTTP ${res.status}`)
  }

  // Guard against SPA fallbacks / proxies returning HTML with 200.
  const ct = res.headers.get('content-type') ?? ''
  if (!ct.includes('application/json')) {
    const snippet = text.slice(0, 200)
    throw new Error(`Expected JSON but got content-type '${ct}'. Body starts with: ${snippet}`)
  }

  return JSON.parse(text) as T
}

export const api = {
  tools: {
    list: () => apiFetch<ToolSummary[]>('/api/tools'),
    get: (toolId: string) => apiFetch<ToolManifest>(`/api/tools/${toolId}`),
  },
  configs: {
    list: (toolId?: string) => {
      const qs = toolId ? `?tool_id=${encodeURIComponent(toolId)}` : ''
      return apiFetch<ConfigRecord[]>(`/api/configs${qs}`)
    },
    get: (configId: string) => apiFetch<ConfigRecord>(`/api/configs/${configId}`),
    state: (configId: string) =>
      apiFetch<ConfigStateInfo>(`/api/configs/${configId}/state`),
    clearState: (configId: string) =>
      apiFetch<{ ok: boolean; deleted: boolean; resolved_path?: string }>(
        `/api/configs/${configId}/state/clear`,
        {
          method: 'POST',
          body: JSON.stringify({}),
        },
      ),
    create: (req: CreateConfigRequest) =>
      apiFetch<ConfigRecord>('/api/configs', {
        method: 'POST',
        body: JSON.stringify(req),
      }),
    update: (configId: string, req: UpdateConfigRequest) =>
      apiFetch<ConfigRecord>(`/api/configs/${configId}`, {
        method: 'PUT',
        body: JSON.stringify(req),
      }),
  },
  runs: {
    list: (toolId?: string) => {
      const qs = toolId ? `?tool_id=${encodeURIComponent(toolId)}` : ''
      return apiFetch<RunRecord[]>(`/api/runs${qs}`)
    },
    get: (runId: string) => apiFetch<RunRecord>(`/api/runs/${runId}`),
    logs: (runId: string, limit = 2000) =>
      apiFetch<RunEvent[]>(`/api/runs/${runId}/logs?limit=${limit}`),
    start: (req: StartRunRequest) =>
      apiFetch<RunRecord>('/api/runs', {
        method: 'POST',
        body: JSON.stringify(req),
      }),
    stop: (runId: string) =>
      apiFetch<{ ok: boolean }>(`/api/runs/${runId}/stop`, {
        method: 'POST',
        body: JSON.stringify({}),
      }),
  },
}
