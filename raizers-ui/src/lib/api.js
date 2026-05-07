const RAW_API_BASE_URL = import.meta.env.VITE_API_BASE_URL?.trim() || ''

function normalizeBaseUrl(value) {
  if (!value) return ''
  return value.endsWith('/') ? value.slice(0, -1) : value
}

const API_BASE_URL = normalizeBaseUrl(RAW_API_BASE_URL)

function buildUrl(path) {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path
}

export async function apiRequest(path, options = {}) {
  const response = await fetch(buildUrl(path), options)

  let data = null
  try {
    data = await response.json()
  } catch {
    data = null
  }

  if (!response.ok) {
    const detail = Array.isArray(data?.detail)
      ? data.detail.map(item => item.msg).join(', ')
      : data?.detail
    throw new Error(detail || `HTTP ${response.status}`)
  }

  return data
}

export async function apiRequestBlob(path, options = {}) {
  const response = await fetch(buildUrl(path), options)
  if (!response.ok) {
    let detail = null
    try {
      const data = await response.json()
      detail = Array.isArray(data?.detail)
        ? data.detail.map(item => item.msg).join(', ')
        : data?.detail
    } catch {
      detail = null
    }
    throw new Error(detail || `HTTP ${response.status}`)
  }

  const blob = await response.blob()
  const disposition = response.headers.get('Content-Disposition') || ''
  const match = disposition.match(/filename=\"?([^"]+)\"?/)
  return {
    blob,
    filename: match?.[1] || 'export.xlsx',
  }
}

export function getHealth() {
  return apiRequest('/api/health')
}

export function getAuditProjects() {
  return apiRequest('/api/audit/projects')
}

export function getAuditSubfolders(projectPath) {
  const params = new URLSearchParams({
    project_path: projectPath,
  })
  return apiRequest(`/api/audit/subfolders?${params.toString()}`)
}

export function startAuditPipeline(payload) {
  return apiRequest('/api/audit/pipeline/start', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function refreshAuditPipeline(payload) {
  return apiRequest('/api/audit/pipeline/refresh', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function getAuditJob(jobId) {
  return apiRequest(`/api/audit/jobs/${jobId}`)
}

export function getOperationResults(projectId) {
  return apiRequest(`/api/audit/projects/${projectId}/operation`)
}

export function startOperationExtract(payload) {
  return apiRequest('/api/audit/extract/operation/start', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function getFinancialResults(projectId) {
  return apiRequest(`/api/audit/projects/${projectId}/financial`)
}

export function getProjectCatalog(projectId) {
  return apiRequest(`/api/audit/projects/${projectId}/catalog`)
}

export function getPatrimoineResults(projectId) {
  return apiRequest(`/api/audit/projects/${projectId}/patrimoine`)
}

export function getExportStatus(projectId) {
  return apiRequest(`/api/audit/projects/${projectId}/export`)
}

export function startFinancialExtract(payload) {
  return apiRequest('/api/audit/extract/financial/start', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function startPatrimoineExtract(payload) {
  return apiRequest('/api/audit/extract/patrimoine/start', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function generateExportReport(payload) {
  return apiRequest('/api/audit/export/report', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function getImmoDraft(projectId) {
  return apiRequest(`/api/audit/projects/${projectId}/immo-draft`)
}

export function saveImmoDraft(projectId, draft) {
  return apiRequest(`/api/audit/projects/${projectId}/immo-draft`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(draft),
  })
}

export function getImmoSuggestions(query, limit = 6, signal) {
  const params = new URLSearchParams({
    q: query,
    limit: String(limit),
  })
  return apiRequest(`/api/immo/suggestions?${params.toString()}`, { signal })
}

export function compareImmo(payload) {
  return apiRequest('/api/immo/compare', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function runScraping(payload) {
  return apiRequest('/api/scraping/run', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export function getScrapingJob(jobId) {
  return apiRequest(`/api/scraping/jobs/${jobId}`)
}

export function getScrapingCache(projectId, address = '') {
  const params = new URLSearchParams({ project_id: projectId })
  if (address) params.set('address', address)
  return apiRequest(`/api/scraping/cache?${params.toString()}`)
}

export function exportScraping(payload) {
  return apiRequestBlob('/api/scraping/export', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}
