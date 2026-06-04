import api from './client'

// Health
export const getServicesHealth  = () => api.get('/health/services')
export const getPlatformOverview = () => api.get('/health/overview')

// Tables
export const getAllTables    = () => api.get('/tables/')
export const previewTable    = (schema: string, table: string, limit = 50) =>
  api.get(`/tables/${schema}/${table}/preview?limit=${limit}`)
export const runQuery        = (sql: string, limit = 100) =>
  api.post('/tables/query', { sql, limit })

// Pipeline
export const getFlinkJobs    = () => api.get('/pipeline/flink/jobs')
export const getFlinkOverview = () => api.get('/pipeline/flink/overview')
export const getAirflowDags  = () => api.get('/pipeline/airflow/dags')
export const triggerDag      = (dagId: string) =>
  api.post(`/pipeline/airflow/trigger/${dagId}`)
export const getDagRuns      = (dagId: string) =>
  api.get(`/pipeline/airflow/runs/${dagId}`)

// Spark
export const getSparkCluster = () => api.get('/spark/cluster')
export const getSparkApps    = () => api.get('/spark/apps')
export const submitSparkJob  = (jobType: string) =>
  api.post('/spark/submit', { job_type: jobType })

// Monitoring
export const getMetrics      = () => api.get('/monitoring/metrics')
export const getTableCounts  = () => api.get('/monitoring/table-counts')
export const getGrafanaDashboards = () =>
  api.get('/monitoring/grafana/dashboards')

// Auth
export const login  = (username: string, password: string) =>
  api.post('/auth/login', { username, password })
export const getMe  = () => api.get('/auth/me')

export const startPyFlinkJob  = () =>
  api.post('/pipeline/flink/start')
export const getTaskStates    = (dagId: string, runId: string) =>
  api.get(`/pipeline/airflow/task-states/${dagId}/${runId}`)
export const getTaskLogs      = (dagId: string, runId: string, taskId: string) =>
  api.get(`/pipeline/airflow/task-logs/${dagId}/${runId}/${taskId}`)
export const getKafkaTopics   = () =>
  api.get('/pipeline/kafka/topics')
