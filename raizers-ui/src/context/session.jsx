import { createContext, useContext, useReducer } from 'react'

const SessionContext = createContext(null)

const initialState = {
  project: null,
  projectId: null,
  projectPath: null,
  subfolder: null,
  projectCatalog: null,
  lastRefresh: null,
  immoResult: null,
  immoDraft: null,
  pipeline: 'idle', // idle | running | done | error
  pipelineStats: null,
  tabs: {
    operation: 'idle',
    financier: 'idle',
    patrimoine: 'idle',
  },
  generated: [],
  jobIds: { operation: null, financier: null, patrimoine: null },
}

function reducer(state, action) {
  switch (action.type) {
    case 'SET_PROJECT':
      return {
        ...initialState,
        project: action.project,
        projectId: action.projectId ?? null,
        projectPath: action.projectPath ?? null,
        subfolder: action.subfolder,
        projectCatalog: action.projectCatalog ?? null,
      }
    case 'SET_PROJECT_CATALOG':
      return { ...state, projectCatalog: action.projectCatalog ?? null }
    case 'SET_LAST_REFRESH':
      return { ...state, lastRefresh: action.lastRefresh ?? null }
    case 'SET_IMMO_RESULT':
      return {
        ...state,
        immoResult: action.immoResult ?? null,
        generated: action.immoResult
          ? [...new Set([...state.generated, 'immo'])]
          : state.generated.filter(item => item !== 'immo'),
      }
    case 'SET_IMMO_DRAFT':
      return {
        ...state,
        immoDraft: action.immoDraft ?? null,
      }
    case 'PIPELINE_START':
      return { ...state, pipeline: 'running' }
    case 'PIPELINE_DONE':
      return { ...state, pipeline: 'done', pipelineStats: action.stats }
    case 'PIPELINE_ERROR':
      return { ...state, pipeline: 'error' }
    case 'TAB_START':
      return { ...state, tabs: { ...state.tabs, [action.tab]: 'running' } }
    case 'SET_JOB_ID':
      return { ...state, jobIds: { ...state.jobIds, [action.tab]: action.jobId } }
    case 'TAB_DONE':
      return {
        ...state,
        tabs: { ...state.tabs, [action.tab]: 'done' },
        jobIds: { ...state.jobIds, [action.tab]: null },
        generated: [...new Set([...state.generated, action.tab])],
      }
    case 'TAB_ERROR':
      return { ...state, tabs: { ...state.tabs, [action.tab]: 'error' } }
    case 'RESET':
      return initialState
    default:
      return state
  }
}

export function SessionProvider({ children }) {
  const [session, dispatch] = useReducer(reducer, initialState)
  return (
    <SessionContext.Provider value={{ session, dispatch }}>
      {children}
    </SessionContext.Provider>
  )
}

export const useSession = () => useContext(SessionContext)
