import { createContext, useContext, useReducer } from 'react'

const SessionContext = createContext(null)

const initialState = {
  project: null,
  subfolder: null,
  pipeline: 'idle', // idle | running | done | error
  pipelineStats: null,
  tabs: {
    operation: 'idle',
    financier: 'idle',
    patrimoine: 'idle',
  },
  generated: [],
}

function reducer(state, action) {
  switch (action.type) {
    case 'SET_PROJECT':
      return { ...initialState, project: action.project, subfolder: action.subfolder }
    case 'PIPELINE_START':
      return { ...state, pipeline: 'running' }
    case 'PIPELINE_DONE':
      return { ...state, pipeline: 'done', pipelineStats: action.stats }
    case 'PIPELINE_ERROR':
      return { ...state, pipeline: 'error' }
    case 'TAB_START':
      return { ...state, tabs: { ...state.tabs, [action.tab]: 'running' } }
    case 'TAB_DONE':
      return {
        ...state,
        tabs: { ...state.tabs, [action.tab]: 'done' },
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
