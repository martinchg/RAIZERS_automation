import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import siteIconUrl from '../../assets/R.png'

const favicon = document.querySelector("link[rel='icon']") ?? document.createElement('link')
favicon.setAttribute('rel', 'icon')
favicon.setAttribute('type', 'image/png')
favicon.setAttribute('href', siteIconUrl)
if (!favicon.parentNode) {
  document.head.appendChild(favicon)
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
