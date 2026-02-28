export type Theme = 'dark' | 'light'

const STORAGE_KEY = 'falkordb-control-plane-theme'

export function getInitialTheme(): Theme {
  const saved = localStorage.getItem(STORAGE_KEY)
  if (saved === 'dark' || saved === 'light') return saved

  const prefersDark = window.matchMedia?.('(prefers-color-scheme: dark)')?.matches
  return prefersDark ? 'dark' : 'light'
}

export function applyTheme(theme: Theme) {
  const root = document.documentElement

  root.classList.remove('dark')
  root.classList.remove('light')

  root.classList.add(theme)
  if (theme === 'dark') root.classList.add('dark')

  localStorage.setItem(STORAGE_KEY, theme)
}
