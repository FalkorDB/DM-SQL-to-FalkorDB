import { Link, NavLink, Outlet } from 'react-router-dom'
import { useEffect, useState } from 'react'

import { applyTheme, getInitialTheme, type Theme } from '../lib/theme'

function NavItem({ to, label }: { to: string; label: string }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        [
          'px-3 py-2 rounded-md text-sm border',
          isActive
            ? 'border-primary text-primary'
            : 'border-transparent text-foreground/80 hover:text-foreground hover:border-border',
        ].join(' ')
      }
    >
      {label}
    </NavLink>
  )
}

export default function Layout() {
  const [theme, setTheme] = useState<Theme>(() => getInitialTheme())

  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  function toggleTheme() {
    const next: Theme = theme === 'dark' ? 'light' : 'dark'
    setTheme(next)
    applyTheme(next)
  }

  function setApiKey() {
    const current = localStorage.getItem('falkordb-control-plane-api-key') ?? ''
    const next = window.prompt('Set CONTROL_PLANE_API_KEY (stored in browser localStorage):', current)
    if (next === null) return

    if (next.trim() === '') {
      localStorage.removeItem('falkordb-control-plane-api-key')
    } else {
      localStorage.setItem('falkordb-control-plane-api-key', next.trim())
    }
  }

  return (
    <div className="min-h-full">
      <header className="border-b border-border">
        <div className="Gradient h-1" />
        <div className="mx-auto max-w-6xl px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <a
              href="https://www.falkordb.com"
              target="_blank"
              rel="noreferrer"
              className="flex items-center"
              aria-label="FalkorDB website"
              title="FalkorDB"
            >
              <img
                src="/icons/F-light.svg"
                alt="FalkorDB"
                className="h-7 w-7 dark:hidden"
              />
              <img
                src="/icons/F-dark.svg"
                alt="FalkorDB"
                className="hidden h-7 w-7 dark:block"
              />
            </a>

            <Link to="/" className="font-semibold tracking-tight">
              FalkorDB Migrate Control Plane
            </Link>
            <nav className="flex items-center gap-2">
              <NavItem to="/tools" label="Tools" />
              <NavItem to="/configs" label="Configs" />
              <NavItem to="/runs" label="Runs" />
              <NavItem to="/metrics" label="Metrics" />
            </nav>
          </div>

          <div className="flex items-center gap-2">
            <button
              className="px-3 py-2 rounded-md text-sm border border-border hover:border-primary"
              onClick={setApiKey}
            >
              API key
            </button>
            <button
              className="px-3 py-2 rounded-md text-sm border border-border hover:border-primary"
              onClick={toggleTheme}
            >
              {theme === 'dark' ? 'Light mode' : 'Dark mode'}
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-6xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  )
}
