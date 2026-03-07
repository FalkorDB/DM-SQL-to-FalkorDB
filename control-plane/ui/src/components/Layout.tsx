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
  const [aboutOpen, setAboutOpen] = useState(false)
  const repoUrl = 'https://github.com/FalkorDB/DM-SQL-to-FalkorDB'
  const licenseUrl =
    'https://github.com/FalkorDB/DM-SQL-to-FalkorDB/blob/main/Snowflake-to-FalkorDB/LICENSE'

  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  useEffect(() => {
    if (!aboutOpen) return

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setAboutOpen(false)
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [aboutOpen])
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
              FalkorDB Data Migration tools
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
              onClick={() => setAboutOpen(true)}
            >
              About
            </button>
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

      {aboutOpen ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="about-dialog-title"
          onClick={() => setAboutOpen(false)}
        >
          <div
            className="Panel w-full max-w-xl p-5 shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 id="about-dialog-title" className="text-lg font-semibold">
                  About FalkorDB Data Migration tools
                </h2>
                <p className="mt-1 text-sm text-foreground/70">
                  Control plane UI for managing migration tools, configurations, runs, and metrics.
                </p>
              </div>
              <button
                className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
                onClick={() => setAboutOpen(false)}
                aria-label="Close about dialog"
              >
                Close
              </button>
            </div>

            <div className="mt-4 space-y-3 text-sm">
              <div>
                <div className="text-foreground/70">Public repository</div>
                <a
                  className="text-primary hover:underline break-all"
                  href={repoUrl}
                  target="_blank"
                  rel="noreferrer"
                >
                  {repoUrl}
                </a>
              </div>

              <div>
                <div className="text-foreground/70">Support</div>
                <a className="text-primary hover:underline" href="mailto:support@falkordb.com">
                  support@falkordb.com
                </a>
              </div>

              <div>
                <div className="text-foreground/70">License</div>
                <a
                  className="text-primary hover:underline break-all"
                  href={licenseUrl}
                  target="_blank"
                  rel="noreferrer"
                >
                  View license
                </a>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
