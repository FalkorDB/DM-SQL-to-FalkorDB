import { Link } from 'react-router-dom'

export default function HomePage() {
  return (
    <div className="space-y-4">
      <h1 className="text-3xl font-semibold">Control Plane</h1>
      <p className="text-foreground/70">
        Manage and run the SQL → FalkorDB migration/sync tools.
      </p>
      <div className="flex items-center gap-2">
        <Link
          to="/tools"
          className="px-2 py-1 rounded-md text-sm border border-primary text-primary hover:bg-primary/10"
        >
          Tools
        </Link>
        <Link
          to="/configs"
          className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
        >
          Configs
        </Link>
        <Link
          to="/runs"
          className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
        >
          Runs
        </Link>
        <Link
          to="/metrics"
          className="px-2 py-1 rounded-md text-sm border border-border hover:border-primary"
        >
          Metrics
        </Link>
      </div>
    </div>
  )
}
