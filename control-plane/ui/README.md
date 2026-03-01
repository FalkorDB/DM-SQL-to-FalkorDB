# FalkorDB Migrate Control Plane UI

This is the web UI for the control plane (see `../server`). It’s a Vite + React + TypeScript app.

## Features

- Tools list/details
- Configs management
  - Create/edit YAML or JSON configs
  - Syntax-highlighted config editor (Auto/YAML/JSON selector)
  - Load config from a local `.yaml`/`.yml`/`.json` file
- Runs management (start/stop, history)
- Live log streaming (SSE)
- View run logs for completed runs (persisted)

## Development

From this directory:

```bash
npm install
npm run dev
```

Vite will print the local URL it picked (usually `http://localhost:5173`, but it may choose another port if it’s already in use).

## Build

```bash
npm run build
```

The production build outputs to `dist/`. The control plane server can be configured to serve that directory.

## Notes

- The UI has an “API key” button that stores the control plane API key in browser localStorage.
- The config editor uses CodeMirror for syntax highlighting.
