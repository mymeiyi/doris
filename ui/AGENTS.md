# UI Development Guide

Web UI dashboard - React/TypeScript frontend separate from FE Java component. 100+ files, modern frontend stack with its own conventions.

## Structure

```
ui/
├── src/
│   ├── pages/            # Route-based page components
│   │   ├── login/        # Authentication page
│   │   ├── home/         # Cluster overview dashboard
│   │   ├── playground/   # Query execution UI
│   │   ├── system/       # System monitoring
│   │   └── ...
│   ├── components/       # Reusable UI components
│   ├── api/              # API client (api.ts)
│   ├── utils/            # Utilities
│   └── App.tsx           # Root component
├── public/               # Static assets
├── package.json          # Dependencies (React, TypeScript)
└── tsconfig.json         # TypeScript config (ES2020, strict mode)
```

## Where to Look

| Task | Path | Notes |
|------|------|-------|
| Auth flow | pages/login/, api/ | Basic Auth headers, localStorage |
| Dashboard | pages/home/ | Cluster overview dashboard |
| Query UI | pages/playground/ | SQL query execution |
| API client | api/api.ts | Centralized REST calls |

## Conventions

- TypeScript with strict mode
- React functional components with hooks
- ESLint/Prettier for code style
- API calls return promises, handle errors
- Store auth token in localStorage

## Commands

```bash
npm install              # Install deps
npm run dev              # Development server
npm run build            # Production build
```

## Auth Integration

UI uses Basic Auth (`Authorization: Basic base64(username:password)`) against FE's `/rest/v1/login`. On successful login, stores username in `localStorage`. All subsequent requests check for auth token; redirects to `/login` on 401. Separation from FE: UI is static frontend built separately, communicates with FE via REST API only.
