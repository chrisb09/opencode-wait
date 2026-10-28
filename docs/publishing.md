# Publishing Guide

## Manual First Publish

To publish `opencode-wait` to npm manually:

```bash
npm install
npm test
npm publish --access public
```

Before publishing, verify:

- `npm test` passes.
- `npm pack --dry-run` contains only `dist/`, `README.md`, `LICENSE`, and declared files.
- `package.json` repository URL points to your fork repository.

## GitHub Actions Automated Release

When a GitHub Release is published, the workflow in `.github/workflows/release.yml`:

1. Checks out the repository.
2. Sets up Node.js 24.
3. Upgrades npm to support Trusted Publishing.
4. Runs `npm ci`, `npm run build`, and `npm run test:built`.
5. Publishes via npm Trusted Publishing (OIDC) if the version is not yet published.

## npm Trusted Publishing Setup

In your package settings on npmjs.com, configure Trusted Publisher:

- **Provider**: GitHub Actions
- **Repository**: `<your-github-username>/opencode-wait`
- **Workflow filename**: `release.yml`
- **Environment name**: (leave blank)
