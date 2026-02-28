# Contributing

Thanks for contributing to `claude-mem-cosmos-db`.

This repository is a maintained fork of [`thedotmack/claude-mem`](https://github.com/thedotmack/claude-mem). The fork keeps the original Claude-Mem architecture and extends it with Azure Cosmos DB-backed shared memory and fork-specific maintenance.

## Scope

Use this fork for:

- Azure Cosmos DB support and other fork-specific features
- Bugs reproducible in `claude-mem-cosmos-db`
- Documentation and workflow changes specific to this fork

Consider contributing upstream when:

- The change is generic to Claude-Mem and not specific to Cosmos DB support
- The bug exists in upstream with no fork-specific behavior involved
- The improvement should benefit both repositories

## Before You Start

1. Search existing issues in this repository first.
2. Check whether the change belongs in this fork or upstream.
3. Open an issue for larger changes before starting implementation.

## Development Setup

```bash
git clone https://github.com/praveenreddy854/claude-mem-cosmos-db.git
cd claude-mem-cosmos-db
npm install
npm run build
```

## Pull Requests

1. Create a feature branch from `main`.
2. Keep PRs focused and small enough to review properly.
3. Add or update tests when behavior changes.
4. Update documentation when user-facing behavior changes.
5. Describe whether the change is fork-specific or a candidate for upstream.

## Review Policy

The `main` branch is protected:

- Pull requests are required
- Code owner review is required
- Approval from `@praveenreddy854` is required before merge

## Style

- Follow the existing code style and project structure.
- Avoid unrelated refactors in feature PRs.
- Preserve upstream attribution in files and docs where relevant.

## Testing

Run the smallest relevant test set for your change. Examples:

```bash
npm run build
bun test
```

If you cannot run a relevant test, explain why in the pull request.
