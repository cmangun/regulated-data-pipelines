# Contributing Guide

Thank you for considering contributing to this project!

## Development Workflow

### Prerequisites

- Node.js 20+ (for TypeScript projects)
- Python 3.11+ (for Python projects)
- Git with conventional commits

### Getting Started

1. Fork the repository
2. Clone your fork locally
3. Install dependencies
4. Create a feature branch
5. Make your changes
6. Run tests and linting
7. Submit a pull request

## Commit Convention

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, semicolons, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvement
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `ci`: CI/CD changes

### Examples

```
feat(adapter): add Azure OpenAI support
fix(governance): correct token counting for embeddings
docs: update quickstart guide
chore: bump dependencies
```

## Branch Naming

Use descriptive branch names:

- `feat/add-azure-adapter`
- `fix/token-count-overflow`
- `docs/update-architecture`
- `chore/upgrade-deps`

## Pull Request Checklist

Before submitting a PR:

- [ ] Tests pass locally (`npm test` or `pytest`)
- [ ] Linting passes (`npm run lint` or `ruff check`)
- [ ] Code is formatted (`npm run format` or `ruff format`)
- [ ] Documentation is updated if needed
- [ ] Commit messages follow conventional commits
- [ ] PR description explains the changes

## Code Review Process

1. Maintainer reviews within 3 business days
2. Address feedback in new commits (don't force-push)
3. Once approved, maintainer will merge
4. Squash merge is preferred for clean history

## Questions?

Open a GitHub Issue or reach out to the maintainers.
