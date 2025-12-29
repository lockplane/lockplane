# Development Workflow

## IMPORTANT: Never push directly to main!

**Always create a Pull Request** for any code changes, even small fixes.

## Repository Setup Note

This repository is **NOT a fork**. We work directly on branches in `origin` (lockplane/lockplane).
- Branches are created in the main repository, not in a personal fork
- PRs are created from `branch-name` → `main` within the same repository

## Checklist for Every Change

### 1. Create a Branch
- [ ] Create a new branch: `git checkout -b descriptive-branch-name`
- [ ] Make your changes on this branch

### 2. Implement and Test
- [ ] Write tests for new code
- [ ] Run tests: `go test -v ./...`
- [ ] Verify all tests pass
- [ ] Run validation tools:
  - `go fmt ./...`
  - `go vet ./...`
  - `errcheck ./...`
  - `staticcheck ./...`

### 3. Commit Changes
- [ ] Commit with descriptive message
- [ ] Push branch: `git push -u origin branch-name`

### 4. Create Pull Request
- [ ] **ALWAYS** create PR with flags (NOT --web):
  ```bash
  gh pr create --title "Clear title" --body "Detailed description"
  ```
- [ ] **IMPORTANT**: Always specify `--base` and `--head` explicitly to avoid ambiguity:
  ```bash
  gh pr create --repo lockplane/lockplane --base main --head lockplane:branch-name --title "..." --body "..."
  ```
- [ ] **NEVER** use `--web` flag (makes user enter details manually)
- [ ] Include in PR body:
  - Summary of what changed
  - Why it changed
  - Test coverage
  - Examples if applicable
- [ ] **NEVER** push directly to main (even for small fixes)

### 5. Monitor CI and Fix Issues
- [ ] Check PR status: `gh pr view <number> --json statusCheckRollup`
- [ ] If tests fail, view logs: `gh run view <run-id> --log-failed`
- [ ] Fix issues and push updates to the same branch
- [ ] Keep iterating until all workflows pass

### 6. Merge
- [ ] Wait for approval (if required)
- [ ] Merge PR only after CI passes

## Exceptions

The **ONLY** time you can push directly to main is when the user **explicitly** says:
- "push this to main"
- "commit directly to main"
- "skip the PR"

Otherwise, **ALWAYS CREATE A PULL REQUEST**.
