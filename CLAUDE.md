# DPQ — Distributed Priority Queue

An in-memory priority queue service in Java 21. Design decisions live in `decisions.md` (referenced as D1, D8d, …); the implementation plan and PR stack live in `plan.md`. Read both before changing code. `Requirements.txt` is the original brief.

## Build & test

- `./gradlew check` (or `make test`): compile and run all tests.
- `./gradlew build` (or `make build`): full build.
- JDK 21 is provisioned automatically by the foojay toolchain resolver (D18a); any JDK ≥ 17 can run Gradle itself.

Modules: `core` (pure engine), `server` (HTTP/JSON + Prometheus), `harness` (load harness over HTTP).

## Branches & PRs

- Branch per PR: `pr/NN-slug` (e.g. `pr/03-lane`).
- PR N's base branch = PR N-1's branch; PR 1's base is `main`.
- Every PR must:
  - compile and pass `./gradlew check`;
  - include tests with the code;
  - stay at **≤ ~400 changed lines**, excluding generated files;
  - use the PR template: **What** / **Why** (with D# refs) / **How to test** / **Next in stack**.
- Workflow: build and open the **whole stack** in order, each PR green in CI, then iterate on review feedback bottom-up.
- PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

### Restack runbook (after PR N is squash-merged)

```bash
git fetch origin
# Replay everything above pr/NN onto the new main; --update-refs moves every
# intermediate pr/* branch along with the top branch (git ≥ 2.38).
git rebase --update-refs --onto origin/main pr/NN pr/<top>
git push --force-with-lease origin $(git for-each-ref --format='%(refname:short)' 'refs/heads/pr/*')
gh pr edit <N+1 number> --base main
```

**Review fixes:** commit on the owning branch, then `git rebase --update-refs <owning branch> pr/<top>` and force-push with lease.

CI (`.github/workflows/ci.yml`) triggers on `pull_request` **without** a branch filter, so stacked PRs are tested too.

## TDD (D17)

- **Applies to PRs 2–7 and 10–11.** For each test listed under the PR in `plan.md`:
  - **Red:** write the test and run it. It must fail *for the expected reason* (an assertion failure, not a compile error in unrelated code).
  - **Green:** write the minimum code to make it pass.
  - **Refactor:** tidy up with all tests green.
- **Commit order inside a PR:** `test: …` (red) → `feat: …` (green) → optional `refactor: …`. Push only when green.
- **PRs 8–9 verify existing code.** Any bug they find becomes a failing unit test first, then the fix.
- **PRs 1, 12–14 are exempt.**
- Tests exercise real behaviour: no trivially-passing or over-mocked tests.

## Code style

- Keep `core` free of HTTP, JSON and metrics-library dependencies.
- Never call `System.currentTimeMillis()`, `System.nanoTime()` or `Instant.now()` directly — use the injected `Clock` (`monotonicMillis()` for deadlines and ages, `wallTime()` for displayed timestamps).
- No `Thread.sleep` in unit tests; advance `FakeClock` instead.
- Every mutable field of a Partition is guarded by its lock. Document the lock ordering (source → DLQ) in code comments.
- Prefer records for immutable value types.
- Follow `plan.md` and `decisions.md`; if either looks wrong, raise it rather than silently deviating.
