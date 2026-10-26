# 0001. Adopt a `crates/` workspace layout for the full OAR Rust port

* **Status:** Accepted
* **Date:** 2026-10-26
* **Authors:** Olivier Richard
* **Deciders:** Olivier Richard, OAR maintainers

## Context and Problem Statement

The repository keeps six Rust crates directly at its root (`oar-scheduler-core`, `oar-scheduler-hooks`, `oar-scheduler-db`,
`oar-scheduler-redox`, `oar-scheduler-meta`, `oar-scheduler-bench`), mixed with repository-level artifacts: the `oar3` git submodule
(the Python reference implementation), `docs/`, the Nix flake, the `justfile`, the CI definitions and the license.

Today the scope is the scheduler only. The next milestone is a complete OAR implementation in Rust. A full OAR is not just the
scheduling algorithm: it is the scheduling algorithm **plus** a shared job/node/queue model, the persistence layer, the daemons
(`bipbip`, `bipbip_commander`, `greta`, `almighty`, `hulot`), the user and administrative CLI (`oarsub`, `oarstat`, `oardel`,
`oarnodesetting`, `oar-database`, ...), the message protocol between those components, and the Python bindings that let the port
replace the Python code progressively. Each of these introduces one or more crates; the realistic target is 15 to 20 crates in a
single workspace.

Keeping crate directories at the root does not scale to that size: the root turns into a list of `oar-*` entries where
repository-level files are indistinguishable from product code, and every new crate degrades the root's readability. We also lack a
stated location for the crates that are about to be written, which invites ad-hoc decisions (`daemons/`, `lib/`, `shared/`, ...).

The constraint is that the reorganization must not disturb the Python reference tree (`oar3/` submodule, needed for parity testing
during the port), the existing workspace dependencies, the maturin build of the Python binding crate, the Nix packaging or the CI.

## Decision Drivers

* Expected growth to 15-20 crates (shared model, database, scheduler, meta-scheduler, daemons, CLI, bindings) in one workspace.
* A stable, conventional location for product code that stays valid whatever crate naming we adopt later.
* Minimal migration cost: sibling `path = "../..."` dependencies, `Cargo.lock`, `flake.nix`, `justfile` and maturin must keep working.
* Clear separation between product crates and repository-level artifacts (reference implementation, ADRs, packaging, tooling).
* Reviewability: adding a crate must be an explicit, reviewable change of the workspace manifest.

## Considered Options

1. **Keep the flat root layout**: crate directories remain at the repository root, as today.
2. **Single `crates/` directory, flat members**: every product crate moves to `crates/<crate-name>`, the workspace manifest stays at
   the root.
3. **`crates/` grouped by domain**: `crates/scheduler/`, `crates/daemons/`, `crates/cli/`, each holding crate directories.
4. **Several independent Cargo workspaces**: one workspace per domain (e.g. `workspaces/scheduler`, `workspaces/oar`), each with its
   own lockfile and build.

## Decision Outcome

Chosen option: **single `crates/` directory with flat members**, because it absorbs the expected growth with the smallest
structural commitment: it reserves a conventional place for the 15-20 crates of the full port, while postponing the naming and
grouping decisions that depend on the port's actual shape.

### Target layout

```text
/
├── Cargo.toml            # workspace manifest, explicit member list
├── Cargo.lock
├── crates/               # all the product crates
│   ├── oar-scheduler-core/
│   ├── oar-scheduler-hooks/
│   ├── oar-scheduler-db/
│   ├── oar-scheduler-meta/
│   ├── oar-scheduler-redox/   # maturin: pyproject.toml stays next to Cargo.toml
│   └── oar-scheduler-bench/
├── docs/
│   ├── adr/
│   └── architecture/
├── oar3/                 # Python reference implementation (git submodule, unchanged)
├── flake.nix, flake.lock, justfile, .github/, .gitlab-ci.yml, README.md, LICENSE.md
```

### Rules

* Every product crate lives in `crates/`; nothing else does. Root directories stay reserved for repository-level concerns.
* The workspace manifest lists the members explicitly (`members = ["crates/oar-scheduler-core", ...]`). A glob
  (`members = ["crates/*"]`) breaks as soon as a non-crate directory appears under `crates/`: Cargo then fails with
  `failed to load manifest for workspace member .../crates/<dir>`. Explicit membership also keeps crate additions visible in review.
  The glob can be adopted later, once the crate count makes the list a maintenance burden and the directory rule is established.
* Crate directories keep their current names during the move. The question of renaming them to an `oar-*` scheme belongs to the
  follow-up ADR on crate naming, once the daemon and CLI crates exist.
* Crates stay siblings under `crates/`, so their `path = "../oar-scheduler-core"` dependencies remain valid: the move changes no
  dependency and no `Cargo.lock` entry.
* The `oar3` submodule and `docs/` stay at the root: they are repository-level artifacts, not product crates.
* New crates for the port (model, protocol, daemons, CLI, bindings) are created under `crates/` from their first commit.

### Migration steps

1. `git mv oar-scheduler-core crates/oar-scheduler-core`, same for the five other crates (`git mv` preserves history:
   `git log --follow` keeps working).
2. `Cargo.toml`: replace `members = [...]` with the explicit `crates/...` list.
3. `flake.nix:45`: `configurePhase = "cd crates/oar-scheduler-redox"` (the `src = ./.` and `importCargoLock { lockFile = ./Cargo.lock; }`
   entries are workspace-root based and stay unchanged).
4. `justfile:80`: `cd crates/oar-scheduler-redox && maturin build`.
5. `.gitlab-ci.yml:13`: run from the workspace root (`cargo test -p oar-scheduler-core`, `cargo tarpaulin -p oar-scheduler-core`
   instead of `cd oar-scheduler-core`) so future moves do not touch CI again.
6. `crates/oar-scheduler-bench/src/benchmarker.rs:208`: build the output path from `CARGO_MANIFEST_DIR` plus `benchmarks/` instead of
   the hardcoded `./oar-scheduler-bench/benchmarks/` prefix.
7. READMEs: in the root `README.md`, prefix the crate links with `crates/` (`[oar-scheduler-core](/oar-scheduler-core)` becomes
   `[oar-scheduler-core](crates/oar-scheduler-core)`) and update the two `raw.githubusercontent.com/.../oar-scheduler-bench/benchmarks/...`
   URLs (`README.md:16`); in the crate READMEs, replace the root-absolute links (`](/oar-scheduler-core)`) by relative ones
   (`](../oar-scheduler-core)`) so that later moves only touch the moved crate. Files concerned: `README.md`,
   `oar-scheduler-core/README.md`, `oar-scheduler-hooks/README.md`, `oar-scheduler-bench/README.md`, `oar-scheduler-redox/README.md`.
8. Delete the stray tracked per-crate lockfiles (`oar-scheduler-bench/Cargo.lock` and `oar-scheduler-core/Cargo.lock`, identical
   leftovers from when those crates built standalone): workspace members share the root lockfile.
9. Add a "Repository layout" section to `README.md` documenting the three root areas (`crates/`, `docs/`, `oar3/`).
10. Update the IDE module file (`oar3-rust.iml`) to the new source roots and drop the entries of the removed `oar3-rust-macros` crate.

### Out of scope (follow-up ADRs)

* Renaming the crates to a consistent `oar-*` scheme once the daemon and CLI crates exist.
* Grouping (`crates/scheduler/`, `crates/daemons/`, ...) if the flat directory proves hard to navigate.
* Location of the SQL schema and configuration templates copied from `oar3/setup` (e.g. `crates/oar-scheduler-db/sql/`).
* Relocating the `oar3/` submodule (e.g. `reference/oar3`), which requires a `.gitmodules` change.
* Introduction of an `xtask` crate for build/CI automation (it would live in `crates/xtask`).

### Consequences

* **Good:** the root stabilizes at a handful of repository-level entries; the 15-20 crates of the full port have a stated home; the
  workspace keeps a single `Cargo.lock`, a single `target/`, one `cargo test --workspace`, and the maturin crate stays a normal
  workspace member.
* **Bad:** one-time churn of a single commit touching the manifest, the flake, the `justfile`, the CI file and the READMEs; external
  bookmarks and the two `raw.githubusercontent.com` image URLs break; every open branch needs a rebase across the move.
* **Mitigation:** do the whole move in one commit so the tree is never half-moved; land it at a quiet time and announce it; run the
  validation checklist below before pushing; keep CI path-agnostic (workspace-root commands) so the next moves are cheaper.

## Pros and Cons of Options

### Option 1: Keep the flat root layout

* **Pros:** no migration at all; every path, link and CI line keeps working.
* **Cons:** the root keeps growing with `oar-*` directories; no stated rule for where the daemon/CLI crates go; repository-level files
  become harder to find as the crate count doubles and triples.

### Option 2: Single `crates/` directory, flat members (chosen)

* **Pros:** conventional Rust monorepo layout; one unambiguous rule for new crates; path dependencies unchanged (siblings); the
  workspace manifest, lockfile and `target/` stay at the root; grouping and renaming stay open for later; low, one-shot migration cost.
* **Cons:** one directory level added to every crate path; a root-level `crates/` is an extra convention that contributors coming from
  the Python tree must learn.

### Option 3: `crates/` grouped by domain

* **Pros:** scales further; the daemon/CLI/lib split is visible in the directory tree.
* **Cons:** the split is not known yet (the port will tell whether `bipbip`, `greta`, `hulot` are daemons or libraries); it forces a
  reclassification per crate and makes `members` patterns longer; nesting hurts greps and shortens the paths shown in tooling output.

### Option 4: Several independent Cargo workspaces

* **Pros:** strong build isolation between domains; each workspace builds and versions independently.
* **Cons:** multiple `Cargo.lock` and `target/` directories; duplicated dependency versions; `cargo test`/`cargo build` no longer cover
  the whole project; shared crates (model, protocol) would need publishing or cross-workspace path dependencies, which is exactly what
  a single workspace avoids today.

## Validation & Compliance

The move is complete when all of the following pass on the reorganized tree:

* `cargo metadata --no-deps --format-version 1` lists exactly the six packages, all with a `crates/` manifest path.
* `cargo build --workspace --all-targets`, `cargo test --workspace` and `cargo clippy --workspace` succeed.
* `cargo build -p oar-scheduler-meta` (the binary entry point) and `just build-scheduler-redox` (maturin) succeed, the latter proving that
  `pyproject.toml` still sits next to the crate manifest.
* `nix build .#oar-scheduler-meta-redox` and `nix build .#oar-scheduler-redox` succeed (the flake paths are the only packaging
  references to the crate directories).
* `grep -rn "](/oar-scheduler-" --include=*.md .` returns nothing: no Markdown link still resolves from the repository root as if the
  crates were at the top level.
* `grep -rn "\(cd \|\./\)oar-scheduler-" flake.nix justfile .gitlab-ci.yml oar-scheduler-bench/src` returns nothing: no build,
  packaging or CI line still `cd`s into, or writes under, the old locations.
* `git log --follow crates/oar-scheduler-core/src/scheduler/kamelot.rs` shows the pre-move history, proving `git mv` was used.
* The GitHub workflow (`cargo build`/`cargo test` at the workspace root) and the GitLab job pass unchanged.

Compliance is reviewed at each new crate: a crate directory that does not live under `crates/`, or a non-crate directory placed under
`crates/`, is a review-blocking violation of this ADR.
