# Crate oar-scheduler-hooks

## Overview

This crate allows sysadmins to define rust functions (hooks) that are called by the scheduler at specific points in the scheduling process, allowing
to overwrite default behavior.

The scheduler entrypoints, `oar-scheduler-redox` or `oar-scheduler-meta` are creating instances of the struct `oar_scheduler_hooks::Hooks` and call a
function of
`oar-scheduler-core` to register the hooks, allowing to keep a non-circular dependency graph:

```rust
// Register plugin hooks from the oar-scheduler-hooks crate into the oar-scheduler-core crate
if let Some(hooks) = oar_scheduler_hooks::Hooks::new() {
oar_scheduler_core::hooks::set_hooks_handler(hooks);
}
```

The `new` function of `Hooks` is called at the beginning of the process, and can return `None` to disable the hook system.
Hook functions return either a `bool` or an `Option<T>`. If `false` or `None` is returned, the default behavior is applied. If `true` or `Some(value)`
is returned, the default behavior is overridden.

**List of available hooks:**

- `sort`: Overrides the job sorting algorithm.
- `assign`: Overrides the job assignment logic for a single job on a given slotset.
- `find`: Overrides the resources request evaluation logic.

Look at `oar-scheduler-core/hooks.rs` for more details on the available hooks and their usage.

## Usage

This crate should keep a fixed structure exposing the struct `Hooks` with a `pub fn new() -> Option<Self>` function,
and implementing the trait `HooksHandler`.

To create custom hooks, either clone the repository and edit directly the `oar-scheduler-hooks` crate, or create a new crate with the same structure
as `oar-scheduler-hooks`,
and replace the `oar-scheduler-hooks` dependency in `oar-scheduler-redox/Cargo.toml` with your custom crate.

In either case, you will need to rebuild the Rust scheduler.
