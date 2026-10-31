# 0006: Deterministic simulation and the async runtime

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md section 10.1 requires the whole engine to run under a seeded, single-threaded simulator with fault
injection, while production uses tokio. Running tokio itself under simulation (current-thread runtime, paused
time, turmoil) is convenient but not fully deterministic: task wake-up order, timers and I/O readiness depend
on runtime internals.

## Options

1. Simulate tokio (turmoil plus paused time), accepting residual non-determinism.
2. Make the engine runtime-agnostic with an executor trait, and write a deterministic executor.
3. **Sans-I/O:** write every engine component (partition processor, invoker, timer service) as a synchronous
   state machine that consumes messages and returns messages and storage operations. The simulator drives
   them from one seeded event queue; production drives them from thin tokio tasks.

## Decision

Option 3. The pure core (`aqueduct-core`) is already sans-I/O; the engine layer follows the same pattern.
Async code exists only in drivers, which are kept small enough to review, and are covered by integration
and crash tests on real disks rather than by the simulator.

The simulator owns its random number generator (SplitMix64, implemented in `aqueduct-sim`) instead of
depending on `rand`, so that a seed reproduces the same run forever, across dependency upgrades. All
collections the simulator or engine iterate are ordered (`BTreeMap`, `Vec`), never `HashMap`.

## Consequences

- Every simulated failure reproduces exactly with `cargo xtask sim --seed <n>`.
- The drivers are a gap in simulation coverage. Keep them thin and test them with the crash harness.
- turmoil remains an option for testing network drivers in phase 3, as an addition, not as the foundation.
