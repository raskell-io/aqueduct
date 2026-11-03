//! The step queue (ADR 0038): steps that remote step workers execute.
//!
//! An engine with remote steps puts every step it has no function for here instead of running it. A
//! worker takes invocations for the step names it implements and holds each under a **lease**. If the
//! result does not come back before the lease ends, or the worker's session ends, the invocation is
//! offered again, with the next attempt number. Delivery is therefore at least once; recording stays
//! exactly once, because the core records the first result for a command and drops the rest.
//!
//! Every method that depends on time takes the clock (`now_ms`), so the queue can be driven by a
//! simulated clock in tests.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use aqueduct_core::{CommandIndex, IdempotencyKey, Payload};

/// A step for a worker to execute: one attempt of one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invocation {
    /// Names this attempt; the worker returns it with the result.
    pub id: String,
    /// The run's run id.
    pub run: String,
    /// The step's command.
    pub cmd: CommandIndex,
    /// The step's name.
    pub step: String,
    /// The step's input.
    pub input: Payload,
    /// The same for every attempt of the command: for deduplication in external systems.
    pub idempotency_key: String,
    /// 1 for the first attempt.
    pub attempt: u32,
    /// How long the worker has for this attempt, in milliseconds.
    pub lease_ms: u64,
}

/// Where a step is: waiting for a worker, or leased to one until a deadline.
#[derive(Debug)]
struct Entry {
    step: String,
    input: Payload,
    /// Attempts offered so far.
    attempts: u32,
    lease: Option<Lease>,
}

#[derive(Debug)]
struct Lease {
    worker: String,
    until_ms: u64,
}

#[derive(Debug, Default)]
struct Inner {
    /// Every step not yet answered, by its run and command.
    entries: BTreeMap<(String, CommandIndex), Entry>,
    /// The order in which waiting steps are offered.
    waiting: VecDeque<(String, CommandIndex)>,
    /// Invocation ids handed out, to the step they belong to.
    issued: BTreeMap<String, (String, CommandIndex)>,
}

/// Steps for remote workers, with leases. Shared between the engine and the transports.
#[derive(Debug)]
pub struct StepQueue {
    inner: Mutex<Inner>,
    ready: Condvar,
    lease_ms: u64,
    clock: Instant,
}

impl StepQueue {
    /// A queue whose leases last `lease`.
    pub fn new(lease: Duration) -> StepQueue {
        StepQueue {
            inner: Mutex::new(Inner::default()),
            ready: Condvar::new(),
            lease_ms: u64::try_from(lease.as_millis()).unwrap_or(u64::MAX).max(1),
            clock: Instant::now(),
        }
    }

    /// The queue's own clock, for callers without a simulated one: milliseconds since it was created.
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A poisoned lock only means a holder panicked; the queue's state stays consistent.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Offers a step for execution. A step already waiting or leased is not offered twice (the engine
    /// re-sends dispatches periodically); one whose lease has run out is offered again.
    pub fn offer(&self, run: &str, cmd: CommandIndex, step: &str, input: Payload, now_ms: u64) {
        let mut inner = self.lock();
        let target = (run.to_owned(), cmd);
        if let Some(entry) = inner.entries.get(&target) {
            match &entry.lease {
                None => return,
                Some(lease) if lease.until_ms > now_ms => return,
                Some(_) => {}
            }
        }
        let entry = inner.entries.entry(target.clone()).or_insert(Entry {
            step: step.to_owned(),
            input,
            attempts: 0,
            lease: None,
        });
        entry.lease = None;
        inner.waiting.push_back(target);
        drop(inner);
        self.ready.notify_all();
    }

    /// Takes up to `max` waiting invocations of the named steps for `worker`, leasing each until
    /// `now_ms` plus the lease. Leases that have run out are offered again first.
    pub fn take(&self, worker: &str, steps: &[String], max: usize, now_ms: u64) -> Vec<Invocation> {
        let mut inner = self.lock();
        Self::expire_locked(&mut inner, now_ms);
        let mut taken = Vec::new();
        let mut kept = VecDeque::new();
        while let Some(target) = inner.waiting.pop_front() {
            let wanted = taken.len() < max
                && inner
                    .entries
                    .get(&target)
                    .is_some_and(|e| e.lease.is_none() && steps.contains(&e.step));
            if !wanted {
                if inner
                    .entries
                    .get(&target)
                    .is_some_and(|e| e.lease.is_none())
                {
                    kept.push_back(target);
                }
                continue;
            }
            let Some(entry) = inner.entries.get_mut(&target) else {
                continue;
            };
            entry.attempts += 1;
            let id = format!("{}#{}.{}", target.0, target.1, entry.attempts);
            entry.lease = Some(Lease {
                worker: worker.to_owned(),
                until_ms: now_ms.saturating_add(self.lease_ms),
            });
            taken.push(Invocation {
                id: id.clone(),
                run: target.0.clone(),
                cmd: target.1,
                step: entry.step.clone(),
                input: entry.input.clone(),
                idempotency_key: IdempotencyKey::new(&target.0, 0, target.1).to_string(),
                attempt: entry.attempts,
                lease_ms: self.lease_ms,
            });
            inner.issued.insert(id, target);
        }
        inner.waiting = kept;
        taken
    }

    /// [`StepQueue::take`] by the queue's own clock, waiting up to `wait` for something to take.
    pub fn wait_take(
        &self,
        worker: &str,
        steps: &[String],
        max: usize,
        wait: Duration,
    ) -> Vec<Invocation> {
        let deadline = Instant::now() + wait;
        loop {
            let taken = self.take(worker, steps, max, self.now_ms());
            let left = deadline.saturating_duration_since(Instant::now());
            if !taken.is_empty() || left.is_zero() || max == 0 {
                return taken;
            }
            // Woken by an offer, or after a while to look at expired leases.
            let inner = self.lock();
            let _ = self
                .ready
                .wait_timeout(inner, left.min(Duration::from_millis(250)));
        }
    }

    /// A worker's result for invocation `id`: the step it answers, if the queue still waits for that
    /// step (whether the lease is current or has run out, the first result counts). `None` for a
    /// result the queue no longer needs: a duplicate, or one for a step already answered.
    pub fn complete(&self, id: &str) -> Option<(String, CommandIndex)> {
        let mut inner = self.lock();
        let target = inner.issued.remove(id)?;
        inner.entries.remove(&target)?;
        inner.issued.retain(|_, t| *t != target);
        inner.waiting.retain(|t| *t != target);
        Some(target)
    }

    /// A worker's session ended: its leases are offered again at once.
    pub fn release(&self, worker: &str) {
        let mut inner = self.lock();
        let mut released = Vec::new();
        for (target, entry) in &mut inner.entries {
            if entry.lease.as_ref().is_some_and(|l| l.worker == worker) {
                entry.lease = None;
                released.push(target.clone());
            }
        }
        let any = !released.is_empty();
        inner.waiting.extend(released);
        drop(inner);
        if any {
            self.ready.notify_all();
        }
    }

    fn expire_locked(inner: &mut Inner, now_ms: u64) {
        let mut expired = Vec::new();
        for (target, entry) in &mut inner.entries {
            if entry.lease.as_ref().is_some_and(|l| l.until_ms <= now_ms) {
                entry.lease = None;
                expired.push(target.clone());
            }
        }
        inner.waiting.extend(expired);
    }

    /// Steps not yet answered: waiting or leased.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether every step offered has been answered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The step names waiting for a worker, with how many are waiting for each.
    pub fn waiting(&self) -> BTreeMap<String, usize> {
        let inner = self.lock();
        let mut waiting = BTreeMap::new();
        for entry in inner.entries.values().filter(|e| e.lease.is_none()) {
            *waiting.entry(entry.step.clone()).or_default() += 1;
        }
        waiting
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue() -> StepQueue {
        StepQueue::new(Duration::from_millis(100))
    }

    fn names(steps: &[&str]) -> Vec<String> {
        steps.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_step_goes_to_a_worker_that_implements_it() {
        let q = queue();
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 0);
        q.offer("r", CommandIndex(1), "ship", Payload::new(*b"2"), 0);
        assert!(q.take("w", &names(&["refund"]), 10, 0).is_empty());
        let taken = q.take("w", &names(&["ship"]), 10, 0);
        assert_eq!(taken.len(), 1);
        assert_eq!((taken[0].step.as_str(), taken[0].attempt), ("ship", 1));
        assert_eq!(taken[0].idempotency_key, "r/0/1");
        assert_eq!(
            q.complete(&taken[0].id),
            Some(("r".to_owned(), CommandIndex(1)))
        );
        // A duplicate result is not needed.
        assert_eq!(q.complete(&taken[0].id), None);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn an_expired_lease_is_offered_again_and_the_first_result_counts() {
        let q = queue();
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 0);
        let first = q.take("slow", &names(&["charge"]), 1, 0);
        // Re-sent dispatches do not offer a leased step again.
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 50);
        assert!(q.take("fast", &names(&["charge"]), 1, 99).is_empty());
        let second = q.take("fast", &names(&["charge"]), 1, 100);
        assert_eq!(second[0].attempt, 2);
        // The slow worker answers late: still the first result, so it counts.
        assert!(q.complete(&first[0].id).is_some());
        assert_eq!(q.complete(&second[0].id), None);
        assert!(q.is_empty());
    }

    #[test]
    fn a_session_that_ends_releases_its_leases() {
        let q = queue();
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 0);
        q.take("gone", &names(&["charge"]), 1, 0);
        q.release("gone");
        let again = q.take("other", &names(&["charge"]), 1, 1);
        assert_eq!(again[0].attempt, 2);
    }
}
