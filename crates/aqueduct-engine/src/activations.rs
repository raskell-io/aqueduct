//! The activation queue (ADR 0039): activations of runs whose workflows remote workflow workers decide.
//!
//! An activation asks a workflow's logic for its commands, given the run's history so far. Each run has
//! at most one activation here: a newer one (for a longer history) replaces an older one. A worker takes
//! activations for the workflows it serves and holds each under a lease; one whose decision does not
//! come back in time, or whose worker's session ends, is offered again. A decision may still arrive for
//! an activation that was replaced: the partition drops a decision for a history that is no longer the
//! run's ([`crate::Partition::decision`]), so being late is harmless.
//!
//! Every method that depends on time takes the clock (`now_ms`), so the queue can be simulated.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use aqueduct_core::Env;

use crate::namespaces::Scope;

/// An activation for a worker: decide run `run` at the first `at` entries of its history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Activation {
    /// Names this activation; the worker returns it with the decision.
    pub id: String,
    /// The run id.
    pub run: String,
    /// The workflow.
    pub workflow: String,
    /// How many history entries the decision is for.
    pub at: usize,
    /// The run's history: its first `at` records in the history format, hash-chained to the run id.
    pub history: Vec<Vec<u8>>,
    /// The clock and seed for values the logic records.
    pub env: Env,
    /// How long the worker has, in milliseconds.
    pub lease_ms: u64,
}

#[derive(Debug)]
struct Entry {
    workflow: String,
    at: usize,
    history: Vec<Vec<u8>>,
    env: Env,
    offers: u64,
    /// The worker holding it, and until when.
    lease: Option<(String, u64)>,
}

#[derive(Debug, Default)]
struct Inner {
    /// The pending activation of each run.
    entries: BTreeMap<String, Entry>,
    waiting: VecDeque<String>,
    /// Activation ids handed out, to the run and history length they are for.
    issued: BTreeMap<String, (String, usize)>,
}

/// Activations for remote workflow workers, with leases.
#[derive(Debug)]
pub struct ActivationQueue {
    inner: Mutex<Inner>,
    ready: Condvar,
    lease_ms: u64,
    clock: Instant,
}

impl ActivationQueue {
    /// A queue whose leases last `lease`.
    pub fn new(lease: Duration) -> ActivationQueue {
        ActivationQueue {
            inner: Mutex::new(Inner::default()),
            ready: Condvar::new(),
            lease_ms: u64::try_from(lease.as_millis()).unwrap_or(u64::MAX).max(1),
            clock: Instant::now(),
        }
    }

    /// The queue's own clock: milliseconds since it was created.
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Offers an activation of `run` at `at` history entries. An older one is replaced; the same one,
    /// waiting or under a lease that has not run out, is not offered twice.
    pub fn offer(
        &self,
        run: &str,
        workflow: &str,
        at: usize,
        history: Vec<Vec<u8>>,
        env: Env,
        now_ms: u64,
    ) {
        let mut inner = self.lock();
        if let Some(entry) = inner.entries.get(run) {
            let current = entry
                .lease
                .as_ref()
                .is_none_or(|(_, until)| *until > now_ms);
            if entry.at > at || (entry.at == at && current) {
                return;
            }
        }
        let offers = inner.entries.get(run).map_or(0, |e| e.offers);
        inner.entries.insert(
            run.to_owned(),
            Entry {
                workflow: workflow.to_owned(),
                at,
                history,
                env,
                offers,
                lease: None,
            },
        );
        inner.waiting.push_back(run.to_owned());
        drop(inner);
        self.ready.notify_all();
    }

    /// Takes up to `max` waiting activations of the named workflows for `worker`, leasing each.
    pub fn take(
        &self,
        worker: &str,
        workflows: &[String],
        max: usize,
        now_ms: u64,
    ) -> Vec<Activation> {
        self.take_in(&Scope::All, worker, workflows, max, now_ms)
    }

    /// [`ActivationQueue::take`], of runs in `scope` only.
    pub fn take_in(
        &self,
        scope: &Scope,
        worker: &str,
        workflows: &[String],
        max: usize,
        now_ms: u64,
    ) -> Vec<Activation> {
        let mut inner = self.lock();
        // Leases that have run out are offered again.
        let mut expired = Vec::new();
        for (run, entry) in &mut inner.entries {
            if entry
                .lease
                .as_ref()
                .is_some_and(|(_, until)| *until <= now_ms)
            {
                entry.lease = None;
                expired.push(run.clone());
            }
        }
        inner.waiting.extend(expired);
        let mut taken = Vec::new();
        let mut kept = VecDeque::new();
        while let Some(run) = inner.waiting.pop_front() {
            let Some(entry) = inner.entries.get_mut(&run) else {
                continue;
            };
            if entry.lease.is_some() {
                continue;
            }
            if taken.len() >= max || !workflows.contains(&entry.workflow) || !scope.contains(&run) {
                kept.push_back(run);
                continue;
            }
            entry.offers += 1;
            entry.lease = Some((worker.to_owned(), now_ms.saturating_add(self.lease_ms)));
            let id = format!("{run}@{}.{}", entry.at, entry.offers);
            taken.push(Activation {
                id: id.clone(),
                run: run.clone(),
                workflow: entry.workflow.clone(),
                at: entry.at,
                history: entry.history.clone(),
                env: entry.env,
                lease_ms: self.lease_ms,
            });
            let at = entry.at;
            inner.issued.insert(id, (run, at));
        }
        inner.waiting = kept;
        taken
    }

    /// [`ActivationQueue::take`] by the queue's own clock, waiting up to `wait` for something to take.
    pub fn wait_take(
        &self,
        worker: &str,
        workflows: &[String],
        max: usize,
        wait: Duration,
    ) -> Vec<Activation> {
        self.wait_take_in(&Scope::All, worker, workflows, max, wait)
    }

    /// [`ActivationQueue::wait_take`], of runs in `scope` only.
    pub fn wait_take_in(
        &self,
        scope: &Scope,
        worker: &str,
        workflows: &[String],
        max: usize,
        wait: Duration,
    ) -> Vec<Activation> {
        let deadline = Instant::now() + wait;
        loop {
            let taken = self.take_in(scope, worker, workflows, max, self.now_ms());
            let left = deadline.saturating_duration_since(Instant::now());
            if !taken.is_empty() || left.is_zero() || max == 0 {
                return taken;
            }
            let inner = self.lock();
            let _ = self
                .ready
                .wait_timeout(inner, left.min(Duration::from_millis(250)));
        }
    }

    /// A worker's decision for activation `id`: the run and history length it is for, if the id was
    /// handed out. The run's pending activation is done if it is the one decided; a decision for an
    /// older one is still returned (the partition drops it as stale).
    pub fn complete(&self, id: &str) -> Option<(String, usize)> {
        self.complete_in(&Scope::All, id)
    }

    /// [`ActivationQueue::complete`], for a worker that serves `scope`: a decision for a run outside
    /// it is not taken. Activation ids are not secrets.
    pub fn complete_in(&self, scope: &Scope, id: &str) -> Option<(String, usize)> {
        let mut inner = self.lock();
        if !scope.contains(&inner.issued.get(id)?.0) {
            return None;
        }
        let (run, at) = inner.issued.remove(id)?;
        if inner.entries.get(&run).is_some_and(|e| e.at == at) {
            inner.entries.remove(&run);
            inner.issued.retain(|_, (r, _)| *r != run);
            inner.waiting.retain(|r| *r != run);
        }
        Some((run, at))
    }

    /// A worker's session ended: its leases are offered again at once.
    pub fn release(&self, worker: &str) {
        let mut inner = self.lock();
        let mut released = Vec::new();
        for (run, entry) in &mut inner.entries {
            if entry.lease.as_ref().is_some_and(|(w, _)| w == worker) {
                entry.lease = None;
                released.push(run.clone());
            }
        }
        let any = !released.is_empty();
        inner.waiting.extend(released);
        drop(inner);
        if any {
            self.ready.notify_all();
        }
    }

    /// Activations not yet decided.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether every activation offered has been decided.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workflows() -> Vec<String> {
        vec!["orders".to_owned()]
    }

    #[test]
    fn a_newer_activation_replaces_an_older_one() {
        let q = ActivationQueue::new(Duration::from_millis(100));
        q.offer("r", "orders", 1, vec![b"a".to_vec()], Env::default(), 0);
        let old = q.take("w", &workflows(), 1, 0);
        q.offer("r", "orders", 3, vec![b"a".to_vec(); 3], Env::default(), 10);
        // An older one cannot replace the newer one.
        q.offer("r", "orders", 2, vec![b"a".to_vec(); 2], Env::default(), 10);
        let new = q.take("w", &workflows(), 1, 10);
        assert_eq!(new[0].at, 3);
        // The old decision arrives: returned, so the partition can drop it; the new one is pending.
        assert_eq!(q.complete(&old[0].id), Some(("r".to_owned(), 1)));
        assert_eq!(q.len(), 1);
        assert_eq!(q.complete(&new[0].id), Some(("r".to_owned(), 3)));
        assert!(q.is_empty());
    }

    #[test]
    fn a_lost_activation_is_offered_again() {
        let q = ActivationQueue::new(Duration::from_millis(100));
        q.offer("r", "orders", 1, Vec::new(), Env::default(), 0);
        q.take("w", &workflows(), 1, 0);
        assert!(q.take("w", &workflows(), 1, 99).is_empty());
        assert_eq!(q.take("w", &workflows(), 1, 100).len(), 1);
        q.release("w");
        assert_eq!(q.take("other", &workflows(), 1, 101).len(), 1);
    }
}
