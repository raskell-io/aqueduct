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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use aqueduct_core::Env;

use crate::namespaces::{Scope, namespace_of};

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
    /// Its place in the waiting list, while it has no lease. Another place in the list is out of date.
    place: u64,
    /// The activation ids handed out for the run.
    ids: Vec<String>,
    /// The build the run is pinned to (ADR 0048), if any.
    version: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    /// The pending activation of each run.
    entries: BTreeMap<String, Entry>,
    /// The order in which waiting activations are offered, by namespace and workflow. Places are in
    /// offer order. A run leased or decided leaves its place behind, dropped when it is reached, so that
    /// nothing here costs more as the backlog grows.
    waiting: BTreeMap<List, VecDeque<(u64, String)>>,
    next_place: u64,
    /// The runs under a lease, by when it runs out.
    leases: BTreeSet<(u64, String)>,
    /// Connected workers, by workflow and build.
    sessions: BTreeMap<(String, Option<String>), usize>,
    /// Activation ids handed out, to the run and history length they are for.
    issued: BTreeMap<String, (String, usize)>,
}

/// A waiting list: a namespace, a workflow, and the build its runs are pinned to.
type List = (String, String, Option<String>);

impl Inner {
    /// Gives a run's activation a place at the end of the waiting list.
    fn enqueue(&mut self, run: String) {
        let place = self.next_place;
        self.next_place += 1;
        let Some(entry) = self.entries.get_mut(&run) else {
            return;
        };
        entry.place = place;
        let list = (
            namespace_of(&run).to_owned(),
            entry.workflow.clone(),
            entry.version.clone(),
        );
        self.waiting
            .entry(list)
            .or_default()
            .push_back((place, run));
    }

    /// The first waiting activation for these workflows in scope, by offer order: its list. A pinned run
    /// goes only to a worker running its build: other code must never decide it.
    fn next_in(
        &mut self,
        scope: &Scope,
        workflows: &[String],
        build: Option<&str>,
    ) -> Option<List> {
        let mut next: Option<(u64, List)> = None;
        for (list, waiting) in &mut self.waiting {
            let pinned_elsewhere = list.2.as_deref().is_some_and(|pin| Some(pin) != build);
            if !scope.includes(&list.0) || !workflows.contains(&list.1) || pinned_elsewhere {
                continue;
            }
            while let Some((place, run)) = waiting.front() {
                let current = self
                    .entries
                    .get(run)
                    .is_some_and(|entry| entry.lease.is_none() && entry.place == *place);
                if current {
                    break;
                }
                waiting.pop_front();
            }
            if let Some((place, _)) = waiting.front()
                && next.as_ref().is_none_or(|(first, _)| place < first)
            {
                next = Some((*place, list.clone()));
            }
        }
        next.map(|(_, list)| list)
    }
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
        self.offer_pinned(run, workflow, None, at, history, env, now_ms);
    }

    /// [`ActivationQueue::offer`], for a run pinned to a build (ADR 0048).
    #[allow(clippy::too_many_arguments)]
    pub fn offer_pinned(
        &self,
        run: &str,
        workflow: &str,
        version: Option<&str>,
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
        let previous = inner.entries.remove(run);
        let (offers, ids, waiting) = match previous {
            Some(previous) => {
                if let Some((_, until)) = &previous.lease {
                    inner.leases.remove(&(*until, run.to_owned()));
                }
                // One already waiting keeps its place.
                let waiting = previous.lease.is_none().then_some(previous.place);
                (previous.offers, previous.ids, waiting)
            }
            None => (0, Vec::new(), None),
        };
        inner.entries.insert(
            run.to_owned(),
            Entry {
                workflow: workflow.to_owned(),
                at,
                history,
                env,
                offers,
                lease: None,
                place: waiting.unwrap_or(0),
                ids,
                version: version.map(str::to_owned),
            },
        );
        if waiting.is_none() {
            inner.enqueue(run.to_owned());
        }
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
        self.take_for(scope, None, worker, workflows, max, now_ms)
    }

    /// [`ActivationQueue::take_in`], for a worker running a build (ADR 0048).
    pub fn take_for(
        &self,
        scope: &Scope,
        build: Option<&str>,
        worker: &str,
        workflows: &[String],
        max: usize,
        now_ms: u64,
    ) -> Vec<Activation> {
        let mut inner = self.lock();
        // Leases that have run out are offered again, in a fixed order.
        let mut expired = Vec::new();
        while let Some((until, _)) = inner.leases.first() {
            if *until > now_ms {
                break;
            }
            if let Some((_, run)) = inner.leases.pop_first() {
                if let Some(entry) = inner.entries.get_mut(&run) {
                    entry.lease = None;
                }
                expired.push(run);
            }
        }
        expired.sort();
        for run in expired {
            inner.enqueue(run);
        }
        let mut taken = Vec::new();
        while taken.len() < max {
            let Some(list) = inner.next_in(scope, workflows, build) else {
                break;
            };
            let Some((_, run)) = inner.waiting.get_mut(&list).and_then(VecDeque::pop_front) else {
                break;
            };
            let Some(entry) = inner.entries.get_mut(&run) else {
                continue;
            };
            entry.offers += 1;
            let until = now_ms.saturating_add(self.lease_ms);
            entry.lease = Some((worker.to_owned(), until));
            let id = format!("{run}@{}.{}", entry.at, entry.offers);
            entry.ids.push(id.clone());
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
            inner.leases.insert((until, run.clone()));
            inner.issued.insert(id, (run, at));
        }
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
        self.wait_take_for(scope, None, worker, workflows, max, wait)
    }

    /// [`ActivationQueue::wait_take_in`], for a worker running a build (ADR 0048).
    pub fn wait_take_for(
        &self,
        scope: &Scope,
        build: Option<&str>,
        worker: &str,
        workflows: &[String],
        max: usize,
        wait: Duration,
    ) -> Vec<Activation> {
        let deadline = Instant::now() + wait;
        loop {
            let taken = self.take_for(scope, build, worker, workflows, max, self.now_ms());
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
        if inner.entries.get(&run).is_some_and(|e| e.at == at)
            && let Some(entry) = inner.entries.remove(&run)
        {
            for id in &entry.ids {
                inner.issued.remove(id);
            }
            if let Some((_, until)) = &entry.lease {
                inner.leases.remove(&(*until, run.clone()));
            }
        }
        Some((run, at))
    }

    /// A worker of these workflows connected, running `build`. It counts until [`Self::disconnected`].
    pub fn connected(&self, workflows: &[String], build: Option<&str>) {
        let mut inner = self.lock();
        for workflow in workflows {
            *inner
                .sessions
                .entry((workflow.clone(), build.map(str::to_owned)))
                .or_default() += 1;
        }
    }

    /// A worker counted by [`Self::connected`] went away.
    pub fn disconnected(&self, workflows: &[String], build: Option<&str>) {
        let mut inner = self.lock();
        for workflow in workflows {
            let key = (workflow.clone(), build.map(str::to_owned));
            if let Some(count) = inner.sessions.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    inner.sessions.remove(&key);
                }
            }
        }
    }

    /// How many connected workers there are, by workflow and build.
    pub fn sessions(&self) -> BTreeMap<(String, Option<String>), usize> {
        self.lock().sessions.clone()
    }

    /// A worker's session ended: its leases are offered again at once.
    pub fn release(&self, worker: &str) {
        let mut inner = self.lock();
        let mut released = Vec::new();
        for (run, entry) in &mut inner.entries {
            if let Some((_, until)) = entry.lease.take_if(|(holder, _)| holder == worker) {
                released.push((until, run.clone()));
            }
        }
        let any = !released.is_empty();
        for (until, run) in released {
            inner.leases.remove(&(until, run.clone()));
            inner.enqueue(run);
        }
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
