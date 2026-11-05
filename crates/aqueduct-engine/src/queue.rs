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

use crate::namespaces::{Scope, namespace_of};

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
    /// The queue it waits in: the step's route, by default its own name.
    queue: String,
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
    /// Stride scheduling between namespaces: the next to be served is the one with work waiting and the
    /// lowest pass, which rises by 1/weight each time it is served. A namespace coming back from idle
    /// starts at the lowest pass of those with work waiting, so idling earns no credit.
    pass: BTreeMap<String, f64>,
    /// The namespaces with work waiting at the last take across namespaces.
    backlogged: std::collections::BTreeSet<String>,
}

/// Steps for remote workers, with leases. Shared between the engine and the transports.
#[derive(Debug)]
pub struct StepQueue {
    inner: Mutex<Inner>,
    ready: Condvar,
    lease_ms: u64,
    clock: Instant,
    /// Steps routed to named task queues (ADR 0040), by step name.
    routes: BTreeMap<String, String>,
    /// Namespaces' shares of a shared worker pool (ADR 0046); 1 by default.
    weights: BTreeMap<String, f64>,
}

impl StepQueue {
    /// A queue whose leases last `lease`, where every step waits in the queue of its own name.
    pub fn new(lease: Duration) -> StepQueue {
        StepQueue::with_routes(lease, BTreeMap::new())
    }

    /// A queue whose leases last `lease`, with steps routed to named task queues: `routes` maps step
    /// names to queue names; a step without a route waits in the queue of its own name.
    pub fn with_routes(lease: Duration, routes: BTreeMap<String, String>) -> StepQueue {
        StepQueue {
            inner: Mutex::new(Inner::default()),
            ready: Condvar::new(),
            lease_ms: u64::try_from(lease.as_millis()).unwrap_or(u64::MAX).max(1),
            clock: Instant::now(),
            routes,
            weights: BTreeMap::new(),
        }
    }

    /// Sets namespaces' shares of workers that serve several namespaces. Others have a weight of 1.
    pub fn with_weights(mut self, weights: BTreeMap<String, f64>) -> StepQueue {
        self.weights = weights;
        self
    }

    /// The queue a step waits in.
    pub fn queue_of<'a>(&'a self, step: &'a str) -> &'a str {
        self.routes.get(step).map_or(step, String::as_str)
    }

    /// The queue's own clock, for callers without a simulated one: milliseconds since it was created.
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A poisoned lock only means a holder panicked; the queue's state stays consistent.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Offers attempt `attempt` of a step for execution (the run's recorded failed attempts, plus one).
    /// A step already waiting or leased is not offered twice (the engine re-sends dispatches
    /// periodically); one whose lease has run out is offered again, and an attempt offered again after
    /// a lost lease counts as the next.
    pub fn offer(
        &self,
        run: &str,
        cmd: CommandIndex,
        step: &str,
        input: Payload,
        attempt: u32,
        now_ms: u64,
    ) {
        let mut inner = self.lock();
        let target = (run.to_owned(), cmd);
        if let Some(entry) = inner.entries.get(&target) {
            match &entry.lease {
                None => return,
                Some(lease) if lease.until_ms > now_ms => return,
                Some(_) => {}
            }
        }
        let queue = self.queue_of(step).to_owned();
        let entry = inner.entries.entry(target.clone()).or_insert(Entry {
            step: step.to_owned(),
            queue,
            input,
            attempts: 0,
            lease: None,
        });
        entry.lease = None;
        // Never below what the run has recorded.
        entry.attempts = entry.attempts.max(attempt.saturating_sub(1));
        inner.waiting.push_back(target);
        drop(inner);
        self.ready.notify_all();
    }

    /// Takes up to `max` waiting invocations from the named queues (a step's queue is its route, by
    /// default its own name) for `worker`, leasing each until `now_ms` plus the lease. Leases that have
    /// run out are offered again first.
    pub fn take(
        &self,
        worker: &str,
        queues: &[String],
        max: usize,
        now_ms: u64,
    ) -> Vec<Invocation> {
        self.take_in(&Scope::All, worker, queues, max, now_ms)
    }

    /// [`StepQueue::take`], of the steps of runs in `scope` only. Among several namespaces, steps are
    /// taken by weighted fair queueing, so a namespace with many steps waiting cannot starve the others;
    /// within a namespace, in the order offered.
    pub fn take_in(
        &self,
        scope: &Scope,
        worker: &str,
        queues: &[String],
        max: usize,
        now_ms: u64,
    ) -> Vec<Invocation> {
        let mut inner = self.lock();
        Self::expire_locked(&mut inner, now_ms);
        let inner = &mut *inner;
        // What can be taken, by namespace, in the order offered; a duplicate or a step answered is
        // dropped from the waiting list.
        let mut candidates: BTreeMap<String, VecDeque<(String, CommandIndex)>> = BTreeMap::new();
        let mut kept = VecDeque::new();
        let mut seen = std::collections::BTreeSet::new();
        for target in inner.waiting.drain(..) {
            let Some(entry) = inner.entries.get(&target) else {
                continue;
            };
            if entry.lease.is_some() || !seen.insert(target.clone()) {
                continue;
            }
            if queues.contains(&entry.queue) && scope.contains(&target.0) {
                candidates
                    .entry(namespace_of(&target.0).to_owned())
                    .or_default()
                    .push_back(target.clone());
            }
            kept.push_back(target);
        }
        inner.waiting = kept;
        // Those coming back from idle catch up. (A worker of one namespace has no one to be fair to.)
        if *scope == Scope::All {
            let floor = candidates
                .keys()
                .filter(|namespace| inner.backlogged.contains(*namespace))
                .filter_map(|namespace| inner.pass.get(namespace))
                .copied()
                .min_by(f64::total_cmp);
            if let Some(floor) = floor {
                for namespace in candidates.keys() {
                    if !inner.backlogged.contains(namespace) {
                        let pass = inner.pass.entry(namespace.clone()).or_default();
                        *pass = pass.max(floor);
                    }
                }
            }
        }
        let mut taken = Vec::new();
        while taken.len() < max {
            let next = candidates
                .iter()
                .filter(|(_, waiting)| !waiting.is_empty())
                .map(|(namespace, _)| {
                    let pass = inner.pass.get(namespace).copied().unwrap_or(0.0);
                    (namespace.clone(), pass)
                })
                .min_by(|a, b| a.1.total_cmp(&b.1));
            let Some((namespace, pass)) = next else {
                break;
            };
            let Some(target) = candidates.get_mut(&namespace).and_then(VecDeque::pop_front) else {
                break;
            };
            let weight = self.weights.get(&namespace).copied().unwrap_or(1.0);
            inner.pass.insert(namespace, pass + 1.0 / weight);
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
            inner.waiting.retain(|waiting| *waiting != target);
            inner.issued.insert(id, target);
        }
        if *scope == Scope::All {
            inner.backlogged = candidates
                .into_iter()
                .filter(|(_, waiting)| !waiting.is_empty())
                .map(|(namespace, _)| namespace)
                .collect();
        }
        taken
    }

    /// [`StepQueue::take`] by the queue's own clock, waiting up to `wait` for something to take.
    pub fn wait_take(
        &self,
        worker: &str,
        queues: &[String],
        max: usize,
        wait: Duration,
    ) -> Vec<Invocation> {
        self.wait_take_in(&Scope::All, worker, queues, max, wait)
    }

    /// [`StepQueue::wait_take`], of the steps of runs in `scope` only.
    pub fn wait_take_in(
        &self,
        scope: &Scope,
        worker: &str,
        queues: &[String],
        max: usize,
        wait: Duration,
    ) -> Vec<Invocation> {
        let deadline = Instant::now() + wait;
        loop {
            let taken = self.take_in(scope, worker, queues, max, self.now_ms());
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
        self.complete_in(&Scope::All, id)
    }

    /// [`StepQueue::complete`], for a worker that serves `scope`: a result for a step outside it is
    /// not one the queue needs. Invocation ids are not secrets.
    pub fn complete_in(&self, scope: &Scope, id: &str) -> Option<(String, CommandIndex)> {
        let mut inner = self.lock();
        if !scope.contains(&inner.issued.get(id)?.0) {
            return None;
        }
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
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 1, 0);
        q.offer("r", CommandIndex(1), "ship", Payload::new(*b"2"), 1, 0);
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
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 1, 0);
        let first = q.take("slow", &names(&["charge"]), 1, 0);
        // Re-sent dispatches do not offer a leased step again.
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 1, 50);
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
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 1, 0);
        q.take("gone", &names(&["charge"]), 1, 0);
        q.release("gone");
        let again = q.take("other", &names(&["charge"]), 1, 1);
        assert_eq!(again[0].attempt, 2);
    }

    #[test]
    fn steps_routed_to_a_named_queue_go_to_its_workers() {
        let routes = [("charge".to_owned(), "billing".to_owned())].into();
        let q = StepQueue::with_routes(Duration::from_millis(100), routes);
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 1, 0);
        q.offer("r", CommandIndex(1), "ship", Payload::new(*b"2"), 1, 0);
        // A worker that names the step but not its queue does not get it.
        assert!(q.take("w", &names(&["charge"]), 10, 0).is_empty());
        let billing = q.take("w", &names(&["billing"]), 10, 0);
        assert_eq!(billing.len(), 1);
        assert_eq!(billing[0].step, "charge");
        // Unrouted steps keep the queue of their own name.
        assert_eq!(q.take("w", &names(&["ship"]), 10, 0).len(), 1);
    }

    #[test]
    fn workers_get_only_their_namespaces_steps() {
        let q = queue();
        q.offer(
            "$ns/acme/r",
            CommandIndex(0),
            "s",
            Payload::new(*b"1"),
            1,
            0,
        );
        q.offer(
            "$ns/globex/r",
            CommandIndex(0),
            "s",
            Payload::new(*b"2"),
            1,
            0,
        );
        q.offer("r", CommandIndex(0), "s", Payload::new(*b"3"), 1, 0);
        let one = |n: &str| Scope::One(n.to_owned());
        let acme = q.take_in(&one("acme"), "w", &names(&["s"]), 10, 0);
        assert_eq!(acme.len(), 1);
        assert_eq!(acme[0].run, "$ns/acme/r");
        assert_eq!(
            q.take_in(&one("default"), "w", &names(&["s"]), 10, 0)[0].run,
            "r"
        );
        assert_eq!(
            q.take_in(&Scope::All, "w", &names(&["s"]), 10, 0)[0].run,
            "$ns/globex/r"
        );
    }

    /// Takes every step one at a time: which namespace each came from.
    fn order(q: &StepQueue) -> Vec<String> {
        std::iter::from_fn(|| q.take("w", &names(&["s"]), 1, 0).into_iter().next())
            .map(|i| namespace_of(&i.run).to_owned())
            .collect()
    }

    #[test]
    fn a_noisy_namespace_cannot_starve_the_others() {
        let q = queue();
        for i in 0..1_000 {
            q.offer(
                "$ns/noisy/r",
                CommandIndex(i),
                "s",
                Payload::new(*b"1"),
                1,
                0,
            );
        }
        for i in 0..10 {
            q.offer(
                "$ns/quiet/r",
                CommandIndex(i),
                "s",
                Payload::new(*b"1"),
                1,
                0,
            );
        }
        let order = order(&q);
        assert_eq!(order.len(), 1_010);
        // Despite arriving last, the quiet namespace is done within its share: every other step.
        let last = order.iter().rposition(|n| n == "quiet").unwrap();
        assert!(last <= 21, "the quiet namespace waited until step {last}");
    }

    #[test]
    fn weights_set_shares_and_idling_earns_no_credit() {
        let weights = [("big".to_owned(), 3.0)].into();
        let q = queue().with_weights(weights);
        let offer =
            |run: &str, i: u32| q.offer(run, CommandIndex(i), "s", Payload::new(*b"1"), 1, 0);
        for i in 0..40 {
            offer("$ns/big/r", i);
            offer("$ns/small/r", i);
        }
        let first: Vec<String> = order(&q).into_iter().take(40).collect();
        let big = first.iter().filter(|n| *n == "big").count();
        assert!((29..=31).contains(&big), "{big}: {first:?}");

        // A namespace idle while another worked does not then monopolise the workers.
        let q = queue();
        for i in 0..100 {
            q.offer("$ns/a/r", CommandIndex(i), "s", Payload::new(*b"1"), 1, 0);
        }
        for _ in 0..50 {
            q.take("w", &names(&["s"]), 1, 0);
        }
        for i in 0..50 {
            q.offer("$ns/b/r", CommandIndex(i), "s", Payload::new(*b"1"), 1, 0);
        }
        let next: Vec<String> = order(&q).into_iter().take(20).collect();
        let b = next.iter().filter(|n| *n == "b").count();
        assert!((9..=11).contains(&b), "{next:?}");
    }

    #[test]
    fn attempts_continue_from_what_the_run_recorded() {
        let q = queue();
        // The run has recorded two failed attempts: this is the third.
        q.offer("r", CommandIndex(0), "charge", Payload::new(*b"1"), 3, 0);
        let taken = q.take("w", &names(&["charge"]), 1, 0);
        assert_eq!(taken[0].attempt, 3);
    }
}
