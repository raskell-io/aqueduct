//! Overload protection (ADR 0045): what a server accepts when more arrives than it should take on.
//!
//! - **Rate limits:** a token bucket per caller; over it, `429` / `RESOURCE_EXHAUSTED` with a retry hint.
//! - **Concurrent requests:** over the cap, `503` / `UNAVAILABLE` at once, rather than queueing without
//!   bound. Waiting calls hold a blocking thread, so the default stays below Tokio's blocking pool.
//! - **Connections:** over the cap, new connections wait in the operating system's queue. Clients have a
//!   deadline for a request's headers, so silent ones cannot use up the places.
//! - **Refused requests** are always refused, cheaply, but only so many a second write an audit line, so
//!   a flood cannot grow the log or slow the server with disk syncs. The rest are counted, and a later
//!   line says how many were not recorded.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Deserialize;

/// What a server accepts. All are per server, and optional in its configuration (`"limits"`).
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct Limits {
    /// Requests a second per caller (a token's name), sustained. Workers are exempt: their requests
    /// are limited by their capacity. Default: no limit.
    pub requests_per_second: Option<f64>,
    /// How many requests a caller may make at once above that rate. Default: one second's worth.
    pub burst: Option<f64>,
    /// Requests in progress at once, across callers. Default 256.
    pub max_concurrent_requests: usize,
    /// Connections open at once. Default 1,024.
    pub max_connections: usize,
    /// How long a client has for a request's headers, in milliseconds, so that silent connections cannot
    /// use up the places. Default 10,000.
    pub header_timeout_ms: u64,
    /// Refused requests a second that write an audit line. Default 10.
    pub audited_refusals_per_second: f64,
    /// Running runs. Above it, starts of new keys and new object calls are refused with a retry hint;
    /// retried starts, and what runs already do, are not. Default: no limit.
    pub max_running_runs: Option<usize>,
    /// Quotas and shares per namespace (ADR 0046).
    pub namespaces: BTreeMap<String, NamespaceLimits>,
}

/// A namespace's quotas, and its share of workers that serve several namespaces.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct NamespaceLimits {
    /// Requests a second across the namespace's callers. Workers are exempt. Default: no limit.
    pub requests_per_second: Option<f64>,
    /// How many requests it may make at once above that rate. Default: one second's worth.
    pub burst: Option<f64>,
    /// Its running runs, as [`Limits::max_running_runs`]. Default: no limit.
    pub max_running_runs: Option<usize>,
    /// Its share of workers that serve several namespaces, relative to others. Default 1.
    pub weight: f64,
}

impl Default for NamespaceLimits {
    fn default() -> NamespaceLimits {
        NamespaceLimits {
            requests_per_second: None,
            burst: None,
            max_running_runs: None,
            weight: 1.0,
        }
    }
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            requests_per_second: None,
            burst: None,
            max_concurrent_requests: 256,
            max_connections: 1_024,
            header_timeout_ms: 10_000,
            audited_refusals_per_second: 10.0,
            max_running_runs: None,
            namespaces: BTreeMap::new(),
        }
    }
}

impl Limits {
    /// No limits at all (for tests and programs that protect themselves).
    pub fn none() -> Limits {
        Limits {
            max_concurrent_requests: tokio::sync::Semaphore::MAX_PERMITS,
            max_connections: tokio::sync::Semaphore::MAX_PERMITS,
            audited_refusals_per_second: f64::MAX,
            ..Limits::default()
        }
    }

    /// Checks that the limits make sense.
    pub fn check(&self) -> Result<(), String> {
        let positive = |name: &str, value: Option<f64>| match value {
            Some(value) if value.is_nan() || value <= 0.0 => {
                Err(format!("limits: {name} must be above 0"))
            }
            _ => Ok(()),
        };
        positive("requests_per_second", self.requests_per_second)?;
        positive("burst", self.burst)?;
        positive(
            "audited_refusals_per_second",
            Some(self.audited_refusals_per_second),
        )?;
        if self.max_concurrent_requests == 0 || self.max_connections == 0 {
            return Err(
                "limits: max_concurrent_requests and max_connections must be at least 1".into(),
            );
        }
        if self.header_timeout_ms == 0 {
            return Err("limits: header_timeout_ms must be at least 1".into());
        }
        if self.max_running_runs == Some(0) {
            return Err("limits: max_running_runs must be at least 1".into());
        }
        for (name, namespace) in &self.namespaces {
            if !aqueduct_engine::namespaces::valid_namespace(name) {
                return Err(format!("limits: `{name}` is not a namespace"));
            }
            positive(
                "a namespace's requests_per_second",
                namespace.requests_per_second,
            )?;
            positive("a namespace's burst", namespace.burst)?;
            positive("a namespace's weight", Some(namespace.weight))?;
            if namespace.max_running_runs == Some(0) {
                return Err(format!(
                    "limits: `{name}`'s max_running_runs must be at least 1"
                ));
            }
        }
        Ok(())
    }
}

/// A token bucket, with the clock as a parameter.
#[derive(Clone, Debug)]
pub(crate) struct Bucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Duration,
}

impl Bucket {
    pub(crate) fn new(rate: f64, capacity: f64, now: Duration) -> Bucket {
        Bucket {
            rate,
            capacity,
            tokens: capacity,
            last: now,
        }
    }

    /// Takes a token; otherwise, how long until there is one.
    pub(crate) fn take(&mut self, now: Duration) -> Result<(), Duration> {
        let elapsed = now.saturating_sub(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }
        Err(Duration::from_secs_f64((1.0 - self.tokens) / self.rate))
    }
}

/// The server's state for [`Limits`].
#[derive(Debug)]
pub(crate) struct Limiter {
    limits: Limits,
    started: Instant,
    callers: Mutex<BTreeMap<String, Bucket>>,
    refusals: Mutex<Bucket>,
    unrecorded: AtomicU64,
    pub(crate) requests: std::sync::Arc<tokio::sync::Semaphore>,
}

impl Limiter {
    pub(crate) fn new(limits: Limits) -> Limiter {
        let rate = limits.audited_refusals_per_second;
        Limiter {
            started: Instant::now(),
            callers: Mutex::new(BTreeMap::new()),
            refusals: Mutex::new(Bucket::new(rate, rate.max(1.0), Duration::ZERO)),
            unrecorded: AtomicU64::new(0),
            requests: std::sync::Arc::new(tokio::sync::Semaphore::new(
                limits.max_concurrent_requests,
            )),
            limits,
        }
    }

    /// Whether `caller` may make a request now; otherwise, how long until it may.
    pub(crate) fn admit(&self, caller: &str) -> Result<(), Duration> {
        let Some(rate) = self.limits.requests_per_second else {
            return Ok(());
        };
        let now = self.started.elapsed();
        let capacity = self.limits.burst.unwrap_or(rate).max(1.0);
        let mut callers = self.callers.lock().unwrap_or_else(|e| e.into_inner());
        callers
            .entry(caller.to_owned())
            .or_insert_with(|| Bucket::new(rate, capacity, now))
            .take(now)
    }

    /// Whether a namespace may make a request now; otherwise, how long until it may.
    pub(crate) fn admit_namespace(&self, namespace: &str) -> Result<(), Duration> {
        let Some(limits) = self.limits.namespaces.get(namespace) else {
            return Ok(());
        };
        let Some(rate) = limits.requests_per_second else {
            return Ok(());
        };
        let now = self.started.elapsed();
        let capacity = limits.burst.unwrap_or(rate).max(1.0);
        let mut callers = self.callers.lock().unwrap_or_else(|e| e.into_inner());
        callers
            // Tokens' names cannot hold this mark as namespaces do not: it cannot clash with a caller.
            .entry(format!("$ns/{namespace}"))
            .or_insert_with(|| Bucket::new(rate, capacity, now))
            .take(now)
    }

    /// Whether a refusal should write an audit line: `Some(n)`, with how many before it were not
    /// recorded; `None` if it should only be counted.
    pub(crate) fn record_refusal(&self) -> Option<u64> {
        let now = self.started.elapsed();
        let mut refusals = self.refusals.lock().unwrap_or_else(|e| e.into_inner());
        if refusals.take(now).is_err() {
            self.unrecorded.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(self.unrecorded.swap(0, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_refills_at_its_rate_up_to_its_capacity() {
        let at = Duration::from_secs_f64;
        let mut bucket = Bucket::new(2.0, 3.0, at(0.0));
        for _ in 0..3 {
            assert!(bucket.take(at(0.0)).is_ok());
        }
        // Empty: one token comes in half a second.
        assert_eq!(bucket.take(at(0.0)), Err(at(0.5)));
        assert!(bucket.take(at(0.5)).is_ok());
        // A long pause fills it only to its capacity.
        assert!(bucket.take(at(100.0)).is_ok());
        assert!(bucket.take(at(100.0)).is_ok());
        assert!(bucket.take(at(100.0)).is_ok());
        assert!(bucket.take(at(100.0)).is_err());
    }

    #[test]
    fn refusals_beyond_the_rate_are_counted_and_then_reported() {
        let limiter = Limiter::new(Limits {
            audited_refusals_per_second: 1_000.0,
            ..Limits::default()
        });
        let recorded = (0..5_000)
            .filter_map(|_| limiter.record_refusal())
            .collect::<Vec<_>>();
        assert!(recorded.len() < 4_000, "{}", recorded.len());
        std::thread::sleep(Duration::from_millis(20));
        let total: u64 = recorded.iter().sum::<u64>()
            + limiter.record_refusal().unwrap()
            + recorded.len() as u64
            + 1;
        assert_eq!(total, 5_001);
    }

    #[test]
    fn nonsense_is_refused() {
        let bad = |limits: Limits| limits.check().is_err();
        assert!(Limits::default().check().is_ok());
        assert!(Limits::none().check().is_ok());
        assert!(bad(Limits {
            requests_per_second: Some(0.0),
            ..Limits::default()
        }));
        assert!(bad(Limits {
            requests_per_second: Some(f64::NAN),
            ..Limits::default()
        }));
        assert!(bad(Limits {
            max_connections: 0,
            ..Limits::default()
        }));
        assert!(bad(Limits {
            max_running_runs: Some(0),
            ..Limits::default()
        }));
    }
}
