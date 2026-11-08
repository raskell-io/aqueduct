//! `aqueduct drill` (ADR 0065): load and checks against a running cluster, for the failover drill and
//! the cluster measurements on real machines. Starts runs at a steady rate on the given servers in turn,
//! waits for each, and reports throughput, latencies, the longest time no start was acknowledged (the
//! failover's cost, when a machine is stopped meanwhile), and whether every acknowledged run is still
//! there at the end: a lost one is a failure.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use tokio::sync::Mutex;

use crate::migrate::runtime;

/// How long one call may take before it counts as failed.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Tally {
    /// Each start's send time (since the drill began) and whether it was acknowledged.
    starts: Vec<(Duration, bool)>,
    start_latencies: Vec<Duration>,
    completion_latencies: Vec<Duration>,
    acknowledged: Vec<String>,
    failed_runs: u64,
}

struct Options {
    servers: Vec<String>,
    workflow: String,
    input: Vec<u8>,
    seconds: u64,
    rate: f64,
    header: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    ca: Option<String>,
    prefix: String,
}

fn options(args: &[String]) -> Result<Options, String> {
    let mut servers = Vec::new();
    let (mut workflow, mut input, mut seconds, mut rate) = (None, b"1".to_vec(), 60u64, 20f64);
    let (mut token_file, mut ca, mut prefix) = (None, None, None);
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let mut value = || {
            rest.next()
                .cloned()
                .ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "--server" => servers.push(value()?),
            "--workflow" => workflow = Some(value()?),
            "--input" => input = value()?.into_bytes(),
            "--seconds" => seconds = value()?.parse().map_err(|e| format!("--seconds: {e}"))?,
            "--rate" => rate = value()?.parse().map_err(|e| format!("--rate: {e}"))?,
            "--token-file" => token_file = Some(value()?),
            "--ca" => ca = Some(value()?),
            "--prefix" => prefix = Some(value()?),
            other => return Err(format!("unknown option `{other}`")),
        }
    }
    if servers.is_empty() {
        return Err("drill needs --server <url>, once for each server".into());
    }
    if !(rate > 0.0 && rate <= 10_000.0) {
        return Err("--rate must be between 0 and 10,000 runs a second".into());
    }
    let token = match &token_file {
        Some(file) => Some(
            std::fs::read_to_string(file)
                .map_err(|e| format!("{file}: {e}"))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("AQUEDUCT_TOKEN").ok(),
    };
    let header = token
        .map(|token| format!("Bearer {token}").parse())
        .transpose()
        .map_err(|_| "the token is not a valid header value".to_owned())?;
    let ca = ca
        .map(|file| std::fs::read_to_string(&file).map_err(|e| format!("{file}: {e}")))
        .transpose()?;
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    Ok(Options {
        servers,
        workflow: workflow.ok_or("drill needs --workflow <name>")?,
        input,
        seconds,
        rate,
        header,
        ca,
        prefix: prefix.unwrap_or_else(|| format!("drill-{unix}")),
    })
}

/// A channel that connects when first used and again after a server comes back.
fn lazy(url: &str, ca: Option<&str>) -> Result<tonic::transport::Channel, String> {
    let mut endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|e| format!("{url}: {e}"))?
        .connect_timeout(Duration::from_secs(2));
    if url.starts_with("https://") {
        let mut tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
        if let Some(ca) = ca {
            tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca));
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| format!("{url}: {e}"))?;
    }
    Ok(endpoint.connect_lazy())
}

fn request<T>(
    header: Option<&tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    message: T,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(header) = header {
        request
            .metadata_mut()
            .insert("authorization", header.clone());
    }
    request
}

/// The value at quantile `q` of sorted durations, in milliseconds.
fn quantile(sorted: &[Duration], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index.min(sorted.len() - 1)].as_secs_f64() * 1_000.0
}

/// The longest stretch, among starts in send order, from the first refused start to the next
/// acknowledged one.
fn longest_gap(starts: &[(Duration, bool)]) -> Duration {
    let mut sorted = starts.to_vec();
    sorted.sort();
    let (mut longest, mut since) = (Duration::ZERO, None);
    for (at, acknowledged) in sorted {
        match (acknowledged, since) {
            (false, None) => since = Some(at),
            (true, Some(began)) => {
                longest = longest.max(at.saturating_sub(began));
                since = None;
            }
            _ => {}
        }
    }
    longest
}

/// Runs `aqueduct drill`.
pub(crate) fn drill(args: &[String]) -> Result<String, String> {
    let options = Arc::new(options(args)?);
    runtime()?.block_on(run(options))
}

async fn run(options: Arc<Options>) -> Result<String, String> {
    let channels: Vec<tonic::transport::Channel> = options
        .servers
        .iter()
        .map(|url| lazy(url, options.ca.as_deref()))
        .collect::<Result<_, _>>()?;
    let channels = Arc::new(channels);
    let tally = Arc::new(Mutex::new(Tally::default()));
    let began = Instant::now();
    let interval = Duration::from_secs_f64(1.0 / options.rate);
    let total = (options.rate * options.seconds as f64).round() as u64;
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..total {
        let due = began + interval.mul_f64(i as f64);
        tokio::time::sleep_until(due.into()).await;
        let (options, channels, tally) = (
            Arc::clone(&options),
            Arc::clone(&channels),
            Arc::clone(&tally),
        );
        tasks.spawn(async move { one(i, &options, &channels, &tally, began).await });
    }
    while tasks.join_next().await.is_some() {}
    let elapsed = began.elapsed();
    // Every acknowledged run must still be there.
    let tally = Arc::try_unwrap(tally)
        .map_err(|_| "a run's task outlived the drill".to_owned())?
        .into_inner();
    let mut lost = Vec::new();
    for key in &tally.acknowledged {
        if !exists(key, &options, &channels).await {
            lost.push(key.clone());
        }
    }
    let mut starts = tally.start_latencies.clone();
    starts.sort();
    let mut completions = tally.completion_latencies.clone();
    completions.sort();
    let refused = tally.starts.iter().filter(|(_, ok)| !ok).count();
    let report = format!(
        "{} starts sent over {:.1} s at {} a second to {} servers\n\
         acknowledged {}, refused or failed {}, runs that failed {}\n\
         start latency ms: p50 {:.1}, p99 {:.1}, max {:.1}\n\
         start-to-end latency ms: p50 {:.1}, p99 {:.1}, max {:.1}\n\
         throughput: {:.1} runs ended a second\n\
         longest time without an acknowledged start: {:.2} s\n\
         acknowledged runs missing at the end: {}\n",
        tally.starts.len(),
        elapsed.as_secs_f64(),
        options.rate,
        options.servers.len(),
        tally.acknowledged.len(),
        refused,
        tally.failed_runs,
        quantile(&starts, 0.5),
        quantile(&starts, 0.99),
        quantile(&starts, 1.0),
        quantile(&completions, 0.5),
        quantile(&completions, 0.99),
        quantile(&completions, 1.0),
        completions.len() as f64 / elapsed.as_secs_f64(),
        longest_gap(&tally.starts).as_secs_f64(),
        lost.len(),
    );
    if lost.is_empty() {
        Ok(report)
    } else {
        Err(format!(
            "{report}LOST: acknowledged runs the cluster no longer has: {}",
            lost.join(", ")
        ))
    }
}

/// Starts run `i` on a server, then waits for it on any.
async fn one(
    i: u64,
    options: &Options,
    channels: &[tonic::transport::Channel],
    tally: &Mutex<Tally>,
    began: Instant,
) {
    let key = format!("{}-{i}", options.prefix);
    let server = (i as usize) % channels.len();
    let sent = Instant::now();
    // As a client does: a server that does not answer is passed over for the next. A start is
    // idempotent (its key), so asking again is safe; it counts as refused only if no server took it.
    let mut acknowledged = false;
    for offset in 0..channels.len() {
        let mut client = IngressClient::new(channels[(server + offset) % channels.len()].clone());
        let started = tokio::time::timeout(
            CALL_TIMEOUT,
            client.start(request(
                options.header.as_ref(),
                v1::StartRequest {
                    key: key.clone(),
                    workflow: options.workflow.clone(),
                    input: options.input.clone(),
                    ..Default::default()
                },
            )),
        )
        .await;
        if matches!(started, Ok(Ok(_))) {
            acknowledged = true;
            break;
        }
    }
    {
        let mut tally = tally.lock().await;
        tally.starts.push((sent - began, acknowledged));
        if acknowledged {
            tally.start_latencies.push(sent.elapsed());
            tally.acknowledged.push(key.clone());
        }
    }
    if !acknowledged {
        return;
    }
    // Waits on each server in turn until one answers with the run's end.
    for attempt in 0..(channels.len() * 6) {
        let mut client = IngressClient::new(channels[(server + attempt) % channels.len()].clone());
        let waited = tokio::time::timeout(
            CALL_TIMEOUT,
            client.wait(request(
                options.header.as_ref(),
                v1::WaitRequest {
                    key: key.clone(),
                    timeout_ms: 5_000,
                },
            )),
        )
        .await;
        if let Ok(Ok(result)) = waited {
            let result = result.into_inner();
            let mut tally = tally.lock().await;
            tally.completion_latencies.push(sent.elapsed());
            if result.status != v1::Status::Completed as i32 {
                tally.failed_runs += 1;
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Whether some server knows the run.
async fn exists(key: &str, options: &Options, channels: &[tonic::transport::Channel]) -> bool {
    for round in 0..5 {
        for channel in channels {
            let mut client = IngressClient::new(channel.clone());
            let described = tokio::time::timeout(
                CALL_TIMEOUT,
                client.describe(request(
                    options.header.as_ref(),
                    v1::DescribeRequest {
                        key: key.to_owned(),
                    },
                )),
            )
            .await;
            match described {
                Ok(Ok(_)) => return true,
                // Not found where the partition's leader answered: lost, unless another server says
                // otherwise.
                Ok(Err(status)) if status.code() == tonic::Code::NotFound => {}
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(200 * (round + 1))).await;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gap_runs_from_the_first_refusal_to_the_next_acknowledgement() {
        let ms = Duration::from_millis;
        let starts = vec![
            (ms(0), true),
            (ms(100), false),
            (ms(150), false),
            (ms(400), true),
            (ms(500), false),
            (ms(550), true),
        ];
        assert_eq!(longest_gap(&starts), ms(300));
        assert_eq!(longest_gap(&[(ms(0), true)]), Duration::ZERO);
        let sorted = vec![ms(1), ms(2), ms(3), ms(4)];
        assert_eq!(quantile(&sorted, 0.0), 1.0);
        assert_eq!(quantile(&sorted, 1.0), 4.0);
    }
}
