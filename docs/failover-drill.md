# The failover drill and cluster measurements

Phase 4 is done when a failover drill on real machines is documented and the cluster's targets are
measured (BRIEF.md sections 9 and 14). This is the procedure; `aqueduct drill` (ADR 0065) is the tool.
Record each run in `docs/measurements/` with the machines, versions and the drill's report.

## What you need

- Three machines (five for the larger drill), on one network, each with `aqueduct` built from the same
  commit. A fourth machine runs the drill, so that its load does not skew the servers.
- A cluster CA and a certificate per server (ADR 0064), a shared secret, and the same tokens file on every
  server (forwarded requests are authenticated by the leader, ADR 0056).
- On each server, a configuration like:

```json
{
  "partitions": 4,
  "cluster": {
    "id": 1,
    "peers": {"1": "https://10.0.0.1:7470", "2": "https://10.0.0.2:7470", "3": "https://10.0.0.3:7470"},
    "secret": "<at least 16 characters, the same everywhere>",
    "tls": {"ca": "ca.pem", "cert": "server.pem", "key": "server-key.pem"}
  }
}
```

  started as `aqueduct server ./data --listen 0.0.0.0:7470 --config cluster.json --tls-cert server.pem
  --tls-key server-key.pem`, with an approved procedure version of a workflow to drive (the drill uses one
  that returns at once, so it measures the engine, not a worker).

## The drill

1. Start all servers; wait until `GET /v1/admin/partitions` on each shows every partition with a leader.
2. On the fourth machine:
   `aqueduct drill --server https://10.0.0.1:7470 --server https://10.0.0.2:7470 --server https://10.0.0.3:7470 --workflow double --seconds 300 --rate 200 --ca ca.pem --token-file token`
3. About a minute in, take a leader's machine away, each way in a separate drill:
   - **stop it** (`systemctl stop`: it hands its partitions over first, ADR 0060);
   - **kill it** (`kill -9`);
   - **cut it off** (unplug its network, or drop its traffic with the firewall);
   - **power it off** (pull the plug, or stop the VM without a shutdown).
4. Two minutes later, bring it back, and let the drill finish.

**Pass:** the report says `acknowledged runs missing at the end: 0` every time. Record the longest time
without an acknowledged start for each way: the target is under 3 s at p99 over repeated drills (BRIEF
section 9: "leader failover to first new dispatch"). The machine that came back must catch up:
`GET /v1/admin/partitions` on it shows the same groups and leaders as the others.

## The measurements

- **Throughput:** with five servers, raise `--rate` until starts begin to be refused or latencies climb;
  the rate just below is the cluster's throughput for that workflow. Repeat with three and with nine
  servers (BRIEF target: at least 2.5x from three to nine), with `"partitions"` at least the number of
  servers, so that leaders spread (ADR 0058).
- **Latency:** the report's p50 and p99 of start and start-to-end latency at half the throughput.
- **A lost disk:** stop a follower, delete its data directory, start it again: with `"snapshots"`
  configured (ADR 0062) it loads the latest snapshot and catches up; the drill keeps passing meanwhile.
