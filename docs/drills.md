# Failure drills, round 2

Five drills the review asked for, each run once on one Windows machine (Docker Desktop, its single-node Kubernetes). Every drill is a command, so it can be re-run:

| Drill | Command | Lost | Duplicates | Order inversions | Verdict |
|---|---|---|---|---|---|
| Redis down + a hanging endpoint on every tenant | `npm run loadtest -- redis-hang` | 0 of 3,104 | 0 | not measured | holds |
| Worker frozen mid-POST past its lease | `LT_SINK_HOST=host.docker.internal npm run loadtest -- lease-double-send` | 0 of 20 | 20 (expected) | n/a | holds; one ledger inaccuracy |
| Network partition: one consumer cut off the broker, process still running | `LT_SINK_HOST=host.docker.internal npm run loadtest -- consumer-partition` | 0 of 3,956 | 1 | 0 | holds |
| KEDA scale-out under live load | `bash loadtest/k8s-drill.sh k8s-scale-out` | 0 of 18,376 | 0 | 0 | holds |
| Rolling consumer restart at 1,000 events/s | `bash loadtest/k8s-drill.sh k8s-rolling-restart` | 0 of 89,881 | 216 | 0 | holds; latency doesn't recover quickly |

"Lost" means accepted with a 202 but never received by the test receiver. "Order inversions" compares, per endpoint, the order of first deliveries with the order ingest accepted the events.

## 1. Redis down while every tenant also has a hanging endpoint

**Setup:** 50 events/s to 10 tenants. Each tenant has a healthy endpoint and one that accepts the connection and never answers. Redis is killed (`docker kill`) for 30s, then started again.

**Why this pairing:** Redis holds the circuit breakers. With Redis gone they can't be read, so every endpoint looks healthy, including the ones that hang. The question was whether Reliant then hammers the hanging endpoints and lets them slow down the healthy ones.

| | Requests to the 10 hanging endpoints per minute | Healthy endpoint, accepted → first attempt |
|---|---|---|
| Before (first 15s) | 72 | p50 30ms, p95 1,146ms, p99 1,908ms |
| Redis down (30s) | 39 | p50 12ms, p95 40ms, p99 51ms |
| After (30s) | 0 | p50 14ms, p95 47ms, p99 59ms |

- **Nothing lost or duplicated, and ingest answered 202 to every request.** The rate limiter fails open (incident 002's fix).
- **No hammering.** Without breakers, the hanging endpoints still got fewer requests per minute than before the outage. The breaker isn't what bounds this load:
  - an endpoint with a queued row gets no inline attempts (ADR-008);
  - its queued row backs off on its own schedule.

  The breaker saves requests on top of that, but nothing depends on it.
- **The healthy endpoint didn't slow down while Redis was down.** The slow "before" window is a one-time start-up cost. The consumer log shows exactly 10 inline timeouts, one per hanging endpoint, all in the first 3 seconds. Each held its partition for up to 1s (ADR-007 describes this). After that, each hanging endpoint had a queued row and got no more inline attempts.
- **Noise:** the consumer logs `breaker update failed` once per endpoint per chunk while Redis is down. That's correct, but loud.

## 2. Lease double-send: a worker frozen mid-POST for longer than its lease

**Setup:** 20 queued deliveries, one per endpoint, to a receiver that answers after 3s. Worker A, a container, claims all 20 and sends them. Then the drill freezes A (`docker pause`) with all 20 in flight and starts worker B. After 75s it unfreezes A. The lease is 60s.

**What happened:**
- **t+65s:** worker B claimed and delivered all 20, once A's leases expired.
- **t+75s:** worker A woke up. All 20 of its writes were refused by the lease fence, and it logged `lease lost` 20 times.
- **Final state:** 20 rows `delivered`, with B's outcome.
- **The receiver got every event twice.** That's the documented limit: the fence keeps the record right, but it can't take back a request that was already sent. Receivers deduplicate on `webhook-id`, which the Python SDK does.

**Finding: the frozen worker's ledger rows say "timed out", but the receiver had answered 200.**
- **Why:** while A was frozen, the receiver answered and its 8s timeout timer expired. When A woke, Node ran the expired timer before reading the response that was waiting in the socket.
- **Effect:** the ledger has 40 attempt rows: B's 20 successes, plus 20 failures from A for requests that actually succeeded. The final state is still correct.
- **Not fixed:** a process that was frozen can't know which happened first. The ledger records what that process saw.

## 3. Network partition: one consumer cut off from the broker, the process keeps running

**How it differs from incident 004:** that drill froze a consumer, which stops the whole process. A partition is worse in one way: the cut-off process keeps running and can still reach Postgres and the receivers, so it could keep delivering.

**Setup:**
- Each of two consumer containers reaches Redpanda over its own Docker network. That uses a second listener, `DRILL`, added to `docker-compose.yml` for this drill.
- The drill detaches the broker from consumer A's network for 60s. Postgres, Redis and the receiver stay reachable.
- Load is 50 events/s to 10 tenants.

**Results:**
- **0 lost and 0 order inversions out of 3,956 events.**
- **1 duplicate.** It arrived 31.7s after the cut, when consumer B took over A's partitions after the 30s session timeout. B reprocessed from the last commit an event A had already delivered.
- **Consumer A delivered nothing while cut off.** It processed 0 chunks after the first 5s. Its membership check before each inline attempt (incident 004's fix) needs the broker, so it stops by itself.
- **Recovery:** A rejoined the group 19.5s after the network came back, and processed 275 chunks afterwards. It didn't crash, and it didn't get stuck holding nothing.

## 4. KEDA scale-out under live load

**Why:** ADR-006 showed scale-out against a backlog with no traffic. This drill scales out while events are arriving, so partitions move between consumers mid-stream.

**Setup:** consumers capped at 50m CPU, so one can't keep up. Load is 200 events/s requested for 120s; about 120/s was achieved (see harness notes).

**Results:**
- **KEDA added a second consumer 30–35s into the load,** and the group rebalanced under traffic.
- **0 lost, 0 duplicates, 0 inversions out of 18,376.**
- The value KEDA feeds the autoscaler matched the broker's own lag reading every 5s, to within a few dozen messages.
- **Latency isn't representative:** p50 3–7s and p95 6–11s, with a consumer deliberately starved to 50m CPU.

**Only one step:** lag stayed under the 500-per-replica target most of the time once there were 2 consumers.

## 5. Rolling consumer restart at 1,000 events/s

**Setup:** 4 consumers, 1,000 events/s requested (850–960/s achieved) spread over 100 endpoints, and `kubectl rollout restart deploy/consumer` 20s in.

**Results (best clean run):**
- **0 lost and 0 order inversions out of 89,881.**
- **216 duplicates (0.24%).** They come from the rebalance window, the same limit incident 004 documents.
- **Latency:**
  - Before the restart, p95 was 153ms.
  - After it, p95 was 10–13s for the rest of the load, while p50 stayed under 130ms.
  - 18,573 events went through the retry queue with no error. They had been queued behind others, or the endpoint ran out of its per-chunk time budget.

**Finding: the retry lane is slow to give back an endpoint that a rebalance pushed into it.** Every endpoint stays in order, so each one takes at most about 1/round-trip events per second. Measured round trips were p50 8–11ms, p95 40–46ms.

**Measuring order correctly:** order is only checked between requests where one got its 202 before the other was sent. Ingest answers 202 after the Kafka ack, so that one is ahead in Kafka. Comparing `created_at` also counts concurrent requests, which have no order, and reported ~9,000 false inversions.

**Tried and reverted: a per-endpoint drain loop in the worker.** After a success, the worker took the same endpoint's next row at once. A test confirmed it drained a 100-row backlog behind a slow endpoint in under 8s instead of 11s. But in an A/B run of this drill on the same image:

| | Drain loop off | Drain loop on |
|---|---|---|
| p50 after the restart | ~150ms | ~24s |
| p95 after the restart | 30–34s | 38–40s |
| Duplicates | 6 | 609 |

The likely cause is 100 drain loops competing for Postgres with the consumers. Run-to-run variance on this machine is also large (p95 after the restart ranged from 10s to 34s across clean runs without the change). The open problem stays open.

## Bugs and lessons from this round

Two problems in the drill harness itself, both fixed in `loadtest/run.js`:
- **Port exhaustion on Windows.** At 1,000/s through `kubectl port-forward`, Node's `fetch` opened a new connection whenever all open ones were busy. That left about 18,700 sockets in TIME_WAIT, until even `kubectl` couldn't connect ("the system lacked sufficient buffer space"). The generator now uses a keep-alive pool of 64 sockets per thread.
- **The harness was distorting what it measured.** It polled `kubectl` synchronously every 5s on the same thread as the test receiver, so deliveries waited on the harness. Also, its 10ms send tick really fires every ~15.6ms on Windows, so it sent 40% less than asked. Polling is now async and the generator is paced by elapsed time.

Found while rehearsing the live deploy rather than by a drill, and also fixed:
- **`schema.sql` seeded `tenant_a` / `tenant_b` everywhere `migrate` ran,** including the AWS overlay. Their API keys are published in the repo, so on a public server anyone could have used them. The seed now lives in `seed-dev.sql` and runs only with `DEV_SEED=true`.
- **Caddy refused to start with `ACME_EMAIL` left blank,** which the example env allowed. CI now validates the Caddyfile.

## Still not drilled

- More than one broker, and losing one of them. Locally there's a single Redpanda; MSK's 3-broker setup is only in Terraform.
- A partition between a *worker* and Postgres while the worker keeps running. The lease fence covers the write side, and drill 2 tested a frozen worker.
- Higher rates. 1,000/s is the most this machine reaches through `kubectl port-forward`.
