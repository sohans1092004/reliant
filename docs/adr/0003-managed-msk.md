# ADR-003: Managed MSK, not Kafka self-hosted on EKS

**Status:** Accepted (written with `infra/terraform`; not yet run in a real AWS account)

## Context

The spec leaves this open: run Kafka on the EKS cluster (Strimzi, or Redpanda as locally), or use Amazon MSK. Kafka's job in Reliant is narrow (ADR-004): an ordered, durable intake buffer between ingest and the consumers. Replay comes from Postgres (ADR-001) and retries from a Postgres queue (ADR-002), so Kafka holds minutes to hours of data, not the system of record.

Its durability still matters. An event is accepted only once Kafka acknowledges it (spec §8), and the local Redpanda drill showed how an ack can mean less than it says: write caching made it an in-memory ack until we turned that off. In a cluster, what makes an ack durable is replication: `acks=all` with `min.insync.replicas=2` across three brokers in three AZs.

## Decision

Use **provisioned MSK**: three brokers, one per AZ, TLS between clients and brokers and between brokers. Its configuration (`infra/terraform/data.tf`) pins the durability settings instead of leaving them to defaults:

| Setting | Value | Why |
|---|---|---|
| `default.replication.factor` | 3 | The app creates topics without a replication factor, so this decides it |
| `min.insync.replicas` | 2 | With the idempotent producer's `acks=all`, an ack means 2 brokers have the event |
| `unclean.leader.election.enable` | false | A lagging replica never becomes leader and silently drops acked events |
| `auto.create.topics.enable` | false | Topics come only from `ensureTopics` (ADR-004: an auto-created topic once had 1 partition) |

The app needs only two settings to use it: `KAFKA_SSL=true` and the comma-separated TLS bootstrap brokers from Terraform's outputs.

## Why not self-hosted on EKS

- **The work it saves is the risky part.** Broker replacement after a node or AZ failure, rolling upgrades without losing in-sync replicas, disk sizing and expansion, and partition rebalancing are exactly the operations that cause data loss when done wrong. MSK does them; on EKS they'd be ours, through an operator we'd also have to run and upgrade.
- **Kafka would compete with the app for the cluster.** Brokers want stable nodes, local or EBS storage, and anti-affinity across AZs. On the same nodes as the consumers, a Kafka rebalance and a KEDA scale-out could hit at once.
- **Portability isn't lost.** The app speaks plain Kafka protocol (kafkajs). Locally it runs on Redpanda, and moving to self-hosted later means changing `KAFKA_BROKER`, not code.

## Consequences

- **MSK bills per broker-hour and per GB of storage, even when idle.** At this portfolio's scale that's the main cost. Nothing has been applied to a real account, so no figure is measured here.
- **Partition count is still ours.** Scaling past 12 consumers needs more partitions, added only at zero lag (ADR-004), which is the same on MSK.
- **No IAM auth for Kafka:** access is by security group (only the EKS nodes) plus TLS. MSK's IAM authentication would add per-client identity, but kafkajs needs a SASL/OAUTHBEARER token provider for it.

## Alternatives considered

- **MSK Serverless:** no broker sizing, but it requires IAM authentication (the point above) and has per-partition limits to plan around.
- **Redpanda or Strimzi on EKS:** cheapest in raw compute, most operational work (see above).
- **A hosted Kafka service outside AWS (e.g. Confluent Cloud):** strong tooling, but traffic leaves the VPC and adds another vendor and bill.
