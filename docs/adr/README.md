# Architecture Decision Records

One file per decision: what was decided, why, and what it costs. Numbers follow the spec (§16 and v2 §6), so an ADR keeps its number even when the decision differs from what the spec proposed.

| ADR | Decision | Status |
|---|---|---|
| [001](0001-postgres-event-archive.md) | Postgres is the durable event archive, independent of Kafka retention | Accepted |
| [002](0002-retry-queue-in-postgres.md) | Retries live in a Postgres queue, not Kafka retry topics (**reverses the spec**) | Accepted |
| [003](0003-managed-msk.md) | Managed MSK, not Kafka self-hosted on EKS; durability settings pinned | Accepted (in Terraform, not yet run on AWS) |
| [004](0004-kafka-for-primary-queue.md) | Kafka (not RabbitMQ) for the primary delivery queue | Accepted |
| [005](0005-commit-offset-after-ledger-write.md) | Commit the Kafka offset only after the ledger write | Accepted |
| [006](0006-autoscaling.md) | Autoscale consumer on Kafka lag and worker on due retry work (KEDA), not CPU or a fixed pool | Accepted |
| [007](0007-endpoint-isolation.md) | Endpoint isolation: bounded inline attempt + hand-off to a separate retry queue | Accepted |
| [008](0008-ordering-guarantee.md) | Ordering: completion-order per endpoint, with a ping to set aside an event a healthy endpoint keeps rejecting | Accepted |
| [009](0009-status-codes-and-dead-endpoint-backlogs.md) | Honour 429/503 `Retry-After`; 410 disables the endpoint; a dead endpoint's backlog goes to the DLQ with its head | Accepted |

