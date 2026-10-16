I froze one of my Kafka consumers for 45 seconds with `docker pause`. When it woke up, it re-sent about 137 webhooks that another consumer had already delivered, because it checked group membership after each HTTP call instead of before.
Moving the check in front of every attempt and every write cut it to 0–1 duplicates per run. The 1 left is a request that was in flight at the freeze, which at-least-once delivery and `webhook-id` dedup are meant to cover.
Full write-up of the drill, the bug and the fix (one laptop, real numbers, no extrapolation): [link]
