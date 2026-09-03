# learn-streaming-data-with-phoebe - official source map

Built 2026-09-03. Single-track 6 sessions, running artifact: the coffee-shop order stream
(the same order #8412 world as learn-data-pipelines - this course is that course's
real-time sibling). Concepts + Kafka core, vendor-light. Difficulty d3 (Advanced), hub
bucket `deng`.

Fast-moving product note: Kafka doc quotes were verified against apache/kafka trunk docs
(4.x wording) + the rendered 3.9 config page on 2026-09-03. Re-verify version-specific
claims (retention default, idempotent-producer version) before any live delivery.

## Simulator canon (streaming-live.js, seed 20260903 - VERIFIED LIVE in browser 2026-09-03)

The 2pm dashboard. Truth so far at 14:00 = $3,723. Full-day truth = $6,115.

| Rung | Freshness lag | Today-view accuracy | Dashboard shows | Counters |
|------|---------------|---------------------|-----------------|----------|
| Batch (nightly) | 14h | showing yesterday | $0 | - |
| + Stream it | seconds | 78.9% | $4,073 | 15 retries double-counted |
| + Idempotent consumer | seconds | 89.1% | $3,485 | over-count gone; late orders missing |
| + Event-time windows (strict close) | seconds | **88.0% - a real dip** | $3,275 | 6 late events dropped |
| + Watermark: allow 3h lateness | seconds | 93.6% | $3,485 | stragglers caught; unarrived still missing |
| + End-of-day replay | seconds | **100.0%** | $6,115 | full-log reprocess converges |
| Anti-lever: stream all 13 tables | seconds | 100.0% (unchanged) | $6,115 | cost $340 -> $1,980/mo (modelled), pipelines 2 -> 13 |

Teaching beats the numbers carry:
- The strict-window DIP (89.1 -> 88.0) is deliberate and honest: a window that closes at
  +0 drops stragglers that arrival-time bucketing was at least counting somewhere. The dip
  is WHY watermarks exist - session 4 ends on it, session 5 resolves it.
- Watermark rung stays at 93.6%: events that have not ARRIVED by 2pm cannot be counted by
  any semantics - completeness is a moving target. Replay closes the day at 100.0%.
- Anti-lever never moves accuracy or lag; only the modelled cost meter. Cost figures carry
  a "modelled" badge; everything else is computed live from the event log ("measured").

## Per-session coverage

### Session 1 - The stale dashboard (batch vs streaming)
| Source | Coverage | What |
|--------|----------|------|
| Kafka intro: event definition | ✓ | "An event records the fact that 'something happened'" |
| Tigani, Big Data is Dead (MotherDuck 2023) | ✓ | when NOT to stream: median heavy-user storage <100GB; 90% of BigQuery queries <100MB; data >1 week old ~20x less likely to be queried |
| Morling, Streaming vs Batch is a Wrong Dichotomy (2025) | ✓ | "If you've experienced a data freshness of a second or two... you don't want to ever miss this magic again" + complexity price |
| Flink use cases page | ◐ | steelman: fraud detection, anomaly detection, rule-based alerting, business process monitoring |

### Session 2 - The log (Kafka core)
| Source | Coverage | What |
|--------|----------|------|
| Kafka intro (apache/kafka trunk docs) | ✓ | topics ("similar to a folder"), partitions ("buckets"), per-partition ordering guarantee (verbatim), producers/consumers "fully decoupled", per-topic retention |
| Kafka 3.9 config reference | ✓ | log.retention.hours default 168 (7 days); log.retention.bytes default -1 |
| Kafka design: replication | ✓ | leader/follower per partition, ISR, "a committed message will not be lost, as long as there is at least one in sync replica alive", acks 0/1/all + min.insync.replicas |
| Kafka design: consumer position | ✓ | offset = "just a single integer"; one consumer per partition per group |
| Kafka ops: consumer lag | ✓ | LAG = LOG-END-OFFSET minus CURRENT-OFFSET (kafka-consumer-groups.sh); records-lag-max metric |
| Confluent Apache Kafka 101 (Berglund/Philippart, 16 modules) | ◐ | shared spine: Topics/Partitions/Brokers/Replication/Producers/Consumers |

### Session 3 - In and out (delivery semantics)
| Source | Coverage | What |
|--------|----------|------|
| Kafka design: Message Delivery Semantics | ✓ | at-least-once is the DEFAULT; pre-0.11 resend -> duplicates; idempotent producer (broker dedupes by producer id + sequence number); transactions since 0.11; read_committed; consumer-side at-most/at-least-once by commit ordering; "simplest way to get exactly-once... Kafka Streams" |
| DDIA ch11 (via Kleppmann notes, keyvanakbary mirror) | ✓ | "Idempotent operations can be an effective way of achieving exactly-once semantics with only a small overhead" |
| Confluent Schema Registry 101 / Kafka 101 SR modules | ◐ | schema discipline exists as a named practice; contents not fetched - do not invent module detail |

### Session 4 - Windowing
| Source | Coverage | What |
|--------|----------|------|
| Flink docs: Windows | ✓ | tumbling ("fixed size and do not overlap"), sliding/hopping (+ slide param), session ("closes when it does not receive elements for a certain period"), global (needs custom trigger) |
| Flink docs: Timely Stream Processing | ✓ | processing time = machine clock, no determinism; event time = "time that each individual event occurred on its producing device" |
| Kafka Streams 101 (Blee-Goldman) | ◐ | Windowing + Time Concepts modules exist in the official course |

### Session 5 - Late data (watermarks, replay, the log as truth)
| Source | Coverage | What |
|--------|----------|------|
| Flink docs: watermarks | ✓ | verbatim: "A Watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements... with a timestamp t' <= t"; default = "late elements are dropped when the watermark is past the end of the window"; allowed lateness; sideOutputLateData; late firings "should be treated as updated results of a previous computation" |
| Marz, How to beat the CAP theorem (2011) | ✓ | "Query = Function(All Data)"; data immutability; recompute from the master dataset as human fault-tolerance |
| Kleppmann blog (2015) | ✓ | "The database you read from is just a cached view of the event log"; replay events to reconstruct exactly what happened |
| DDIA ch11 | ◐ | CDC, event sourcing, stream joins taxonomy (stream-stream / stream-table / table-table) - section list search-derived + cross-verified via public notes; O'Reilly ch page 403'd |

### Session 6 - Ship it (operating, cost, lambda vs kappa)
| Source | Coverage | What |
|--------|----------|------|
| Kafka ops/monitoring | ✓ | consumer lag monitoring, verbatim: "For a consumer to keep up, max lag needs to be less than a threshold and min fetch rate needs to be larger than 0" |
| AWS Kinesis pricing page | ✓ | provisioned shard billed per hour regardless of traffic - $0.015/shard-hour us-east-1 - the meter runs 24/7 (anti-lever anchor) |
| Tigani 2023 | ✓ | most workloads are small + recent-biased; a single modern machine handles most |
| Marz 2011 (Lambda) | ✓ | batch layer + realtime layer, merge at query; logic implemented twice |
| Kreps, Questioning the Lambda Architecture (O'Reilly Radar 2014) | ✓ | "maintaining code that needs to produce the same result in two complex distributed systems is exactly as painful as it seems like it would be"; Kappa = retain the full log, reprocess with a second job, switch; "you only do reprocessing when your processing code changes" (retrieved via reader proxy of the canonical URL) |
| Morling 2025 | ✓ | streaming systems batch internally; dichotomy is false |
| Confluent Kafka Internal Architecture (Jun Rao, 15 modules) | ◐ | deep-ops overflow named honestly as not covered: control plane, tiered storage, geo-replication |

## Not covered by design
- Kafka broker operations/tuning, KRaft/control plane, tiered storage, geo-replication
  (Confluent Internal Architecture course territory - named on session 6 as the next step)
- Flink/Kafka Streams APIs and code - this course teaches semantics, not a framework
- ksqlDB, Kafka Connect connector catalog
- Vendor comparisons (Kinesis appears only as a pricing evidence point)

## Verification gaps (recorded honestly - do not paper over)
- O'Reilly DDIA ch11 page returned 403 everywhere; section list is search-index derived,
  cross-verified against a fetched public notes mirror. Cite the book, not the mirror,
  and keep claims to what the notes + Kleppmann's own blog support.
- Kreps article body retrieved via reader proxy of the canonical O'Reilly URL.
- "A streaming pipeline has a failure window every second" - NOT verified, do not use.
- Confluent module DURATIONS verified only for Kafka 101; SR 101 contents not fetched.
- Kafka doc quotes are trunk (4.x) wording; retention default cross-checked on rendered 3.9.
