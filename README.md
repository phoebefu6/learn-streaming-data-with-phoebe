# Learn Streaming Data with Phoebe

Six 45-minute sessions on streaming data: batch vs stream, the Kafka log, delivery
semantics, windows, watermarks and replay. The running artifact is a coffee shop's
order stream (the same world as [Learn Data Pipelines](https://phoebefu6.github.io/learn-data-pipelines-with-phoebe/)) -
this course is its real-time sibling.

**Live:** https://phoebefu6.github.io/learn-streaming-data-with-phoebe/

## The 2pm dashboard

Every session upgrades the same simulator: a deterministic day of order events whose
aggregation is genuinely computed in the browser. Flip the stream on and accuracy lands
at 78.9% (fresh but wrong); idempotency, event-time windows, watermarks and replay earn
it back to 100.0% - including one deliberate dip at the windowing rung, because strict
windows drop stragglers. Cost figures are modelled and badged as such; everything else
is measured.

## Structure

| # | Session | Difficulty |
|---|---------|------------|
| 1 | The stale dashboard | easy |
| 2 | The log | medium |
| 3 | In and out | medium |
| 4 | Windowing | hands-on |
| 5 | Late data | hands-on |
| 6 | Ship it | hardest |

Sources and the verified fact base live in `materials/official-course-map.md`.

by Phoebe Fu · part of [Learn with Phoebe](https://phoebefu6.github.io/learn-with-phoebe/)
