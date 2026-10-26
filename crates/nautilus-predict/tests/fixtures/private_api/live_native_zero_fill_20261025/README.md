# Native zero-fill command evidence

Recorded against the Catalog and project-fork Native revisions in
[source.json](source.json). The HTTP, Native callback and terminal-check files
are copied from completed probes without modifying their bytes. The source index
is derived metadata, not another venue observation.

| Path | Observed Native and venue behavior |
| --- | --- |
| LIMIT post-only GTD, two orders | Accepted, expected unsupported-modify rejection, then Canceled; two remove requests; zero fills and no fallback removal |
| MARKET FOK and GTC, one each | Accepted, strategy cancel intent, then venue noMarketMatch and Native Rejected; one remove returned noop; REST CANCELLED with zero filled |

Native dispatches typed handlers before the generic order handler. Commands
issued inside Accepted callbacks can advance Cache to PendingUpdate/PendingCancel
before that Accepted event is logged by the generic handler.

The LIMIT collector failed with ProxyError during its final read-only snapshot.
Its diagnostic log remains in the local run directory. Independent follow-up
GETs established both order terminals and zero OPEN orders; they do not replace
the missing wallet capture.
MARKET has separate wallet observations and an independent SDK request check.

These probes establish no-fill command/rejection/cancellation interactions. They
do not establish successful MARKET removal, matching, fees or production latency.
