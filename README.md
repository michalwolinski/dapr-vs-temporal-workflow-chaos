# Dapr Workflow vs Temporal: what breaks, and what it costs

Companion repository for the article by [Michał Woliński](https://wolinski.com):
**https://wolinski.com/blog/dapr-vs-temporal-workflow-chaos.html**

One order saga (reserve, charge, ship, refund if shipping fails) runs on Dapr Workflow and on Temporal, in two versions:
`OrderSaga` as most examples write it, and `SafeOrderSaga` with an idempotency key and, when the charge's outcome is
unknown, a refund only once the provider reports the key's final status, with compensation retried for up to an hour
rather than as briefly as the charge. Copy the second one, and read the article's
notes on what a workflow ID as a key does not cover. The tests break
links with Toxiproxy and containers with Docker while the charge is in flight, after the money moved and before the
engine heard about it, and count the charges.

## Run it

Needs Docker. Everything else runs in containers pinned by digest.

```bash
./run.sh                              # 24 tests, about fourteen minutes, writes test-results.log
./run.sh -Dtest=ChaosTest             # only the failure tests
./run.sh -Dtest=StepCostTest -Ddelay=2
./run.sh -Dtest=ExploreTest -Drounds=3 -Dexcluded=none   # the exploration: every scenario, printed, not asserted
```

`test-results.log` is the run the article quotes. `runs/` holds the exploration and the repeats behind the claims.

## Versions

| Component | Version |
|---|---|
| Dapr runtime (`daprd`, Placement, Scheduler) | 1.19.1 |
| Dapr Java SDK (`dapr-sdk-workflows`) | 1.19.1 |
| Redis (Dapr state store) | 8.10.2 |
| Temporal Server | 1.32.2, on PostgreSQL 18.7 |
| Temporal Java SDK | 1.41.1 |
| Toxiproxy | 2.13.0 |
| JDK | Temurin 25 (Maven image pinned by digest) |

## Claims and the tests behind them

| Claim | Test |
|---|---|
| A rolling deploy during the charge: Dapr 2 charges, Temporal 1 | `ChaosTest.aRollingDeployDuringTheChargeChargesTwiceOnDaprAndOnceOnTemporal` |
| A 300 ms break in the app's stream charges twice on Dapr, once on Temporal | `ChaosTest.daprRunsTheChargeAgainAfterAThreeHundredMillisecondBlip`, `temporalDeliversTheResultOfAChargeThatFinishedDuringABlip` |
| A 2 s cut or blackhole of the app link: Dapr 2 charges, Temporal 1 | `aBreakShorterThanTheTimeoutCostsTemporalNothingAndDaprACharge`, `aBlackholedLinkShorterThanTheTimeoutCostsTemporalNothingAndDaprACharge` |
| A break longer than Temporal's 5 s timeout charges twice on both | `aBreakLongerThanTheTimeoutChargesTwiceOnBoth` |
| Restarting the Dapr sidecar charges twice | `restartingTheDaprSidecarRunsTheChargeAgain` |
| A crashed worker charges twice on both; Temporal waits for the timeout | `aCrashedWorkerChargesTwiceOnBothAndTemporalWaitsForTheTimeout` |
| Losing the engine's database for 2 s does not repeat the charge (except Temporal with a blackholed PostgreSQL: 3 of 14 runs charged twice, not asserted) | `aLostStoreLinkDoesNotRunTheChargeAgainOnEither` |
| A hung charge: Dapr waits, Temporal runs it three times and fails the saga with nothing refunded | `aHungChargeWaitsForeverOnDaprAndRunsThreeTimesAtOnceOnTemporal` |
| Dapr refuses a start with no worker connected; Temporal queues it | `daprRefusesToStartAWorkflowWithNoWorkerConnectedAndTemporalQueuesIt` |
| An idempotency key turns every repeated charge into one | `RemedyTest.anIdempotencyKeyTurnsEveryRepeatedChargeIntoOne` |
| A Dapr worker that waits for its running activities before `close()` charges once on a deploy | `ChaosTest.aDaprWorkerThatWaitsForItsRunningActivitiesBeforeClosingChargesOnceOnADeploy` |
| It also turns the rolling-deploy duplicate on Dapr into one charge | `RemedyTest.anIdempotencyKeyTurnsTheRollingDeployDuplicateOnDaprIntoOneCharge` |
| Compensation outlasts a six-second provider outage on both engines | `RemedyTest.compensationOutlastsAProviderOutage` |
| A heartbeat alone still leaves Temporal waiting 5 s on the dead worker's sticky queue | `RemedyTest.aHeartbeatAloneLeavesTemporalWaitingForTheDeadWorkersStickyQueue` |
| Heartbeat plus a 1 s sticky queue timeout: Temporal recovers from a crash within Dapr's range | `RemedyTest.aHeartbeatAndAShortStickyTimeoutLetTemporalRecoverFromACrashInAboutASecond` |
| A hung charge is refunded once when the saga treats a timeout as an unknown outcome | `RemedyTest.aHungChargeIsRefundedOnceWhenTheSagaTreatsATimeoutAsAnUnknownOutcome` |
| A provider that settles late: the saga refunds only after the key's final status is known | `RemedyTest.aLateProviderCanChargeAfterTheRefundUnlessTheSagaAsksForTheFinalStatus` |
| The puzzle: a client timeout on Dapr charges three times and reports a failure | `RemedyTest.thePuzzleAClientTimeoutOnDaprChargesThreeTimesAndReportsAFailure` |
| Step cost and round trips per link | `StepCostTest` |
| Redis commands and SQL statements per step | `StoreOpsTest` |

Results that differed between runs (a Temporal server restart, a 10 s PostgreSQL cut, a 2 s PostgreSQL blackhole) are
not asserted; they are in `runs/`.

## Limits

Single nodes, one laptop, one workflow at a time. Redis is not a production actor state store, as `daprd` itself warns.
Both SDKs in one build need a single pinned gRPC and Jackson version, which a real service would not.

## Author

Michał Woliński, [wolinski.com](https://wolinski.com)

## License

MIT, see [LICENSE](LICENSE).
