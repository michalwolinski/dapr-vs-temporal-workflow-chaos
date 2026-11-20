package com.wolinski.chaos;

import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Observation only: where a crashed worker's seconds go on Temporal, with and without heartbeats. */
@Tag("explore")
class CrashHistoryProbeTest {
    @TempDir Path dir;

    @Test
    void history() throws Exception {
        try (Lab lab = new Lab().start()) {
            WorkflowClient client = WorkflowClient.newInstance(WorkflowServiceStubs.newServiceStubs(
                    WorkflowServiceStubsOptions.newBuilder().setTarget(lab.temporalClient()).build()));
            for (boolean safe : new boolean[] {false, true}) {
                String order = safe ? "probe-heartbeat-crash" : "probe-plain-crash";
                Path d = Files.createDirectories(dir.resolve(order));
                try (var engine = new TemporalEngine(lab)) {
                    Breakage.run(engine, d, order, Duration.ofSeconds(2), Engine::crashWorker,
                            e -> e.startWorker(d.resolve("ledger"), d, d.resolve("worker.log")), false, safe);
                }
                long previous = 0;
                for (HistoryEvent event : client.fetchHistory(order).getEvents()) {
                    long t = event.getEventTime().getSeconds() * 1_000_000_000L + event.getEventTime().getNanos();
                    String detail = event.hasWorkflowTaskTimedOutEventAttributes()
                            ? " " + event.getWorkflowTaskTimedOutEventAttributes().getTimeoutType()
                            : event.hasActivityTaskTimedOutEventAttributes()
                            ? " " + event.getActivityTaskTimedOutEventAttributes().getFailure().getTimeoutFailureInfo().getTimeoutType()
                            : "";
                    System.out.printf("HISTORY %-22s %3d %-34s +%7.1f ms%s%n", order, event.getEventId(),
                            event.getEventType().name().replace("EVENT_TYPE_", ""), 0 == previous ? 0 : (t - previous) / 1e6, detail);
                    previous = t;
                }
            }
        }
    }
}
