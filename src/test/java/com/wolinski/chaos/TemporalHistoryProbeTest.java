package com.wolinski.chaos;

import com.wolinski.chaos.temporal.Steps;
import com.wolinski.chaos.temporal.TemporalWorkerMain;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Observation only: where the time goes between the events of one empty step. */
@Tag("explore")
class TemporalHistoryProbeTest {
    @TempDir Path dir;

    @Test
    void history() throws Exception {
        try (Lab lab = new Lab().start(); var engine = new TemporalEngine(lab)) {
            engine.startWorker(dir.resolve("ledger"), dir, dir.resolve("worker.log"));
            for (int i = 0; i < 3; i++) {
                engine.runSteps("warm-" + i, 3, java.time.Duration.ofMinutes(1));
            }
            engine.runSteps("probe", 4, java.time.Duration.ofMinutes(1));
            WorkflowClient client = WorkflowClient.newInstance(WorkflowServiceStubs.newServiceStubs(
                    WorkflowServiceStubsOptions.newBuilder().setTarget(lab.temporalClient()).build()));
            WorkflowExecutionHistory history = client.fetchHistory("probe");
            long previous = 0;
            for (HistoryEvent event : history.getEvents()) {
                long t = event.getEventTime().getSeconds() * 1_000_000_000L + event.getEventTime().getNanos();
                System.out.printf("EVENT %3d %-40s +%6.1f ms%n", event.getEventId(),
                        event.getEventType().name().replace("EVENT_TYPE_", ""), 0 == previous ? 0 : (t - previous) / 1e6);
                previous = t;
            }
            System.out.println(TemporalWorkerMain.QUEUE + " " + Steps.class.getSimpleName() + " " + WorkflowOptions.class.getSimpleName());
        }
    }
}
