package com.wolinski.chaos.dapr;

import io.dapr.durabletask.TaskFailedException;
import io.dapr.workflows.Workflow;
import io.dapr.workflows.WorkflowStub;
import io.dapr.workflows.WorkflowTaskOptions;
import io.dapr.workflows.WorkflowTaskRetryPolicy;
import java.time.Duration;

/** Reserve, charge, ship; if the carrier refuses, refund and release. The sidecar replays this from its history. */
public class OrderSaga implements Workflow {
    static final WorkflowTaskOptions RETRY = new WorkflowTaskOptions(WorkflowTaskRetryPolicy.newBuilder()
            .setMaxNumberOfAttempts(3)
            .setFirstRetryInterval(Duration.ofMillis(200))
            .build());

    @Override
    public WorkflowStub create() {
        return ctx -> {
            String order = ctx.getInput(String.class);
            ctx.callActivity("reserve", order, RETRY).await();
            ctx.callActivity("charge", order, RETRY).await();
            try {
                ctx.callActivity("ship", order, RETRY).await();
            } catch (TaskFailedException refused) {
                ctx.callActivity("refund", order, RETRY).await();
                ctx.callActivity("release", order, RETRY).await();
                ctx.complete("cancelled");
                return;
            }
            ctx.complete("shipped");
        };
    }
}
