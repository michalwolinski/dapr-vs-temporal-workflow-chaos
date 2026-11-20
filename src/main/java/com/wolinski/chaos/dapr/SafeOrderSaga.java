package com.wolinski.chaos.dapr;

import io.dapr.durabletask.TaskFailedException;
import io.dapr.workflows.Workflow;
import io.dapr.workflows.WorkflowContext;
import io.dapr.workflows.WorkflowStub;
import io.dapr.workflows.WorkflowTaskOptions;
import io.dapr.workflows.WorkflowTaskRetryPolicy;
import java.time.Duration;

/**
 * The saga as it should be written for an engine that runs activities at least once. The charge carries an
 * idempotency key derived from the workflow instance, so a repeated activity cannot charge twice. And a charge that
 * failed is treated as an unknown outcome, not as "nothing happened": the saga asks the provider for the key's final
 * status, waiting on a durable timer while it is pending, refunds if money moved, and releases the stock.
 */
public class SafeOrderSaga implements Workflow {
    /** Compensation must not give up after a short provider outage: back off up to 10 s, for up to an hour. */
    static final WorkflowTaskOptions COMPENSATE = new WorkflowTaskOptions(WorkflowTaskRetryPolicy.newBuilder()
            .setMaxNumberOfAttempts(1000)
            .setFirstRetryInterval(Duration.ofMillis(200))
            .setBackoffCoefficient(2)
            .setMaxRetryInterval(Duration.ofSeconds(10))
            .setRetryTimeout(Duration.ofHours(1))
            .build());

    @Override
    public WorkflowStub create() {
        return ctx -> {
            String order = ctx.getInput(String.class);
            String key = "order-" + ctx.getInstanceId();
            ctx.callActivity("reserve", order, OrderSaga.RETRY).await();
            try {
                ctx.callActivity("charge-once", new Keyed(order, key), OrderSaga.RETRY).await();
            } catch (TaskFailedException unknownOutcome) {
                refundWhenSettled(ctx, order, key);
                ctx.callActivity("release", order, COMPENSATE).await();
                ctx.complete("cancelled");
                return;
            }
            try {
                ctx.callActivity("ship", order, OrderSaga.RETRY).await();
            } catch (TaskFailedException refused) {
                ctx.callActivity("refund-once", new Keyed(order, key), COMPENSATE).await();
                ctx.callActivity("release", order, COMPENSATE).await();
                ctx.complete("cancelled");
                return;
            }
            ctx.complete("shipped");
        };
    }

    /** A provider may still process a charge the saga gave up on, so wait for its final status before refunding. */
    private static void refundWhenSettled(WorkflowContext ctx, String order, String key) {
        for (int check = 0; check < 300; check++) {
            String status = ctx.callActivity("charge-status", key, COMPENSATE, String.class).await();
            if ("PENDING".equals(status)) {
                ctx.createTimer(Duration.ofSeconds(1)).await();
                continue;
            }
            if ("CHARGED".equals(status)) {
                ctx.callActivity("refund-once", new Keyed(order, key), COMPENSATE).await();
            }
            return;
        }
        throw new IllegalStateException("charge " + key + " still pending after five minutes");
    }

    /** An order and the idempotency key the provider sees. */
    public record Keyed(String order, String key) {}
}
