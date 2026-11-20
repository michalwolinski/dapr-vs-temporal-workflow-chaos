package com.wolinski.chaos.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * The saga as it should be written for an engine that runs activities at least once: an idempotency key on the charge;
 * when the charge's outcome is unknown, ask the provider for the key's final status and refund only once it is known;
 * and a heartbeat timeout so that a dead attempt is noticed in about a second instead of after the whole
 * start-to-close timeout.
 */
public class SafeOrderSagaImpl implements SafeOrderSaga {
    private final ShopActivities shop = Workflow.newActivityStub(ShopActivities.class, ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(5))
            .setHeartbeatTimeout(Duration.ofSeconds(1))
            .setRetryOptions(RetryOptions.newBuilder()
                    .setMaximumAttempts(3)
                    .setInitialInterval(Duration.ofMillis(200))
                    .build())
            .build());
    /** Compensation must not give up after a short provider outage: back off up to 10 s, for up to an hour. */
    private final ShopActivities compensation = Workflow.newActivityStub(ShopActivities.class, ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(5))
            .setScheduleToCloseTimeout(Duration.ofHours(1))
            .setRetryOptions(RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofMillis(200))
                    .setMaximumInterval(Duration.ofSeconds(10))
                    .build())
            .build());

    @Override
    public String place(String order) {
        String key = "order-" + Workflow.getInfo().getWorkflowId();
        shop.reserve(order);
        try {
            shop.chargeOnce(order, key);
        } catch (ActivityFailure unknownOutcome) {
            refundWhenSettled(order, key);
            compensation.release(order);
            return "cancelled";
        }
        try {
            shop.ship(order);
        } catch (ActivityFailure refused) {
            compensation.refundOnce(order, key);
            compensation.release(order);
            return "cancelled";
        }
        return "shipped";
    }

    /** A provider may still process a charge the saga gave up on, so wait for its final status before refunding. */
    private void refundWhenSettled(String order, String key) {
        for (int check = 0; check < 300; check++) {
            String status = compensation.chargeStatus(key);
            if ("PENDING".equals(status)) {
                Workflow.sleep(Duration.ofSeconds(1));
                continue;
            }
            if ("CHARGED".equals(status)) {
                compensation.refundOnce(order, key);
            }
            return;
        }
        throw ApplicationFailure.newFailure("charge " + key + " still pending after five minutes", "ChargeUnsettled");
    }
}
