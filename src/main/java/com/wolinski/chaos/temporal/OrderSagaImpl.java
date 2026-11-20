package com.wolinski.chaos.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** The same saga as the Dapr one. The difference is the timeout: Temporal will not start an activity without one. */
public class OrderSagaImpl implements OrderSaga {
    private final ShopActivities shop = Workflow.newActivityStub(ShopActivities.class, ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(5))
            .setRetryOptions(RetryOptions.newBuilder()
                    .setMaximumAttempts(3)
                    .setInitialInterval(Duration.ofMillis(200))
                    .build())
            .build());

    @Override
    public String place(String order) {
        shop.reserve(order);
        shop.charge(order);
        try {
            shop.ship(order);
        } catch (ActivityFailure refused) {
            shop.refund(order);
            shop.release(order);
            return "cancelled";
        }
        return "shipped";
    }
}
