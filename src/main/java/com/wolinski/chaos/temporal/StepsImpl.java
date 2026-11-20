package com.wolinski.chaos.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** N activities that do nothing, one after another: what is left is the cost of a step. */
public class StepsImpl implements Steps {
    private final ShopActivities shop = Workflow.newActivityStub(ShopActivities.class, ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(5))
            .build());

    @Override
    public int run(int n) {
        for (int i = 0; i < n; i++) {
            shop.noop(i);
        }
        return n;
    }
}
