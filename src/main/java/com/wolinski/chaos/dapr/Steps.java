package com.wolinski.chaos.dapr;

import io.dapr.workflows.Workflow;
import io.dapr.workflows.WorkflowStub;

/** N activities that do nothing, one after another: what is left is the cost of a step. */
public class Steps implements Workflow {
    @Override
    public WorkflowStub create() {
        return ctx -> {
            int n = ctx.getInput(Integer.class);
            for (int i = 0; i < n; i++) {
                ctx.callActivity("noop", i, Integer.class).await();
            }
            ctx.complete(n);
        };
    }
}
