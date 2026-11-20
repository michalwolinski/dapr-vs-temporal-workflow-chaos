package com.wolinski.chaos.temporal;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface SafeOrderSaga {
    @WorkflowMethod
    String place(String order);
}
