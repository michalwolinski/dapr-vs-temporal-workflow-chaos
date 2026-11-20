package com.wolinski.chaos.temporal;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderSaga {
    @WorkflowMethod
    String place(String order);
}
