package com.wolinski.chaos.temporal;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface Steps {
    @WorkflowMethod
    int run(int n);
}
