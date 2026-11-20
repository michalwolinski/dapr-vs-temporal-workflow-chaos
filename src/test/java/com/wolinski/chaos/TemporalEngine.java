package com.wolinski.chaos;

import com.wolinski.chaos.temporal.OrderSaga;
import com.wolinski.chaos.temporal.Steps;
import com.wolinski.chaos.temporal.TemporalWorkerMain;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testcontainers.containers.GenericContainer;

final class TemporalEngine implements Engine {
    private final Lab lab;
    private final WorkflowServiceStubs service;
    private final WorkflowClient client;
    private final Duration stickyTimeout;
    private Process worker;

    TemporalEngine(Lab lab) {
        this(lab, null);
    }

    /** @param stickyTimeout the workers' sticky queue schedule-to-start timeout, or null for the SDK's 5 s */
    TemporalEngine(Lab lab, Duration stickyTimeout) {
        this.lab = lab;
        this.stickyTimeout = stickyTimeout;
        this.service = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder().setTarget(lab.temporalClient()).build());
        this.client = WorkflowClient.newInstance(service);
    }

    @Override
    public String name() {
        return "Temporal";
    }

    @Override
    public String appLink() {
        return "temporal-app";
    }

    @Override
    public String storeLink() {
        return "temporal-store";
    }

    @Override
    public GenericContainer<?> container() {
        return lab.temporal;
    }

    @Override
    public void startWorker(Path ledger, Path holds, Path log) throws IOException {
        worker = null == stickyTimeout
                ? Jvm.start(log, TemporalWorkerMain.class, lab.temporalApp(), ledger.toString(), holds.toString())
                : Jvm.start(log, TemporalWorkerMain.class, lab.temporalApp(), ledger.toString(), holds.toString(),
                        String.valueOf(stickyTimeout.toMillis()));
    }

    /** A rolling deploy: a new worker starts, then the old one gets SIGTERM while it may still be working. */
    @Override
    public Process replaceWorker(Path ledger, Path holds, Path log) throws IOException {
        Process old = worker;
        startWorker(ledger, holds, log);
        Jvm.terminate(old);
        return old;
    }

    @Override
    public void crashWorker() throws InterruptedException {
        if (null != worker) {
            Jvm.crash(worker);
            worker = null;
        }
    }

    @Override
    public void startSaga(String order) {
        OrderSaga saga = client.newWorkflowStub(OrderSaga.class, options(order));
        WorkflowClient.start(saga::place, order);
    }

    @Override
    public void startSafeSaga(String order) {
        var saga = client.newWorkflowStub(com.wolinski.chaos.temporal.SafeOrderSaga.class, options(order));
        WorkflowClient.start(saga::place, order);
    }

    @Override
    public Duration runSteps(String id, int n, Duration timeout) throws Exception {
        long start = System.nanoTime();
        Steps steps = client.newWorkflowStub(Steps.class, options(id));
        WorkflowClient.start(steps::run, n);
        client.newUntypedWorkflowStub(id).getResult(timeout.toMillis(), TimeUnit.MILLISECONDS, Integer.class);
        return Duration.ofNanos(System.nanoTime() - start);
    }

    @Override
    public String outcome(String order, Duration wait) {
        try {
            return client.newUntypedWorkflowStub(order).getResult(wait.toMillis(), TimeUnit.MILLISECONDS, String.class);
        } catch (TimeoutException stillRunning) {
            return status(order);
        } catch (io.temporal.client.WorkflowFailedException failed) {
            return status(order);
        }
    }

    private String status(String order) {
        return service.blockingStub().describeWorkflowExecution(DescribeWorkflowExecutionRequest.newBuilder()
                        .setNamespace("default")
                        .setExecution(io.temporal.api.common.v1.WorkflowExecution.newBuilder().setWorkflowId(order))
                        .build())
                .getWorkflowExecutionInfo().getStatus().name().replace("WORKFLOW_EXECUTION_STATUS_", "");
    }

    private static WorkflowOptions options(String id) {
        return WorkflowOptions.newBuilder().setTaskQueue(TemporalWorkerMain.QUEUE).setWorkflowId(id).build();
    }

    @Override
    public void close() throws Exception {
        crashWorker();
        service.shutdownNow();
    }
}
