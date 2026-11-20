package com.wolinski.chaos;

import com.wolinski.chaos.dapr.DaprWorkerMain;
import com.wolinski.chaos.dapr.OrderSaga;
import com.wolinski.chaos.dapr.Steps;
import io.dapr.workflows.client.DaprWorkflowClient;
import io.dapr.workflows.client.WorkflowState;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.testcontainers.containers.GenericContainer;

final class DaprEngine implements Engine {
    private final Lab lab;
    private final DaprWorkflowClient client;
    private Process worker;

    DaprEngine(Lab lab) {
        this.lab = lab;
        this.client = new DaprWorkflowClient(DaprWorkerMain.properties(lab.daprClient()));
    }

    @Override
    public String name() {
        return "Dapr";
    }

    @Override
    public String appLink() {
        return "dapr-app";
    }

    @Override
    public String storeLink() {
        return "dapr-store";
    }

    @Override
    public GenericContainer<?> container() {
        return lab.daprd;
    }

    @Override
    public void startWorker(Path ledger, Path holds, Path log) throws IOException {
        worker = Jvm.start(log, DaprWorkerMain.class, lab.daprApp(), ledger.toString(), holds.toString());
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

    /**
     * Dapr refuses to start a workflow until a worker for the app is connected to the sidecar, so a start right after
     * the worker JVM launches has to wait for it. Temporal accepts the start and queues it for a worker.
     */
    @Override
    public void startSaga(String order) {
        start(OrderSaga.class, order);
    }

    @Override
    public void startSafeSaga(String order) {
        start(com.wolinski.chaos.dapr.SafeOrderSaga.class, order);
    }

    private void start(Class<? extends io.dapr.workflows.Workflow> saga, String order) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) {
            try {
                client.scheduleNewWorkflow(saga, order, order);
                return;
            } catch (io.grpc.StatusRuntimeException noWorkerYet) {
                if (io.grpc.Status.Code.FAILED_PRECONDITION != noWorkerYet.getStatus().getCode() || System.nanoTime() > deadline) {
                    throw noWorkerYet;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw noWorkerYet;
                }
            }
        }
    }

    @Override
    public Duration runSteps(String id, int n, Duration timeout) throws Exception {
        long start = System.nanoTime();
        client.scheduleNewWorkflow(Steps.class, n, id);
        WorkflowState state = client.waitForWorkflowCompletion(id, timeout, false);
        long elapsed = System.nanoTime() - start;
        if (null == state || false == state.isCompleted()) {
            throw new IllegalStateException(id + " did not complete: " + (null == state ? "no state" : state.getRuntimeStatus()));
        }
        return Duration.ofNanos(elapsed);
    }

    /** The client reaches the engine through the sidecar too, so while the sidecar is down the client is down. */
    @Override
    public String outcome(String order, Duration wait) {
        long deadline = System.nanoTime() + wait.toNanos();
        while (true) {
            try {
                return outcomeOnce(order, Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
            } catch (io.grpc.StatusRuntimeException sidecarDown) {
                if (System.nanoTime() > deadline) {
                    return "CLIENT-" + sidecarDown.getStatus().getCode();
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return "INTERRUPTED";
                }
            }
        }
    }

    private String outcomeOnce(String order, Duration wait) {
        try {
            WorkflowState state = client.waitForWorkflowCompletion(order, wait, true);
            if (null != state && state.isCompleted()) {
                return switch (state.getRuntimeStatus()) {
                    case COMPLETED -> state.readOutputAs(String.class);
                    default -> state.getRuntimeStatus().name();
                };
            }
        } catch (TimeoutException stillRunning) {
            // fall through to the status below
        }
        WorkflowState state = client.getWorkflowState(order, false);
        return null == state ? "UNKNOWN" : state.getRuntimeStatus().name();
    }

    @Override
    public void close() throws Exception {
        crashWorker();
        client.close();
    }
}
