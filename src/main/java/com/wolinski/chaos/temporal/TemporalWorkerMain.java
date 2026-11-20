package com.wolinski.chaos.temporal;

import com.wolinski.chaos.shop.Ledger;
import com.wolinski.chaos.shop.Shop;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A worker in its own JVM, so that a crash is a real crash. Args: frontend address, ledger, hold dir, and optionally
 * the sticky queue's schedule-to-start timeout in milliseconds (the SDK's default is 5 s).
 */
public final class TemporalWorkerMain {
    public static final String QUEUE = "orders";

    private TemporalWorkerMain() {}

    public static void main(String[] args) throws InterruptedException {
        WorkflowClient client = WorkflowClient.newInstance(WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder().setTarget(args[0]).build()));
        WorkerFactory factory = WorkerFactory.newInstance(client);
        var options = WorkerOptions.newBuilder();
        if (3 < args.length) {
            options.setStickyQueueScheduleToStartTimeout(Duration.ofMillis(Long.parseLong(args[3])));
        }
        var worker = factory.newWorker(QUEUE, options.build());
        worker.registerWorkflowImplementationTypes(OrderSagaImpl.class, SafeOrderSagaImpl.class, StepsImpl.class);
        worker.registerActivitiesImplementations(new ShopActivitiesImpl(new Shop(new Ledger(Path.of(args[1])), Path.of(args[2]))));
        // A deploy sends SIGTERM: stop polling, let the tasks already received finish, then exit.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            factory.shutdown();
            factory.awaitTermination(60, TimeUnit.SECONDS);
        }));
        factory.start();
        Thread.currentThread().join();
    }
}
