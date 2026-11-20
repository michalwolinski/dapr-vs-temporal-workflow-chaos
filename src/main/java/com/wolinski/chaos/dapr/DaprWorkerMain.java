package com.wolinski.chaos.dapr;

import com.wolinski.chaos.shop.Ledger;
import com.wolinski.chaos.shop.Shop;
import io.dapr.config.Properties;
import io.dapr.workflows.WorkflowActivity;
import io.dapr.workflows.runtime.WorkflowRuntimeBuilder;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** The application next to the sidecar, in its own JVM so that a crash is a real crash. Args: sidecar gRPC endpoint, ledger, hold dir. */
public final class DaprWorkerMain {
    private DaprWorkerMain() {}

    /** Activities running in this process right now, for the drain-on-shutdown experiment. */
    private static final AtomicInteger RUNNING = new AtomicInteger();

    public static void main(String[] args) throws InterruptedException {
        Shop shop = new Shop(new Ledger(Path.of(args[1])), Path.of(args[2]));
        var builder = new WorkflowRuntimeBuilder(properties(args[0]))
                .registerWorkflow(OrderSaga.class)
                .registerWorkflow(SafeOrderSaga.class)
                .registerWorkflow(Steps.class)
                .registerActivity("reserve", activity(shop::reserve))
                .registerActivity("charge", activity(shop::charge))
                .registerActivity("ship", activity(shop::ship))
                .registerActivity("refund", activity(shop::refund))
                .registerActivity("charge-once", counted(ctx -> {
                    var keyed = ctx.getInput(SafeOrderSaga.Keyed.class);
                    shop.charge(keyed.order(), keyed.key(), () -> {});
                    return null;
                }))
                .registerActivity("charge-status", (WorkflowActivity) ctx -> shop.chargeStatus(ctx.getInput(String.class)))
                .registerActivity("refund-once", (WorkflowActivity) ctx -> {
                    var keyed = ctx.getInput(SafeOrderSaga.Keyed.class);
                    shop.refund(keyed.order(), keyed.key());
                    return null;
                })
                .registerActivity("release", activity(shop::release))
                .registerActivity("noop", (WorkflowActivity) ctx -> ctx.getInput(Integer.class));
        var runtime = builder.build();
        // A deploy sends SIGTERM; close the runtime the way a framework closing its context would. With a drain file in
        // the hold dir, first wait (up to 25 s) for the activities this process is running: the SDK has no drain of its
        // own, and close() closes the stream before the thread pool.
        boolean drain = Files.exists(Path.of(args[2]).resolve("drain"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            long deadline = System.nanoTime() + 25_000_000_000L;
            while (drain && 0 < RUNNING.get() && System.nanoTime() < deadline) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    break;
                }
            }
            runtime.close();
        }));
        runtime.start(false);
        Thread.currentThread().join();
    }

    public static Properties properties(String endpoint) {
        return new Properties(Map.of(Properties.GRPC_ENDPOINT.getName(), endpoint));
    }

    private static WorkflowActivity activity(Consumer<String> step) {
        return counted(ctx -> {
            step.accept(ctx.getInput(String.class));
            return null;
        });
    }

    private static WorkflowActivity counted(WorkflowActivity activity) {
        return ctx -> {
            RUNNING.incrementAndGet();
            try {
                return activity.run(ctx);
            } finally {
                RUNNING.decrementAndGet();
            }
        };
    }
}
