package com.wolinski.chaos;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;

/** What a test needs from either engine, so that every scenario runs the same way on both. */
interface Engine extends AutoCloseable {

    String name();

    /** The proxy between the worker JVM and the engine. */
    String appLink();

    /** The proxy between the engine and its database. */
    String storeLink();

    /** The container that holds the engine: the Dapr sidecar, or the Temporal server. */
    GenericContainer<?> container();

    void startWorker(Path ledger, Path holds, Path log) throws IOException;

    void crashWorker() throws InterruptedException;

    /** Starts a new worker, then sends the old one SIGTERM; returns the old process, which may still be shutting down. */
    Process replaceWorker(Path ledger, Path holds, Path log) throws IOException;

    void startSaga(String order);

    /** The saga with an idempotency key, refunds on an unknown outcome, and (on Temporal) heartbeats. */
    void startSafeSaga(String order);

    /** Starts the step-cost workflow and waits for it; returns how long that took on the client's clock. */
    Duration runSteps(String id, int n, Duration timeout) throws Exception;

    /** "shipped" or "cancelled" when the saga finished within the wait, otherwise the engine's status for it. */
    String outcome(String order, Duration wait);

    @Override
    void close() throws Exception;
}
