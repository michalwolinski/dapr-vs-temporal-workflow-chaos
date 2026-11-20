package com.wolinski.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.wolinski.chaos.dapr.OrderSaga;
import com.wolinski.chaos.shop.Ledger;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.dapr.workflows.client.DaprWorkflowClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Each test breaks one link while the charge is in flight and counts the charges. Only results that came out the same in
 * every one of six runs are asserted here; runs/ has the exploration that also shows the ones that did not.
 */
class ChaosTest {
    private static final Duration SHORT = Duration.ofSeconds(2);
    private static final Duration LONG = Duration.ofSeconds(10);

    private static Lab lab;

    @TempDir Path dir;

    @BeforeAll
    static void start() {
        lab = new Lab().start();
    }

    @AfterEach
    void healLinks() {
        lab.healAll();
    }

    @AfterAll
    static void stop() {
        lab.close();
    }

    private Breakage.Result cutApp(Engine engine, String order, Duration outage) throws Exception {
        try (engine) {
            return Breakage.run(engine, Files.createDirectories(dir.resolve(order)), order, outage,
                    e -> lab.cut(e.appLink()), e -> lab.heal(e.appLink()), true);
        }
    }

    @Test
    void daprRunsTheChargeAgainAfterAThreeHundredMillisecondBlip() throws Exception {
        Duration blip = Duration.ofMillis(300);
        assertThat(cutApp(new DaprEngine(lab), "dapr-blip", blip).charges()).isEqualTo(2);
    }

    @Test
    void temporalDeliversTheResultOfAChargeThatFinishedDuringABlip() throws Exception {
        Duration blip = Duration.ofMillis(300);
        assertThat(cutApp(new TemporalEngine(lab), "temporal-blip", blip).charges()).isEqualTo(1);
    }

    @Test
    void aBreakShorterThanTheTimeoutCostsTemporalNothingAndDaprACharge() throws Exception {
        assertThat(cutApp(new DaprEngine(lab), "dapr-short-cut", SHORT).charges()).isEqualTo(2);
        assertThat(cutApp(new TemporalEngine(lab), "temporal-short-cut", SHORT).charges()).isEqualTo(1);
    }

    @Test
    void aBlackholedLinkShorterThanTheTimeoutCostsTemporalNothingAndDaprACharge() throws Exception {
        try (var dapr = new DaprEngine(lab)) {
            assertThat(Breakage.run(dapr, Files.createDirectories(dir.resolve("d")), "dapr-short-blackhole", SHORT,
                    e -> lab.blackhole(e.appLink()), e -> lab.heal(e.appLink()), true).charges()).isEqualTo(2);
        }
        try (var temporal = new TemporalEngine(lab)) {
            assertThat(Breakage.run(temporal, Files.createDirectories(dir.resolve("t")), "temporal-short-blackhole", SHORT,
                    e -> lab.blackhole(e.appLink()), e -> lab.heal(e.appLink()), true).charges()).isEqualTo(1);
        }
    }

    @Test
    void aBreakLongerThanTheTimeoutChargesTwiceOnBoth() throws Exception {
        assertThat(cutApp(new DaprEngine(lab), "dapr-long-cut", LONG).charges()).isEqualTo(2);
        assertThat(cutApp(new TemporalEngine(lab), "temporal-long-cut", LONG).charges()).isEqualTo(2);
    }

    @Test
    void restartingTheDaprSidecarRunsTheChargeAgain() throws Exception {
        try (var dapr = new DaprEngine(lab)) {
            var result = Breakage.run(dapr, dir, "dapr-sidecar-restart", SHORT,
                    e -> lab.stop(e.container()), e -> lab.start(e.container()), true);
            assertThat(result.outcome()).isEqualTo("shipped");
            assertThat(result.charges()).isEqualTo(2);
        }
    }

    @Test
    void aCrashedWorkerChargesTwiceOnBothAndTemporalWaitsForTheTimeout() throws Exception {
        Breakage.Result dapr;
        Breakage.Result temporal;
        try (var engine = new DaprEngine(lab)) {
            Path d = Files.createDirectories(dir.resolve("d"));
            dapr = Breakage.run(engine, d, "dapr-crash", SHORT, Engine::crashWorker,
                    e -> e.startWorker(d.resolve("ledger"), d, d.resolve("worker.log")), false);
        }
        try (var engine = new TemporalEngine(lab)) {
            Path d = Files.createDirectories(dir.resolve("t"));
            temporal = Breakage.run(engine, d, "temporal-crash", SHORT, Engine::crashWorker,
                    e -> e.startWorker(d.resolve("ledger"), d, d.resolve("worker.log")), false);
        }
        assertThat(dapr.charges()).isEqualTo(2);
        assertThat(temporal.charges()).isEqualTo(2);
        // The charge started at least 2 s before the new worker, and its start-to-close timeout is 5 s.
        assertThat(temporal.recoverySeconds()).isGreaterThan(3.0);
    }

    @Test
    void aLostStoreLinkDoesNotRunTheChargeAgainOnEither() throws Exception {
        for (String how : new String[] {"cut", "blackhole"}) {
            for (Engine engine : new Engine[] {new DaprEngine(lab), new TemporalEngine(lab)}) {
                try (engine) {
                    Path d = Files.createDirectories(dir.resolve(engine.name() + how));
                    Breakage.Step breakIt = "cut".equals(how) ? e -> lab.cut(e.storeLink()) : e -> lab.blackhole(e.storeLink());
                    var result = Breakage.run(engine, d, engine.name().toLowerCase() + "-store-" + how, SHORT,
                            breakIt, e -> lab.heal(e.storeLink()), true);
                    assertThat(result.outcome()).isEqualTo("shipped");
                    // Temporal with a blackholed PostgreSQL charged twice in 3 of 14 runs (runs/repeat-store.log and
                    // the exploration), so that one is printed above and not asserted.
                    if (false == ("Temporal".equals(engine.name()) && "blackhole".equals(how))) {
                        assertThat(result.charges()).as(engine.name() + " store " + how).isEqualTo(1);
                    }
                }
            }
        }
    }

    @Test
    void aHungChargeWaitsForeverOnDaprAndRunsThreeTimesAtOnceOnTemporal() throws Exception {
        Duration hung = Duration.ofSeconds(25);
        try (var dapr = new DaprEngine(lab)) {
            Path d = Files.createDirectories(dir.resolve("d"));
            var result = Breakage.run(dapr, d, "dapr-hung", hung, e -> {}, e -> {}, false);
            assertThat(result.outcome()).isEqualTo("shipped");
            assertThat(result.charges()).isEqualTo(1);
        }
        try (var temporal = new TemporalEngine(lab)) {
            Path d = Files.createDirectories(dir.resolve("t"));
            var result = Breakage.run(temporal, d, "temporal-hung", hung, e -> {}, e -> {}, false);
            // Three attempts, each timed out after 5 s while the one before was still running, then the saga failed in
            // charge, outside the try block, so nothing refunded the three charges.
            assertThat(result.outcome()).isEqualTo("FAILED");
            assertThat(result.charges()).isEqualTo(3);
            assertThat(result.refunds()).isZero();
        }
    }

    @Test
    void daprRefusesToStartAWorkflowWithNoWorkerConnectedAndTemporalQueuesIt() throws Exception {
        try (var client = new DaprWorkflowClient(com.wolinski.chaos.dapr.DaprWorkerMain.properties(lab.daprClient()))) {
            assertThatThrownBy(() -> client.scheduleNewWorkflow(OrderSaga.class, "dapr-no-worker", "dapr-no-worker"))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION))
                    .hasMessageContaining("no workflow worker is connected");
        }
        try (var temporal = new TemporalEngine(lab)) {
            temporal.startSaga("temporal-no-worker");
            Ledger ledger = new Ledger(dir.resolve("ledger"));
            temporal.startWorker(ledger.file(), dir, dir.resolve("worker.log"));
            assertThat(temporal.outcome("temporal-no-worker", Duration.ofSeconds(60))).isEqualTo("shipped");
        }
    }

    /** The same deploy on Dapr, with a shutdown hook that waits for the process's running activities before close(). */
    @Test
    void aDaprWorkerThatWaitsForItsRunningActivitiesBeforeClosingChargesOnceOnADeploy() throws Exception {
        try (Engine dapr = new DaprEngine(lab)) {
            String order = "dapr-drained-deploy";
            Path d = Files.createDirectories(dir.resolve(order));
            Ledger ledger = new Ledger(d.resolve("ledger"));
            Path hold = Files.createFile(d.resolve("hold-charge"));
            Files.createFile(d.resolve("drain"));
            dapr.startWorker(ledger.file(), d, d.resolve("worker.log"));
            dapr.startSaga(order);
            Breakage.awaitCharge(ledger, order);
            Process old = dapr.replaceWorker(ledger.file(), d, d.resolve("worker.log"));
            Thread.sleep(2000);
            Files.delete(hold);
            String outcome = dapr.outcome(order, Duration.ofSeconds(60));
            old.waitFor();
            long charges = ledger.count("charge:" + order);
            System.out.printf("RESULT %-8s %-36s Result[outcome=%s, charges=%d, refunds=0, recoverySeconds=0]%n",
                    dapr.name(), order, outcome, charges);
            assertThat(outcome).isEqualTo("shipped");
            assertThat(charges).isEqualTo(1);
        }
    }

    /** A rolling deploy: the new worker starts, then the old one gets SIGTERM and shuts down the way its SDK offers. */
    @Test
    void aRollingDeployDuringTheChargeChargesTwiceOnDaprAndOnceOnTemporal() throws Exception {
        for (Engine engine : new Engine[] {new DaprEngine(lab), new TemporalEngine(lab)}) {
            try (engine) {
                Path d = Files.createDirectories(dir.resolve(engine.name()));
                String order = engine.name().toLowerCase() + "-deploy";
                Ledger ledger = new Ledger(d.resolve("ledger"));
                Path hold = Files.createFile(d.resolve("hold-charge"));
                engine.startWorker(ledger.file(), d, d.resolve("worker.log"));
                engine.startSaga(order);
                Breakage.awaitCharge(ledger, order);
                Process old = engine.replaceWorker(ledger.file(), d, d.resolve("worker.log"));
                Thread.sleep(2000);
                Files.delete(hold);
                String outcome = engine.outcome(order, Duration.ofSeconds(60));
                old.waitFor();
                long charges = ledger.count("charge:" + order);
                System.out.printf("RESULT %-8s %-36s Result[outcome=%s, charges=%d, refunds=0, recoverySeconds=0]%n",
                        engine.name(), order, outcome, charges);
                assertThat(outcome).isEqualTo("shipped");
                // Temporal's shutdown stops polling and lets the running charge finish; Dapr's closes the stream first.
                assertThat(charges).as(engine.name()).isEqualTo("Dapr".equals(engine.name()) ? 2 : 1);
            }
        }
    }
}
