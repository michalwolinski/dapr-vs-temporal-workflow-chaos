package com.wolinski.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.wolinski.chaos.shop.Ledger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The fixes the article recommends, tested against the same breakages: an idempotency key on the charge, a refund by
 * that key when the charge's outcome is unknown, and on Temporal a heartbeat timeout. Also the puzzle.
 */
class RemedyTest {
    private static final Duration SHORT = Duration.ofSeconds(2);

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

    private Breakage.Result safely(Engine engine, String order, Duration outage, Breakage.Step breakIt,
            Breakage.Step fixIt, boolean finishDuringOutage) throws Exception {
        try (engine) {
            return Breakage.run(engine, Files.createDirectories(dir.resolve(order)), order, outage, breakIt, fixIt,
                    finishDuringOutage, true);
        }
    }

    private Breakage.Step restartWorker(String order) {
        return e -> e.startWorker(dir.resolve(order).resolve("ledger"), dir.resolve(order), dir.resolve(order).resolve("worker.log"));
    }

    private long repeats(String order) {
        return new Ledger(dir.resolve(order).resolve("ledger")).count("charge-repeated:" + order);
    }

    @Test
    void anIdempotencyKeyTurnsEveryRepeatedChargeIntoOne() throws Exception {
        var daprBlip = safely(new DaprEngine(lab), "safe-dapr-blip", Duration.ofMillis(300),
                e -> lab.cut(e.appLink()), e -> lab.heal(e.appLink()), true);
        var daprCrash = safely(new DaprEngine(lab), "safe-dapr-crash", SHORT,
                Engine::crashWorker, restartWorker("safe-dapr-crash"), false);
        var temporalCrash = safely(new TemporalEngine(lab), "safe-temporal-crash", SHORT,
                Engine::crashWorker, restartWorker("safe-temporal-crash"), false);
        var temporalLongCut = safely(new TemporalEngine(lab), "safe-temporal-long-cut", Duration.ofSeconds(10),
                e -> lab.cut(e.appLink()), e -> lab.heal(e.appLink()), true);

        for (var result : new Breakage.Result[] {daprBlip, daprCrash, temporalCrash, temporalLongCut}) {
            assertThat(result.outcome()).isEqualTo("shipped");
            assertThat(result.charges()).isEqualTo(1);
        }
        // The engines still ran the charge twice; the provider just recognised the key the second time.
        for (String order : new String[] {"safe-dapr-blip", "safe-dapr-crash", "safe-temporal-crash", "safe-temporal-long-cut"}) {
            assertThat(repeats(order)).as(order).isEqualTo(1);
        }
    }

    /** The headline case: on Dapr the old worker is still finishing its charge when the new one starts the same one. */
    @Test
    void anIdempotencyKeyTurnsTheRollingDeployDuplicateOnDaprIntoOneCharge() throws Exception {
        String order = "safe-dapr-deploy";
        Path d = Files.createDirectories(dir.resolve(order));
        Ledger ledger = new Ledger(d.resolve("ledger"));
        Path hold = Files.createFile(d.resolve("hold-charge"));
        String outcome;
        try (var dapr = new DaprEngine(lab)) {
            dapr.startWorker(ledger.file(), d, d.resolve("worker.log"));
            dapr.startSafeSaga(order);
            Breakage.awaitCharge(ledger, order);
            Process old = dapr.replaceWorker(ledger.file(), d, d.resolve("worker.log"));
            Thread.sleep(2000);
            Files.delete(hold);
            outcome = dapr.outcome(order, Duration.ofSeconds(60));
            old.waitFor();
        }
        System.out.printf("RESULT %-8s %-36s Result[outcome=%s, charges=%d, refunds=0, recoverySeconds=0]%n",
                "Dapr", order, outcome, ledger.count("charge:" + order));
        assertThat(outcome).isEqualTo("shipped");
        assertThat(ledger.count("charge:" + order)).isEqualTo(1);
        // The second call arrived while the first was still waiting for the provider: the overlapping-request case.
        assertThat(repeats(order)).isEqualTo(1);
    }

    /**
     * The charge fails three times on a client timeout, and the provider goes down for six seconds just as the saga
     * starts to compensate. Compensation that retries as briefly as the charge would fail the workflow with the money
     * taken; it has to outlast the outage.
     */
    @Test
    void compensationOutlastsAProviderOutage() throws Exception {
        for (Engine engine : new Engine[] {new DaprEngine(lab), new TemporalEngine(lab)}) {
            try (engine) {
                String order = engine.name().toLowerCase() + "-compensation-outage";
                Path d = Files.createDirectories(dir.resolve(order));
                Ledger ledger = new Ledger(d.resolve("ledger"));
                Path hold = Files.createFile(d.resolve("hold-charge"));
                Files.writeString(d.resolve("client-timeout-charge"), "1000");
                engine.startWorker(ledger.file(), d, d.resolve("worker.log"));
                engine.startSafeSaga(order);
                Breakage.awaitCharge(ledger, order);
                while (ledger.count("charge-repeated:" + order) < 2) {
                    Thread.sleep(20);
                }
                // The third and last attempt is running; its client timeout ends it within a second.
                Path down = Files.createFile(d.resolve("provider-down"));
                Thread.sleep(6000);
                Files.delete(down);
                Files.delete(hold);
                String outcome = engine.outcome(order, Duration.ofSeconds(60));
                long charges = ledger.count("charge:" + order);
                long refunds = ledger.count("refund:" + order);
                System.out.printf("RESULT %-8s %-36s Result[outcome=%s, charges=%d, refunds=%d, recoverySeconds=0]%n",
                        engine.name(), order, outcome, charges, refunds);
                assertThat(outcome).as(engine.name()).isEqualTo("cancelled");
                assertThat(charges).as(engine.name()).isEqualTo(1);
                assertThat(refunds).as(engine.name()).isEqualTo(1);
                assertThat(ledger.count("release:" + order)).as(engine.name()).isEqualTo(1);
            }
        }
    }

    @Test
    void aHeartbeatAloneLeavesTemporalWaitingForTheDeadWorkersStickyQueue() throws Exception {
        var result = safely(new TemporalEngine(lab), "heartbeat-crash", SHORT,
                Engine::crashWorker, restartWorker("heartbeat-crash"), false);
        assertThat(result.charges()).isEqualTo(1);
        // The heartbeat ends the dead attempt within its 1 s timeout, but the workflow task that follows is still
        // offered to the dead worker's sticky queue for the SDK's default 5 s (runs/crash-history-probe.log).
        assertThat(result.recoverySeconds()).isGreaterThan(5.0);
    }

    @Test
    void aHeartbeatAndAShortStickyTimeoutLetTemporalRecoverFromACrashInAboutASecond() throws Exception {
        var result = safely(new TemporalEngine(lab, Duration.ofSeconds(1)), "sticky-crash", SHORT,
                Engine::crashWorker, restartWorker("sticky-crash"), false);
        assertThat(result.charges()).isEqualTo(1);
        assertThat(result.recoverySeconds()).isLessThan(3.0);
    }

    @Test
    void aHungChargeIsRefundedOnceWhenTheSagaTreatsATimeoutAsAnUnknownOutcome() throws Exception {
        // The activity heartbeats from the loop that waits for the provider, so the worker looks alive throughout:
        // the heartbeat timeout never fires, and only the 5 s start-to-close timeout ends each attempt.
        var result = safely(new TemporalEngine(lab), "safe-temporal-hung", Duration.ofSeconds(25),
                e -> {}, e -> {}, false);
        assertThat(result.outcome()).isEqualTo("cancelled");
        assertThat(result.charges()).isEqualTo(1);
        assertThat(result.refunds()).isEqualTo(1);
        assertThat(repeats("safe-temporal-hung")).isEqualTo(2);
    }

    @Test
    void thePuzzleAClientTimeoutOnDaprChargesThreeTimesAndReportsAFailure() throws Exception {
        Path naive = Files.createDirectories(dir.resolve("puzzle-naive"));
        Files.writeString(naive.resolve("client-timeout-charge"), "1000");
        Breakage.Result result;
        try (var dapr = new DaprEngine(lab)) {
            result = Breakage.run(dapr, naive, "puzzle-naive", Duration.ofSeconds(5), e -> {}, e -> {}, false);
        }
        assertThat(result.charges()).isEqualTo(3);
        assertThat(result.outcome()).isEqualTo("FAILED");
        assertThat(result.refunds()).isZero();

        Path safe = Files.createDirectories(dir.resolve("puzzle-safe"));
        Files.writeString(safe.resolve("client-timeout-charge"), "1000");
        Files.createFile(safe.resolve("late-charge"));
        try (var dapr = new DaprEngine(lab)) {
            result = Breakage.run(dapr, safe, "puzzle-safe", Duration.ofSeconds(5), e -> {}, e -> {}, false, true);
        }
        // The safe run also settles late, like the provider in the puzzle: it answers after the hold, not at once.
        Thread.sleep(2000);
        Ledger ledger = new Ledger(safe.resolve("ledger"));
        assertThat(ledger.count("charge:puzzle-safe")).isEqualTo(1);
        assertThat(result.outcome()).isEqualTo("cancelled");
        assertThat(ledger.count("refund:puzzle-safe")).isEqualTo(1);
    }

    /** A provider that processes the charge only when the hold is released, after the saga has given up on it. */
    @Test
    void aLateProviderCanChargeAfterTheRefundUnlessTheSagaAsksForTheFinalStatus() throws Exception {
        Path d = Files.createDirectories(dir.resolve("late-temporal-hung"));
        Files.createFile(d.resolve("late-charge"));
        Breakage.Result result;
        try (var temporal = new TemporalEngine(lab)) {
            result = Breakage.run(temporal, d, "late-temporal-hung", Duration.ofSeconds(25), e -> {}, e -> {}, false, true);
        }
        Thread.sleep(2000);
        Ledger ledger = new Ledger(d.resolve("ledger"));
        System.out.printf("LATE outcome=%s charges=%d refunds=%d%n", result.outcome(),
                ledger.count("charge:late-temporal-hung"), ledger.count("refund:late-temporal-hung"));
        assertThat(result.outcome()).isEqualTo("cancelled");
        assertThat(ledger.count("charge:late-temporal-hung")).isEqualTo(1);
        assertThat(ledger.count("refund:late-temporal-hung")).isEqualTo(1);
    }
}
