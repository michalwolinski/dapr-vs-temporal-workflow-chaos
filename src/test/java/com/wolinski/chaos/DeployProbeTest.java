package com.wolinski.chaos;

import com.wolinski.chaos.shop.Ledger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Observation only: a rolling deploy while the charge is running. */
@Tag("explore")
class DeployProbeTest {
    @TempDir Path dir;

    @Test
    void deploy() throws Exception {
        try (Lab lab = new Lab().start()) {
            for (int round = 0; round < 3; round++) {
                for (Engine engine : new Engine[] {new DaprEngine(lab), new TemporalEngine(lab)}) {
                    try (engine) {
                        String order = engine.name().toLowerCase() + "-deploy-" + round;
                        Path d = Files.createDirectories(dir.resolve(order));
                        Ledger ledger = new Ledger(d.resolve("ledger"));
                        Path hold = Files.createFile(d.resolve("hold-charge"));
                        engine.startWorker(ledger.file(), d, d.resolve("worker.log"));
                        engine.startSaga(order);
                        Breakage.awaitCharge(ledger, order);
                        Process old = engine.replaceWorker(ledger.file(), d, d.resolve("worker.log"));
                        Thread.sleep(2000);
                        boolean oldStillRunning = old.isAlive();
                        Files.delete(hold);
                        String outcome = engine.outcome(order, Duration.ofSeconds(60));
                        old.waitFor();
                        System.out.printf("DEPLOY %-8s %s outcome=%s charges=%d oldAliveAfter2s=%s oldExit=%d%n", engine.name(),
                                order, outcome, ledger.count("charge:" + order), oldStillRunning, old.exitValue());
                    }
                }
            }
        }
    }
}
