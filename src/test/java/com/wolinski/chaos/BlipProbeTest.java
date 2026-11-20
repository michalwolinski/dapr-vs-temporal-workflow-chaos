package com.wolinski.chaos;

import com.wolinski.chaos.shop.Ledger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Observation only: the shortest break that still matters, with both logs printed. */
@Tag("explore")
class BlipProbeTest {
    @TempDir Path dir;

    @Test
    void blip() throws Exception {
        try (Lab lab = new Lab().start()) {
            for (String which : new String[] {"Dapr", "Temporal"}) {
                for (int blip : new int[] {300, 0}) {
                    Path d = Files.createDirectories(dir.resolve(which + blip));
                    Ledger ledger = new Ledger(d.resolve("ledger"));
                    try (Engine engine = "Dapr".equals(which) ? new DaprEngine(lab) : new TemporalEngine(lab)) {
                        String order = which.toLowerCase() + "-blip-" + blip;
                        Path hold = Files.createFile(d.resolve("hold-charge"));
                        engine.startWorker(ledger.file(), d, d.resolve("worker.log"));
                        engine.startSaga(order);
                        while (0 == ledger.count("charge:" + order)) {
                            Thread.sleep(20);
                        }
                        Thread.sleep(1000);
                        if (0 < blip) {
                            lab.cut(engine.appLink());
                            Thread.sleep(blip);
                            lab.heal(engine.appLink());
                        }
                        Thread.sleep(1000);
                        Files.delete(hold);
                        String outcome = engine.outcome(order, Duration.ofSeconds(60));
                        System.out.printf("OBSERVED %s blip=%dms outcome=%s ledger=%s%n", which, blip, outcome, ledger.all());
                        System.out.println("WORKER LOG\n" + Files.readString(d.resolve("worker.log")));
                    }
                }
            }
            System.out.println("DAPRD LOG\n" + lab.logs(lab.daprd).lines()
                    .filter(l -> l.contains("orkflow") || l.contains("ctivit") || l.contains("stream"))
                    .reduce("", (a, b) -> a + b + "\n"));
        }
    }
}
