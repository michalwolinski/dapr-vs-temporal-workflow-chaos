package com.wolinski.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What one activity costs, and how many times it crosses each link. The cost of a step is the slope between a workflow
 * of 1 and of 41 empty activities, so start-up and the client's own calls cancel out. Then the same with a delay on one
 * link: the extra cost per step, divided by the round-trip delay, is how many round trips a step makes on that link.
 */
class StepCostTest {
    private static final int SHORT = 1;
    private static final int LONG = 41;
    private static final int REPEATS = 7;
    private static final int DELAY_MS = Integer.getInteger("delay", 5);

    private static Lab lab;

    @TempDir Path dir;

    @BeforeAll
    static void start() {
        lab = new Lab().start();
    }

    @AfterAll
    static void stop() {
        lab.close();
    }

    @Test
    void stepCostAndRoundTrips() throws Exception {
        List<String> report = new ArrayList<>();
        for (Engine engine : List.of(new DaprEngine(lab), new TemporalEngine(lab))) {
            try (engine) {
                engine.startWorker(dir.resolve("ledger"), dir, Files.createTempFile(dir, "worker", ".log"));
                slope(engine, "warmup");
                double base = slope(engine, "base");
                List<String> links = "Dapr".equals(engine.name())
                        ? List.of("dapr-app", "dapr-store")
                        : "dev".equals(System.getProperty("temporal"))
                        ? List.<String>of() // the development server: only its base cost, for comparison
                        : List.of("temporal-app", "temporal-store");
                report.add(line(engine.name(), "none", base, Double.NaN));
                System.out.println(report.getLast());
                for (String link : links) {
                    lab.latency(link, DELAY_MS);
                    double slowed = slope(engine, link);
                    lab.heal(link);
                    double roundTrips = (slowed - base) / (2.0 * DELAY_MS);
                    report.add(line(engine.name(), link, slowed, roundTrips));
                    System.out.println(report.getLast());
                    assertThat(roundTrips).as(engine.name() + " round trips on " + link).isGreaterThan(0.5);
                }
            }
        }
        System.out.println("STEP COST (median of " + REPEATS + ", +" + DELAY_MS + " ms each way on the named link)");
        report.forEach(System.out::println);
    }

    private double slope(Engine engine, String label) throws Exception {
        double[] perStep = new double[REPEATS];
        for (int i = 0; i < REPEATS; i++) {
            String id = engine.name() + "-" + label + "-" + i;
            double shortMs = engine.runSteps(id + "-s", SHORT, Duration.ofMinutes(2)).toNanos() / 1e6;
            double longMs = engine.runSteps(id + "-l", LONG, Duration.ofMinutes(2)).toNanos() / 1e6;
            perStep[i] = (longMs - shortMs) / (LONG - SHORT);
        }
        Arrays.sort(perStep);
        return perStep[REPEATS / 2];
    }

    private static String line(String engine, String link, double ms, double roundTrips) {
        return String.format(Locale.ROOT, "STEP %-8s delay-on=%-12s ms/step=%7.2f round-trips/step=%s",
                engine, link, ms, Double.isNaN(roundTrips) ? "-" : String.format(Locale.ROOT, "%.2f", roundTrips));
    }
}
