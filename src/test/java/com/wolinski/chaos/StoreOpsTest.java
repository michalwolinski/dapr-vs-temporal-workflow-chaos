package com.wolinski.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The store's own count of what one step costs it: Redis commands for Dapr (INFO commandstats), SQL statements for
 * Temporal (pg_stat_statements). Like StepCostTest it takes the slope between a workflow of 1 and of 41 empty
 * activities, so starting and completing the workflow cancel out. Both engines also talk to their store when idle, so
 * the same counter is read across an idle window as long as each run and that is subtracted.
 */
class StoreOpsTest {
    private static final int SHORT = 1;
    private static final int LONG = 41;

    @TempDir Path dir;

    @Test
    void storeOperationsPerStep() throws Exception {
        try (Lab lab = new Lab().start()) {
            lab.exec(lab.postgres, "psql", "-U", "temporal", "-d", "temporal", "-c", "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            LongSupplier redisCommands = () -> redisCalls(lab);
            LongSupplier sqlStatements = () -> number(lab.exec(lab.postgres, "psql", "-U", "temporal", "-d", "temporal", "-tAc",
                    "SELECT coalesce(sum(calls), 0) FROM pg_stat_statements WHERE query NOT ILIKE '%pg_stat%'"));

            try (var dapr = new DaprEngine(lab); var temporal = new TemporalEngine(lab)) {
                dapr.startWorker(dir.resolve("ledger"), dir, Files.createTempFile(dir, "dapr", ".log"));
                temporal.startWorker(dir.resolve("ledger"), dir, Files.createTempFile(dir, "temporal", ".log"));
                dapr.runSteps("warm-d", 5, Duration.ofMinutes(1));
                temporal.runSteps("warm-t", 5, Duration.ofMinutes(1));

                double redis = perStep("Dapr", "redis-commands", redisCommands, (id, n) -> dapr.runSteps(id, n, Duration.ofMinutes(2)));
                double statements = perStep("Temporal", "sql-statements", sqlStatements, (id, n) -> temporal.runSteps(id, n, Duration.ofMinutes(2)));

                assertThat(redis).isPositive();
                assertThat(statements).isPositive();
            }
        }
    }

    interface Run {
        Duration run(String id, int steps) throws Exception;
    }

    private static double perStep(String engine, String counter, LongSupplier read, Run run) throws Exception {
        double shortRun = net(read, () -> run.run(engine + "-ops-short", SHORT));
        double longRun = net(read, () -> run.run(engine + "-ops-long", LONG));
        double perStep = (longRun - shortRun) / (LONG - SHORT);
        System.out.printf(Locale.ROOT, "STORE %-8s %-17s %d steps: %.0f, %d steps: %.0f -> %.1f per step%n",
                engine, counter, SHORT, shortRun, LONG, longRun, perStep);
        return perStep;
    }

    interface Timed {
        Duration run() throws Exception;
    }

    /** What the store counted during one run, minus what it counts idle in the same time. */
    private static double net(LongSupplier read, Timed run) throws Exception {
        long before = read.getAsLong();
        Duration took = run.run();
        long during = read.getAsLong() - before;
        long idleBefore = read.getAsLong();
        Thread.sleep(took.toMillis());
        long idle = read.getAsLong() - idleBefore;
        return during - idle;
    }

    private static long redisCalls(Lab lab) {
        return lab.exec(lab.redis, "redis-cli", "INFO", "commandstats").lines()
                .filter(line -> line.startsWith("cmdstat_") && false == line.startsWith("cmdstat_info"))
                .mapToLong(line -> Long.parseLong(line.replaceAll("^[^:]+:calls=(\\d+),.*", "$1").trim()))
                .sum();
    }

    private static long number(String text) {
        return Long.parseLong(text.trim());
    }
}
