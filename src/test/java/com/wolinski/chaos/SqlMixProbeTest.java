package com.wolinski.chaos;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Observation only: which SQL statements one Temporal step issues, with a fast and with a slow database link. */
@Tag("explore")
class SqlMixProbeTest {
    private static final int STEPS = 40;

    @TempDir Path dir;

    @Test
    void sqlMix() throws Exception {
        try (Lab lab = new Lab().start(); var temporal = new TemporalEngine(lab)) {
            String psql = "psql -U temporal -d temporal -tA -F ' | ' -c ";
            lab.exec(lab.postgres, "sh", "-c", psql + "'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'");
            temporal.startWorker(dir.resolve("ledger"), dir, Files.createTempFile(dir, "t", ".log"));
            temporal.runSteps("warm", 10, Duration.ofMinutes(1));
            for (int delay : new int[] {0, 5}) {
                if (0 < delay) {
                    lab.latency("temporal-store", delay);
                }
                lab.exec(lab.postgres, "sh", "-c", psql + "'SELECT pg_stat_statements_reset()'");
                Duration took = temporal.runSteps("mix-" + delay, STEPS, Duration.ofMinutes(3));
                System.out.println("SQLMIX delay=" + delay + "ms took=" + took.toMillis() + "ms per-step calls | query");
                System.out.println(lab.exec(lab.postgres, "sh", "-c", psql + "\"SELECT round(calls / " + STEPS + ".0, 2), "
                        + "left(regexp_replace(query, '\\s+', ' ', 'g'), 110) FROM pg_stat_statements "
                        + "WHERE query NOT ILIKE '%pg_stat%' ORDER BY calls DESC LIMIT 25\""));
                lab.heal("temporal-store");
            }
        }
    }
}
