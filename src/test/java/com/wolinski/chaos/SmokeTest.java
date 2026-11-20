package com.wolinski.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.wolinski.chaos.shop.Ledger;
import com.wolinski.chaos.shop.Shop;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Nothing broken yet: both engines run the saga to the end, and the compensation path too. */
class SmokeTest {
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

    @ParameterizedTest
    @ValueSource(strings = {"Dapr", "Temporal"})
    void sagaShipsAndCompensates(String which) throws Exception {
        Ledger ledger = new Ledger(dir.resolve("ledger"));
        try (Engine engine = "Dapr".equals(which) ? new DaprEngine(lab) : new TemporalEngine(lab)) {
            engine.startWorker(ledger.file(), dir, dir.resolve("worker.log"));
            engine.startSaga("ok-1");
            engine.startSaga(Shop.NO_STOCK + "1");

            assertThat(engine.outcome("ok-1", Duration.ofSeconds(60))).isEqualTo("shipped");
            assertThat(engine.outcome(Shop.NO_STOCK + "1", Duration.ofSeconds(60))).isEqualTo("cancelled");
            assertThat(ledger.all()).containsSubsequence(List.of("reserve:ok-1", "charge:ok-1", "ship:ok-1"));
            assertThat(ledger.count("refund:" + Shop.NO_STOCK + "1")).isEqualTo(1);
            assertThat(ledger.count("release:" + Shop.NO_STOCK + "1")).isEqualTo(1);
        }
    }
}
