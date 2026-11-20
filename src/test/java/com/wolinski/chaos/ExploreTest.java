package com.wolinski.chaos;

import com.wolinski.chaos.shop.Ledger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Observation only: break each link in the middle of "charge" and print what each engine did. */
@Tag("explore")
class ExploreTest {
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

    interface Step {
        void run(Engine engine) throws Exception;
    }

    record Scenario(String name, Step breakIt, Step fixIt, boolean releaseDuringOutage) {}

    private final List<Function<Path, Scenario>> scenarios = Boolean.getBoolean("hang.only")
            ? List.of(d -> new Scenario("activity-hangs", e -> {}, e -> {}, false))
            : Boolean.getBoolean("store.only")
            ? List.of(d -> new Scenario("store-link-cut", e -> lab.cut(e.storeLink()), e -> lab.heal(e.storeLink()), true),
                    d -> new Scenario("store-link-blackhole", e -> lab.blackhole(e.storeLink()), e -> lab.heal(e.storeLink()), true))
            : List.of(
            d -> new Scenario("worker-crash", Engine::crashWorker,
                    e -> e.startWorker(d.resolve("ledger"), d, d.resolve("worker.log")), false),
            d -> new Scenario("engine-restart", e -> lab.stop(e.container()), e -> lab.start(e.container()), false),
            d -> new Scenario("engine-restart-done-during", e -> lab.stop(e.container()), e -> lab.start(e.container()), true),
            d -> new Scenario("app-link-cut", e -> lab.cut(e.appLink()), e -> lab.heal(e.appLink()), true),
            d -> new Scenario("app-link-blackhole", e -> lab.blackhole(e.appLink()), e -> lab.heal(e.appLink()), true),
            d -> new Scenario("store-link-cut", e -> lab.cut(e.storeLink()), e -> lab.heal(e.storeLink()), true),
            d -> new Scenario("store-link-blackhole", e -> lab.blackhole(e.storeLink()), e -> lab.heal(e.storeLink()), true));

    @ParameterizedTest
    @ValueSource(strings = {"Dapr", "Temporal"})
    void observe(String which) throws Exception {
        int i = 0;
        for (int round = 0; round < Integer.getInteger("rounds", 1); round++)
        for (int outage : Boolean.getBoolean("hang.only") ? new int[] {30_000} : new int[] {2_000, 10_000})
        for (var make : scenarios) {
            Path d = Files.createDirectories(dir.resolve(which + "-" + i++));
            Scenario s = make.apply(d);
            Ledger ledger = new Ledger(d.resolve("ledger"));
            try (Engine engine = "Dapr".equals(which) ? new DaprEngine(lab) : new TemporalEngine(lab)) {
                String order = which.toLowerCase() + "-" + s.name() + "-" + outage + "-r" + round;
                Path hold = Files.createFile(d.resolve("hold-charge"));
                engine.startWorker(ledger.file(), d, d.resolve("worker.log"));
                engine.startSaga(order);
                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (0 == ledger.count("charge:" + order) && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                s.breakIt().run(engine);
                if (s.releaseDuringOutage()) {
                    Thread.sleep(500);
                    Files.delete(hold);
                }
                Thread.sleep(outage - (s.releaseDuringOutage() ? 500 : 0));
                Files.deleteIfExists(hold);
                long fixed = System.nanoTime();
                s.fixIt().run(engine);
                String outcome = engine.outcome(order, Duration.ofSeconds(90));
                double recovery = (System.nanoTime() - fixed) / 1e9;
                System.out.printf("OBSERVED %-8s %-28s %5dms outcome=%-10s charges=%d recovery=%.1fs ledger=%s%n",
                        which, s.name(), outage, outcome, ledger.count("charge:" + order), recovery, ledger.all());
                if (false == "shipped".equals(outcome)) {
                    System.out.println("WORKER LOG " + which + " " + s.name() + "\n"
                            + String.join("\n", Files.readAllLines(d.resolve("worker.log")).stream().limit(60).toList()));
                }
            }
        }
    }
}
