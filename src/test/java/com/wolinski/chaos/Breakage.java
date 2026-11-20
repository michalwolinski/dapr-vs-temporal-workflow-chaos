package com.wolinski.chaos;

import com.wolinski.chaos.shop.Ledger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * One saga, one thing broken while "charge" is running. The charge has already written to the ledger and is held there
 * by a hold file, so whatever breaks, breaks after the money moved and before the engine heard about it.
 */
final class Breakage {
    interface Step {
        void run(Engine engine) throws Exception;
    }

    record Result(String outcome, long charges, long refunds, double recoverySeconds) {}

    private Breakage() {}

    /**
     * @param finishDuringOutage let the charge finish halfway through the outage, so its result exists but cannot be
     *                           delivered; otherwise it finishes when the outage ends
     */
    static Result run(Engine engine, Path dir, String order, Duration outage, Step breakIt, Step fixIt,
            boolean finishDuringOutage) throws Exception {
        return run(engine, dir, order, outage, breakIt, fixIt, finishDuringOutage, false);
    }

    static Result run(Engine engine, Path dir, String order, Duration outage, Step breakIt, Step fixIt,
            boolean finishDuringOutage, boolean safe) throws Exception {
        Ledger ledger = new Ledger(dir.resolve("ledger"));
        Path hold = Files.createFile(dir.resolve("hold-charge"));
        engine.startWorker(ledger.file(), dir, dir.resolve("worker.log"));
        if (safe) {
            engine.startSafeSaga(order);
        } else {
            engine.startSaga(order);
        }
        awaitCharge(ledger, order);

        long fixed;
        breakIt.run(engine);
        try {
            if (finishDuringOutage) {
                Thread.sleep(outage.toMillis() / 2);
                Files.delete(hold);
                Thread.sleep(outage.toMillis() - outage.toMillis() / 2);
            } else {
                Thread.sleep(outage.toMillis());
                Files.delete(hold);
            }
        } finally {
            Files.deleteIfExists(hold);
            fixed = System.nanoTime();
            fixIt.run(engine);
        }
        String outcome = engine.outcome(order, Duration.ofSeconds(90));
        Result result = new Result(outcome, ledger.count("charge:" + order), ledger.count("refund:" + order),
                (System.nanoTime() - fixed) / 1e9);
        System.out.printf("RESULT %-8s %-36s %s%n", engine.name(), order, result);
        return result;
    }

    static void awaitCharge(Ledger ledger, String order) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        // "charge:" for a provider that settles at once, "charge-accepted:" for one that settles later.
        while (ledger.all().stream().noneMatch(line -> line.startsWith("charge") && line.endsWith(order))) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("charge for " + order + " never started");
            }
            Thread.sleep(20);
        }
    }
}
