package com.wolinski.chaos.shop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The three services the saga talks to, reduced to what the tests need: every call writes its side effect to the
 * ledger first, and then, if the test left a hold file for that step, waits until the file is gone. That is how a
 * test breaks something at the worst moment: after the money moved, before the engine heard about it.
 *
 * <p>A test can also leave a client-timeout file holding a number of milliseconds: the caller then stops waiting after
 * that long and throws, while the provider has already done the work. That is a payment client with a timeout talking
 * to a slow provider.
 *
 * <p>A provider-down file makes every call to the keyed payment API fail at once, the way a provider outage does.
 *
 * <p>And a late-charge file makes the keyed charge settle only when its hold is released, on the provider's side,
 * whether or not anyone is still waiting for it: a provider that accepted the request and processes it later.
 */
public record Shop(Ledger ledger, Path holds) {
    public static final String NO_STOCK = "no-stock-";
    private static final Object PROVIDER = new Object();

    public void reserve(String order) {
        effect("reserve:" + order, "reserve", () -> {});
    }

    /** A charge with no idempotency key: every call moves money. */
    public void charge(String order) {
        effect("charge:" + order, "charge", () -> {});
    }

    /**
     * A charge with an idempotency key, the way real payment providers offer it: the first call with a key moves money,
     * every later call with the same key returns the first result and moves nothing.
     */
    public void charge(String order, String key, Runnable whileWaiting) {
        requireProvider();
        boolean late = Files.exists(holds.resolve("late-charge"));
        synchronized (PROVIDER) {
            if (0 < ledger.count("charge-accepted:" + key)) {
                ledger.record("charge-repeated:" + order);
            } else {
                ledger.record("charge-accepted:" + key);
                if (late) {
                    Thread.ofPlatform().start(() -> {
                        // The provider's side: it waits for the hold, not for the caller's timeout.
                        Path hold = holds.resolve("hold-charge");
                        while (Files.exists(hold)) {
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                        settle(order, key);
                    });
                } else {
                    settle(order, key);
                }
            }
        }
        await("charge", whileWaiting);
    }

    /** What the provider knows about a key: CHARGED, PENDING (accepted, not yet processed) or NONE. */
    public String chargeStatus(String key) {
        requireProvider();
        synchronized (PROVIDER) {
            if (0 < ledger.count("charge-key:" + key)) {
                return "CHARGED";
            }
            return 0 < ledger.count("charge-accepted:" + key) ? "PENDING" : "NONE";
        }
    }

    private void requireProvider() {
        if (Files.exists(holds.resolve("provider-down"))) {
            throw new IllegalStateException("payment provider unavailable");
        }
    }

    private void settle(String order, String key) {
        synchronized (PROVIDER) {
            ledger.record("charge-key:" + key);
            ledger.record("charge:" + order);
        }
    }

    /** Refunds the charge made with this key, once; with no such charge it does nothing. */
    public void refund(String order, String key) {
        requireProvider();
        synchronized (PROVIDER) {
            if (0 < ledger.count("charge-key:" + key) && 0 == ledger.count("refund-key:" + key)) {
                ledger.record("refund-key:" + key);
                ledger.record("refund:" + order);
            }
        }
    }

    public void ship(String order) {
        if (order.startsWith(NO_STOCK)) {
            ledger.record("ship-refused:" + order);
            throw new IllegalStateException("carrier refused " + order);
        }
        effect("ship:" + order, "ship", () -> {});
    }

    public void refund(String order) {
        effect("refund:" + order, "refund", () -> {});
    }

    public void release(String order) {
        effect("release:" + order, "release", () -> {});
    }

    private void effect(String effect, String step, Runnable whileWaiting) {
        ledger.record(effect);
        await(step, whileWaiting);
    }

    private void await(String step, Runnable whileWaiting) {
        Path hold = holds.resolve("hold-" + step);
        Path clientTimeout = holds.resolve("client-timeout-" + step);
        long started = System.nanoTime();
        while (Files.exists(hold)) {
            if (Files.exists(clientTimeout) && System.nanoTime() - started > millis(clientTimeout) * 1_000_000) {
                throw new IllegalStateException("payment client gave up waiting for " + step);
            }
            whileWaiting.run();
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static long millis(Path file) {
        try {
            return Long.parseLong(Files.readString(file).trim());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
