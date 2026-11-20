package com.wolinski.chaos.temporal;

import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface ShopActivities {
    void reserve(String order);

    void charge(String order);

    /** Charges with an idempotency key, heartbeating while it waits for the provider. */
    void chargeOnce(String order, String key);

    void refundOnce(String order, String key);

    /** CHARGED, PENDING or NONE: what the provider knows about the key. */
    String chargeStatus(String key);

    void ship(String order);

    void refund(String order);

    void release(String order);

    int noop(int i);
}
