package com.wolinski.chaos.temporal;

import com.wolinski.chaos.shop.Shop;
import io.temporal.activity.Activity;

public record ShopActivitiesImpl(Shop shop) implements ShopActivities {
    @Override
    public void reserve(String order) {
        shop.reserve(order);
    }

    @Override
    public void charge(String order) {
        shop.charge(order);
    }

    @Override
    public void chargeOnce(String order, String key) {
        var context = Activity.getExecutionContext();
        shop.charge(order, key, () -> context.heartbeat(null));
    }

    @Override
    public String chargeStatus(String key) {
        return shop.chargeStatus(key);
    }

    @Override
    public void refundOnce(String order, String key) {
        shop.refund(order, key);
    }

    @Override
    public void ship(String order) {
        shop.ship(order);
    }

    @Override
    public void refund(String order) {
        shop.refund(order);
    }

    @Override
    public void release(String order) {
        shop.release(order);
    }

    @Override
    public int noop(int i) {
        return i;
    }
}
