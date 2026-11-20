package com.wolinski.chaos.shop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** The outside world: a file that survives a process crash, recording every side effect that really happened. */
public record Ledger(Path file) {

    public synchronized void record(String effect) {
        try {
            Files.writeString(file, effect + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> all() {
        try {
            return Files.exists(file) ? Files.readAllLines(file) : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long count(String effect) {
        return all().stream().filter(effect::equals).count();
    }
}
