package com.wolinski.chaos;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Starts a class in a JVM of its own, so that a crash takes the whole process with it. */
final class Jvm {
    private Jvm() {}

    static Process start(Path log, Class<?> main, String... args) throws IOException {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(classpath);
        command.add(main.getName());
        command.addAll(List.of(args));
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                .start();
    }

    /** SIGTERM, what a deploy sends: shutdown hooks run. Does not wait for the process to exit. */
    static void terminate(Process process) {
        process.destroy();
    }

    /** kill -9: no shutdown hook, no goodbye to the engine. */
    static void crash(Process process) throws InterruptedException {
        process.destroyForcibly();
        process.waitFor();
    }
}
