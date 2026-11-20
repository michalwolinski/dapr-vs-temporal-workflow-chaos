package com.wolinski.chaos;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Both engines on one Docker network, with Toxiproxy on every link the tests break. Nothing outside the network talks
 * to an engine directly: the test JVM and the worker JVMs go through a proxy, so restarting a container never moves a
 * port, and the same proxy can cut, blackhole or slow the link.
 *
 * <pre>
 *   worker JVM ──dapr-app──▶ daprd ──dapr-store──▶ redis        daprd ──▶ placement, scheduler
 *   worker JVM ──temporal-app──▶ temporal ──temporal-store──▶ postgres
 *   test JVM ──dapr-client / temporal-client──▶ the same engines, on links the tests leave alone
 * </pre>
 */
final class Lab implements AutoCloseable {
    private static final String DAPR = "1.19.1";
    static final DockerImageName DAPRD = image("daprio/daprd:" + DAPR + "@sha256:bff5d1af45241bb1fe8f257ff3d1acfa9b26723430d8ef443b34085029a9dd59");
    static final DockerImageName PLACEMENT = image("daprio/placement:" + DAPR + "@sha256:72ea011b71c86ac76d4755fa9903cd12f9214c142dc7ba437e2dff5734e0fc5d");
    static final DockerImageName SCHEDULER = image("daprio/scheduler:" + DAPR + "@sha256:e4792df46d313cd04c64991825d476e34c179a2692921b6fdf0643375b615839");
    static final DockerImageName REDIS = image("redis:8.10.2@sha256:97ec88159e6d7cfb5ddef9015386087d58a912de80112f06ebdfa78fa5989901");
    static final DockerImageName TEMPORAL = image("temporalio/server:1.32.2@sha256:7bf69188a6232fea14869b9cef3d94aa82a1601d99517f7e86c71f22da4e0670");
    static final DockerImageName TEMPORAL_TOOLS = image("temporalio/admin-tools:1.32.2@sha256:4a95866efba29d7e17ab012df5232795e8b0ad0f7d5f9d1ec4df321f7792a754");
    static final DockerImageName TEMPORAL_DEV = image("temporalio/temporal:1.10.1@sha256:b228dadf9cdb6c7c02a1394b89dd76f827aac9760e458fce42f0899a10141225");
    static final DockerImageName POSTGRES = image("postgres:18.7@sha256:e754b839f447639b7004895f4d8a4bfcb4e9dbe3a1bc035f27f646562a16fd61");
    static final DockerImageName TOXIPROXY = image("ghcr.io/shopify/toxiproxy:2.13.0@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa");

    private static final String STATE_STORE = """
            apiVersion: dapr.io/v1alpha1
            kind: Component
            metadata:
              name: workflowstore
            spec:
              type: state.redis
              version: v1
              metadata:
              - name: redisHost
                value: toxiproxy:6379
              - name: actorStateStore
                value: "true"
            """;

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY)
            .withNetwork(network).withNetworkAliases("toxiproxy")
            .withExposedPorts(8474, 50001, 50011, 7233, 7243)
            .waitingFor(Wait.forHttp("/version").forPort(8474));
    final GenericContainer<?> redis = new GenericContainer<>(REDIS)
            .withNetwork(network).withNetworkAliases("redis");
    private final GenericContainer<?> placement = new GenericContainer<>(PLACEMENT)
            .withNetwork(network).withNetworkAliases("placement")
            .withCommand("./placement", "--port", "50006");
    private final GenericContainer<?> scheduler = new GenericContainer<>(SCHEDULER)
            .withNetwork(network).withNetworkAliases("scheduler")
            .withCommand("./scheduler", "--port", "50007", "--etcd-data-dir", "/tmp/etcd");
    final GenericContainer<?> daprd = new GenericContainer<>(DAPRD)
            .withNetwork(network).withNetworkAliases("daprd")
            .withCopyToContainer(Transferable.of(STATE_STORE), "/components/workflowstore.yaml")
            .withCommand("./daprd", "--app-id", "orders",
                    "--placement-host-address", "placement:50006",
                    "--scheduler-host-address", "scheduler:50007",
                    "--resources-path", "/components",
                    "--dapr-grpc-port", "50001", "--log-level", "info")
            .waitingFor(Wait.forLogMessage(".*dapr initialized.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
    /** -Dtemporal=dev swaps the real server for the CLI's development server, SQLite in one process. */
    private static final boolean DEV = "dev".equals(System.getProperty("temporal"));
    private static final String SQL_TOOL = "temporal-sql-tool --plugin postgres12 --ep postgres -p 5432 -u temporal --pw temporal";
    private static final String SCHEMA = String.join(" && ",
            SQL_TOOL + " --db temporal create-database",
            SQL_TOOL + " --db temporal setup-schema -v 0.0",
            SQL_TOOL + " --db temporal update-schema -d /etc/temporal/schema/postgresql/v12/temporal/versioned",
            SQL_TOOL + " --db temporal_visibility create-database",
            SQL_TOOL + " --db temporal_visibility setup-schema -v 0.0",
            SQL_TOOL + " --db temporal_visibility update-schema -d /etc/temporal/schema/postgresql/v12/visibility/versioned");
    private static final String NAMESPACE = "until temporal operator namespace describe default --address temporal:7233;"
            + " do temporal operator namespace create default --address temporal:7233 --retention 24h || sleep 1; done";

    final GenericContainer<?> postgres = new GenericContainer<>(POSTGRES)
            .withNetwork(network).withNetworkAliases("postgres")
            .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements", "-c", "pg_stat_statements.track=all")
            .withEnv("POSTGRES_USER", "temporal").withEnv("POSTGRES_PASSWORD", "temporal")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2));
    final GenericContainer<?> temporal = DEV
            ? new GenericContainer<>(TEMPORAL_DEV)
                    .withNetwork(network).withNetworkAliases("temporal")
                    .withCommand("server", "start-dev", "--ip", "0.0.0.0", "--headless", "--db-filename", "/tmp/temporal.db")
                    .waitingFor(Wait.forLogMessage(".*Temporal Server:.*", 1).withStartupTimeout(Duration.ofMinutes(2)))
            : new GenericContainer<>(TEMPORAL)
                    .withNetwork(network).withNetworkAliases("temporal")
                    .withCopyToContainer(Transferable.of(resource("/temporal/server.yaml")), "/etc/temporal/server.yaml")
                    .withEnv("TEMPORAL_SERVER_CONFIG_FILE_PATH", "/etc/temporal/server.yaml")
                    .waitingFor(Wait.forLogMessage(".*", 1));

    private final HttpClient http = HttpClient.newHttpClient();

    Lab start() {
        toxiproxy.start();
        proxy("dapr-store", 6379, "redis:6379");
        proxy("dapr-app", 50001, "daprd:50001");
        proxy("dapr-client", 50011, "daprd:50001");
        proxy("temporal-app", 7233, "temporal:7233");
        proxy("temporal-client", 7243, "temporal:7233");
        proxy("temporal-store", 5432, "postgres:5432");
        redis.start();
        placement.start();
        scheduler.start();
        daprd.start();
        if (DEV) {
            temporal.start();
        } else {
            postgres.start();
            tools(SCHEMA);
            temporal.start();
            tools(NAMESPACE);
        }
        return this;
    }

    /** host:port the worker JVMs use to reach the Dapr sidecar. */
    String daprApp() {
        return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(50001);
    }

    String daprClient() {
        return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(50011);
    }

    String temporalApp() {
        return toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(7233);
    }

    String temporalClient() {
        return toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(7243);
    }

    /** Drop every open connection on the link and refuse new ones: the peer sees a reset at once. */
    void cut(String link) {
        post("/proxies/" + link, "{\"enabled\": false}");
    }

    void heal(String link) {
        post("/proxies/" + link, "{\"enabled\": true}");
        for (String toxic : new String[] {"blackhole-up", "blackhole-down", "latency-up", "latency-down"}) {
            send(HttpRequest.newBuilder(api("/proxies/" + link + "/toxics/" + toxic)).DELETE().build());
        }
    }

    /** Every link back to normal, so that one failed test cannot leave the next one cut off. */
    void healAll() {
        for (String link : new String[] {"dapr-store", "dapr-app", "dapr-client", "temporal-app", "temporal-client", "temporal-store"}) {
            heal(link);
        }
    }

    /** Keep connections open but deliver nothing in either direction: what a dead switch looks like to TCP. */
    void blackhole(String link) {
        toxic(link, "blackhole-up", "timeout", "upstream", "{\"timeout\": 0}");
        toxic(link, "blackhole-down", "timeout", "downstream", "{\"timeout\": 0}");
    }

    /** Add the same delay to every chunk of data in each direction. */
    void latency(String link, int millis) {
        toxic(link, "latency-up", "latency", "upstream", "{\"latency\": " + millis + "}");
        toxic(link, "latency-down", "latency", "downstream", "{\"latency\": " + millis + "}");
    }

    void stop(GenericContainer<?> container) {
        container.getDockerClient().stopContainerCmd(container.getContainerId()).withTimeout(0).exec();
    }

    void start(GenericContainer<?> container) {
        container.getDockerClient().startContainerCmd(container.getContainerId()).exec();
    }

    /** Runs a command inside a container and returns what it printed; fails if it exits non-zero. */
    String exec(GenericContainer<?> container, String... command) {
        try {
            var result = container.execInContainer(command);
            if (0 != result.getExitCode()) {
                throw new IllegalStateException(String.join(" ", command) + " -> " + result.getStderr());
            }
            return result.getStdout();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    String logs(GenericContainer<?> container) {
        return container.getLogs();
    }

    private void tools(String script) {
        try (var tools = new GenericContainer<>(TEMPORAL_TOOLS)
                .withNetwork(network)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sh"))
                .withCommand("-c", script)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(2)))) {
            tools.start();
        } catch (RuntimeException failed) {
            throw new IllegalStateException("admin-tools failed: " + script, failed);
        }
    }

    private static String resource(String path) {
        try (var in = Lab.class.getResourceAsStream(path)) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private void proxy(String name, int listen, String upstream) {
        post("/proxies", "{\"name\": \"%s\", \"listen\": \"0.0.0.0:%d\", \"upstream\": \"%s\", \"enabled\": true}"
                .formatted(name, listen, upstream));
    }

    private void toxic(String link, String name, String type, String stream, String attributes) {
        post("/proxies/" + link + "/toxics", "{\"name\": \"%s\", \"type\": \"%s\", \"stream\": \"%s\", \"attributes\": %s}"
                .formatted(name, type, stream, attributes));
    }

    private void post(String path, String json) {
        HttpResponse<String> response = send(HttpRequest.newBuilder(api(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json)).build());
        if (300 <= response.statusCode()) {
            throw new IllegalStateException(path + " -> " + response.statusCode() + " " + response.body());
        }
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private URI api(String path) {
        return URI.create("http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8474) + path);
    }

    private static DockerImageName image(String name) {
        return DockerImageName.parse(name);
    }

    @Override
    public void close() {
        temporal.stop();
        postgres.stop();
        daprd.stop();
        scheduler.stop();
        placement.stop();
        redis.stop();
        toxiproxy.stop();
        network.close();
    }
}
