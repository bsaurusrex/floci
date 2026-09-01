package io.github.hectorvent.floci.services.dynamodb.container;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.EndpointInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * Owns the single {@code amazon/dynamodb-local} container that backs the DynamoDB data plane
 * when {@code floci.services.dynamodb.backend=container}.
 *
 * <p>One container serves every account and region. DynamoDB local keeps a separate store per
 * (access key id, region) pair unless {@code -sharedDb} is given, so {@code -sharedDb} is
 * deliberately omitted: {@link DynamoDbLocalClient} sends the caller's account id as the access
 * key, which reproduces Floci's own account plus region scoping inside the container.
 */
@ApplicationScoped
public class DynamoDbLocalContainerManager {

    private static final Logger LOG = Logger.getLogger(DynamoDbLocalContainerManager.class);

    static final int DYNAMODB_LOCAL_PORT = 8000;

    private static final long PROBE_CONNECT_MS = 1000;
    private static final long PROBE_RETRY_MS = 250;

    private final ContainerBuilder containerBuilder;
    private final ContainerLifecycleManager lifecycleManager;
    private final ContainerLogStreamer logStreamer;
    private final ContainerDetector containerDetector;
    private final EmulatorConfig config;
    private final RegionResolver regionResolver;

    private final Object startLock = new Object();
    private volatile String containerId;
    private volatile EndpointInfo endpoint;
    private Closeable logStream;

    @Inject
    public DynamoDbLocalContainerManager(ContainerBuilder containerBuilder,
                                         ContainerLifecycleManager lifecycleManager,
                                         ContainerLogStreamer logStreamer,
                                         ContainerDetector containerDetector,
                                         EmulatorConfig config,
                                         RegionResolver regionResolver) {
        this.containerBuilder = containerBuilder;
        this.lifecycleManager = lifecycleManager;
        this.logStreamer = logStreamer;
        this.containerDetector = containerDetector;
        this.config = config;
        this.regionResolver = regionResolver;
    }

    /**
     * Returns the base URL of the backing container, starting it on first use.
     */
    public String baseUrl() {
        EndpointInfo current = endpoint;
        if (current == null) {
            synchronized (startLock) {
                current = endpoint;
                if (current == null) {
                    current = start();
                    endpoint = current;
                }
            }
        }
        return "http://" + current.host() + ":" + current.port();
    }

    private EndpointInfo start() {
        EmulatorConfig.DynamoDbServiceConfig ddb = config.services().dynamodb();
        String image = ddb.containerImage();
        String containerName = ContainerStorageHelper.dockerName(config, "floci-dynamodb-local");

        LOG.infov("Starting DynamoDB local container {0} from image {1}", containerName, image);
        lifecycleManager.removeIfExists(containerName);

        ContainerBuilder.Builder specBuilder = containerBuilder.newContainer(image)
                .withName(containerName)
                .withDockerNetwork(ddb.containerDockerNetwork())
                .withLogRotation()
                // -sharedDb is deliberately omitted; see the class javadoc.
                .withCmd(List.of("-jar", "DynamoDBLocal.jar", "-inMemory"))
                .withLabels(ContainerStorageHelper.resourceIdentityLabels(
                        "dynamodb", "local", regionResolver.getAccountId(),
                        regionResolver.getDefaultRegion()));

        if (containerDetector.isRunningInContainer()) {
            specBuilder.withExposedPort(DYNAMODB_LOCAL_PORT);
        } else {
            // Loopback only. The backing container performs no signature validation and picks its
            // store straight from the credential scope, so anything that can reach the published
            // port can read or write any account's tables without going through Floci at all.
            // Nothing but Floci is meant to dial it.
            specBuilder.withLoopbackDynamicPort(DYNAMODB_LOCAL_PORT);
        }

        ContainerSpec spec = specBuilder.build();
        ContainerInfo info = lifecycleManager.createAndStart(spec);
        EndpointInfo resolved = resolveEndpoint(info);
        containerId = info.containerId();

        String shortId = info.containerId().length() >= 8
                ? info.containerId().substring(0, 8)
                : info.containerId();
        logStream = logStreamer.attach(
                info.containerId(), "/aws/dynamodb/local", logStreamer.generateLogStreamName(shortId),
                regionResolver.getDefaultRegion(), "dynamodb-local");

        waitForReady(resolved, ddb.containerStartupTimeoutSeconds());
        LOG.infov("DynamoDB local container ready on {0}", resolved);
        return resolved;
    }

    /**
     * Resolves the address Floci dials the backing container on.
     *
     * <p>In native mode the published port is bound to {@code 127.0.0.1}, so the literal address
     * replaces the shared resolver's {@code localhost}. A host that resolves {@code localhost} to
     * {@code ::1} first would otherwise dial an address the container never bound, and startup
     * would time out with the data plane unavailable. Only the host is swapped: the port comes
     * from the resolver, which reads the binding Docker actually made.
     */
    private EndpointInfo resolveEndpoint(ContainerInfo info) {
        EndpointInfo resolved = info.getEndpoint(DYNAMODB_LOCAL_PORT);
        if (resolved == null) {
            throw new IllegalStateException(
                    "DynamoDB local exposed no endpoint on port " + DYNAMODB_LOCAL_PORT);
        }
        if (containerDetector.isRunningInContainer()) {
            return resolved;
        }
        return new EndpointInfo(ContainerBuilder.LOOPBACK_HOST_IP, resolved.port());
    }

    private static void waitForReady(EndpointInfo target, int timeoutSeconds) {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(target.host(), target.port()), (int) PROBE_CONNECT_MS);
                return;
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debugv("DynamoDB local probe attempt {0}: {1}", attempt, e.getMessage());
                }
            }
            try {
                Thread.sleep(PROBE_RETRY_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for DynamoDB local", ie);
            }
        }
        throw new IllegalStateException("DynamoDB local did not become ready on " + target
                + " within " + timeoutSeconds + "s");
    }

    /**
     * Stops the backing container. Safe to call when it was never started.
     */
    public void stopAll() {
        synchronized (startLock) {
            if (containerId == null) {
                return;
            }
            LOG.info("Stopping DynamoDB local container");
            lifecycleManager.stopAndRemove(containerId, logStream);
            containerId = null;
            endpoint = null;
            logStream = null;
        }
    }
}
