package io.github.hectorvent.floci.services.dynamodb.container;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.EndpointInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamoDbLocalContainerManagerEndpointTest {

    private DynamoDbLocalContainerManager managerWith(boolean inContainer) {
        ContainerDetector detector = mock(ContainerDetector.class);
        when(detector.isRunningInContainer()).thenReturn(inContainer);
        return new DynamoDbLocalContainerManager(
                mock(ContainerBuilder.class),
                mock(ContainerLifecycleManager.class),
                mock(ContainerLogStreamer.class),
                detector,
                mock(EmulatorConfig.class),
                mock(RegionResolver.class));
    }

    private EndpointInfo resolve(DynamoDbLocalContainerManager manager, ContainerInfo info)
            throws Exception {
        Method m = DynamoDbLocalContainerManager.class
                .getDeclaredMethod("resolveEndpoint", ContainerInfo.class);
        m.setAccessible(true);
        return (EndpointInfo) m.invoke(manager, info);
    }

    @Test
    void nativeModeDialsTheLiteralLoopbackAddressNotLocalhost() throws Exception {
        // The published port binds 127.0.0.1. A host resolving localhost to ::1 first would
        // otherwise dial an address the container never bound.
        ContainerInfo info = new ContainerInfo(
                "cid",
                Map.of(8000, new EndpointInfo("localhost", 49153)),
                Map.of(8000, 49153));

        EndpointInfo resolved = resolve(managerWith(false), info);

        assertEquals(ContainerBuilder.LOOPBACK_HOST_IP, resolved.host());
        assertEquals(49153, resolved.port());
    }

    @Test
    void containerModeKeepsTheDockerNetworkAddress() throws Exception {
        ContainerInfo info = new ContainerInfo(
                "cid",
                Map.of(8000, new EndpointInfo("172.20.0.3", 8000)),
                Map.of());

        EndpointInfo resolved = resolve(managerWith(true), info);

        assertEquals("172.20.0.3", resolved.host());
        assertEquals(8000, resolved.port());
    }

}
