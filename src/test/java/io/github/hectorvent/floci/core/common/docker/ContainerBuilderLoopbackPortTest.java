package io.github.hectorvent.floci.core.common.docker;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContainerBuilderLoopbackPortTest {

    private final ContainerBuilder builder = new ContainerBuilder(
            org.mockito.Mockito.mock(EmulatorConfig.class),
            org.mockito.Mockito.mock(DockerHostResolver.class),
            org.mockito.Mockito.mock(EmbeddedDnsServer.class));

    @Test
    void aLoopbackPortBindingPinsTheHostInterface() {
        ContainerSpec spec = builder.newContainer("amazon/dynamodb-local:3.3.1")
                .withLoopbackDynamicPort(8000)
                .build();

        assertEquals(ContainerBuilder.LOOPBACK_HOST_IP, spec.hostIp());
        assertTrue(spec.hasPortBindings());
        assertEquals(0, spec.portBindings().get(8000), "0 asks Docker for an ephemeral host port");
    }

    @Test
    void anOrdinaryDynamicPortKeepsDockersDefaultOfEveryInterface() {
        ContainerSpec spec = builder.newContainer("busybox:stable")
                .withDynamicPort(8000)
                .build();

        assertNull(spec.hostIp());
    }
}
