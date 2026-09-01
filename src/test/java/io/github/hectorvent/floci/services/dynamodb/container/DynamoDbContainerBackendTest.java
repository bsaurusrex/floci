package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.dynamodb.model.TableDefinition;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDbContainerBackendTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DynamoDbLocalClient client = mock(DynamoDbLocalClient.class);
    private final DynamoDbStreamPump streamPump = mock(DynamoDbStreamPump.class);

    private static final String ACCOUNT = "000000000000";

    private final RegionResolver regionResolver = mock(RegionResolver.class);

    private DynamoDbContainerBackend backendWith(String configured) {
        return backendWith(configured, ACCOUNT);
    }

    private DynamoDbContainerBackend backendWith(String configured, String accountId) {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.DynamoDbServiceConfig ddb = mock(EmulatorConfig.DynamoDbServiceConfig.class);
        lenient().when(config.services()).thenReturn(services);
        lenient().when(services.dynamodb()).thenReturn(ddb);
        lenient().when(ddb.backend()).thenReturn(configured);
        lenient().when(regionResolver.getAccountId()).thenReturn(accountId);
        return new DynamoDbContainerBackend(config, client, streamPump, MAPPER, regionResolver);
    }

    private static TableDefinition table(String name) {
        TableDefinition definition = new TableDefinition();
        definition.setTableName(name);
        return definition;
    }

    @Test
    void isEnabledOnlyForTheContainerBackend() {
        assertTrue(backendWith("container").isEnabled());
        assertTrue(backendWith("CONTAINER").isEnabled(), "the backend name is case-insensitive");
        assertFalse(backendWith("native").isEnabled());
    }

    @Test
    void controlPlaneActionsAreNotForwarded() {
        DynamoDbContainerBackend backend = backendWith("container");

        // These are exactly the actions DynamoDB local cannot serve, so Floci must keep them.
        for (String action : new String[]{
                "CreateTable", "DescribeTable", "ListTables", "TagResource", "ListTagsOfResource",
                "UpdateContinuousBackups", "ExportTableToPointInTime",
                "EnableKinesisStreamingDestination"}) {
            assertNull(backend.forwardIfDataPlane(action, MAPPER.createObjectNode(), "us-east-1"),
                    action + " must stay in Floci");
        }
        verify(client, never()).call(any(), any(), any());
    }

    @Test
    void dataPlaneActionsAreForwardedVerbatimIncludingErrors() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.validate#ValidationException");
        error.put("message", "The provided expression refers to an attribute that does not exist");
        when(client.call(eq("UpdateItem"), any(), eq("us-east-1")))
                .thenReturn(new DynamoDbLocalClient.Result(400, error));

        Response response = backend.forwardIfDataPlane("UpdateItem", MAPPER.createObjectNode(), "us-east-1");

        // The container's status and body are the point of routing here: they must not be rewritten.
        assertEquals(400, response.getStatus());
        assertEquals(error, response.getEntity());
    }

    @Test
    void createTableMirrorDropsMembersDynamoDbLocalRejects() {
        DynamoDbContainerBackend backend = backendWith("container");
        when(client.call(eq("CreateTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");
        request.putArray("Tags").addObject().put("Key", "env").put("Value", "dev");
        request.putObject("SSESpecification").put("Enabled", true);
        request.put("TableClass", "STANDARD_INFREQUENT_ACCESS");
        request.put("DeletionProtectionEnabled", true);

        backend.mirrorControlPlane("CreateTable", request, "us-east-1", table("Users"));

        ArgumentCaptor<JsonNode> mirrored = ArgumentCaptor.forClass(JsonNode.class);
        verify(client).call(eq("CreateTable"), mirrored.capture(), eq("us-east-1"));
        JsonNode sent = mirrored.getValue();
        assertEquals("Users", sent.path("TableName").asText());
        assertFalse(sent.has("Tags"), "Floci serves tags; DynamoDB local rejects them");
        assertFalse(sent.has("SSESpecification"));
        assertFalse(sent.has("TableClass"));
        assertFalse(sent.has("DeletionProtectionEnabled"));
        // The caller's request must not be mutated. Floci still answers DescribeTable from it.
        assertTrue(request.has("Tags"));
    }

    @Test
    void mirroredStreamAlwaysCarriesBothImagesAndRegistersThePump() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode created = MAPPER.createObjectNode();
        created.putObject("TableDescription").put("LatestStreamArn", "arn:stream/1");
        when(client.call(eq("CreateTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, created));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");
        request.putObject("StreamSpecification")
                .put("StreamEnabled", true)
                .put("StreamViewType", "KEYS_ONLY");

        TableDefinition definition = table("Users");
        backend.mirrorControlPlane("CreateTable", request, "us-east-1", definition);

        ArgumentCaptor<JsonNode> mirrored = ArgumentCaptor.forClass(JsonNode.class);
        verify(client).call(eq("CreateTable"), mirrored.capture(), eq("us-east-1"));
        // KEYS_ONLY would starve the pump of the images Floci needs to re-emit; it widens the
        // mirror and narrows again in DynamoDbStreamService.captureEvent.
        assertEquals("NEW_AND_OLD_IMAGES",
                mirrored.getValue().path("StreamSpecification").path("StreamViewType").asText());
        verify(streamPump).register(ACCOUNT, definition, "us-east-1", "arn:stream/1");
        assertTrue(backend.isMirrored("us-east-1", "Users"));
    }

    @Test
    void deleteTableDeregistersThePumpEvenWhenTheContainerHasNoSuchTable() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode notFound = MAPPER.createObjectNode();
        notFound.put("__type", "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(400, notFound));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        backend.mirrorControlPlane("DeleteTable", request, "us-east-1", null);

        verify(streamPump).deregister(ACCOUNT, "us-east-1", "Users");
    }

    @Test
    void aRejectedCreateTableMirrorSurfacesRatherThanLeavingASilentlyBrokenTable() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.validate#ValidationException");
        error.put("message", "Invalid table/index name.");
        when(client.call(eq("CreateTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(400, error));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        IllegalStateException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class,
                () -> backend.mirrorControlPlane("CreateTable", request, "us-east-1", table("Users")));
        assertTrue(thrown.getMessage().contains("ValidationException"));
    }

    @Test
    void aFailedDeleteIsFatalSoDeletedItemsCannotStayReadable() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        error.put("message", "boom");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        // Floci has already dropped its own copy, so a surviving container table would keep the
        // deleted items reachable through forwarded reads. Failing loudly beats that.
        IllegalStateException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", request, "us-east-1", null));
        assertTrue(thrown.getMessage().contains("would stay readable"), thrown.getMessage());
    }

    @Test
    void createDropsAStaleContainerTableLeftByAFailedDelete() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode inUse = MAPPER.createObjectNode();
        inUse.put("__type", "com.amazonaws.dynamodb.v20120810#ResourceInUseException");
        when(client.call(eq("CreateTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(400, inUse))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        backend.mirrorControlPlane("CreateTable", request, "us-east-1", table("Users"));

        // Dropped the previous generation, then created the new one, so no items survive across it.
        verify(client).call(eq(ACCOUNT), eq("DeleteTable"), any(), eq("us-east-1"));
        verify(client, times(2)).call(eq("CreateTable"), any(), eq("us-east-1"));
    }


    @Test
    void afterAFailedDropForwardedReadsAreRefusedRatherThanServingTheOldGeneration() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        // Floci's metadata says the table is gone. A forwarded read must agree, not answer from
        // the container copy the drop failed to remove.
        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        AwsException thrown = org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("GetItem", read, "us-east-1"));
        assertEquals("ResourceNotFoundException", thrown.getErrorCode());
        verify(client, never()).call(eq("GetItem"), any(), any());
    }

    @Test
    void anOrphanedTableIsForwardedAgainOnceTheDropFinallySucceeds() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));
        when(client.call(eq("GetItem"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        // The retry succeeds, so the table is genuinely gone and the container can answer.
        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        assertEquals(200, backend.forwardIfDataPlane("GetItem", read, "us-east-1").getStatus());
    }

    @Test
    void orphanCheckSeesTablesNamedByBatchAndTransactRequests() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        ObjectNode batch = MAPPER.createObjectNode();
        batch.putObject("RequestItems").putObject("Users");
        org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("BatchGetItem", batch, "us-east-1"));

        ObjectNode transact = MAPPER.createObjectNode();
        transact.putArray("TransactItems").addObject().putObject("Put").put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("TransactWriteItems", transact, "us-east-1"));
    }


    @Test
    void partiqlIsRefusedWhileAnyTableIsOrphaned() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        // The table name lives in the statement text, so per-table matching cannot see it.
        ObjectNode statement = MAPPER.createObjectNode();
        statement.put("Statement", "SELECT * FROM \"Users\" WHERE pk = 'a'");
        AwsException thrown = org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("ExecuteStatement", statement, "us-east-1"));
        assertEquals("ResourceNotFoundException", thrown.getErrorCode());
        verify(client, never()).call(eq("ExecuteStatement"), any(), any());
    }

    @Test
    void partiqlFlowsAgainOnceTheOutstandingDropSucceeds() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));
        when(client.call(eq("ExecuteStatement"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        ObjectNode statement = MAPPER.createObjectNode();
        statement.put("Statement", "SELECT * FROM \"Other\"");
        assertEquals(200,
                backend.forwardIfDataPlane("ExecuteStatement", statement, "us-east-1").getStatus());
    }

    @Test
    void anOrphanInAnotherRegionDoesNotBlockPartiql() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));
        when(client.call(eq("ExecuteStatement"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        ObjectNode statement = MAPPER.createObjectNode();
        statement.put("Statement", "SELECT * FROM \"Users\"");
        assertEquals(200,
                backend.forwardIfDataPlane("ExecuteStatement", statement, "eu-west-1").getStatus());
    }


    @Test
    void anOrphanFromOneAccountNeitherBlocksNorDropsAnotherAccountsTable() {
        // Account A's drop fails, leaving an orphan.
        DynamoDbContainerBackend accountA = backendWith("container", "111111111111");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq("111111111111"), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> accountA.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        // The retry must never be issued under another account: that would drop a live table.
        verify(client, never()).call(eq("222222222222"), eq("DeleteTable"), any(), any());
    }

    @Test
    void theOrphanRetryIsIssuedUnderTheAccountThatOwnsTheTable() {
        DynamoDbContainerBackend backend = backendWith("container", "111111111111");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq("111111111111"), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));
        when(client.call(eq("GetItem"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        assertEquals(200, backend.forwardIfDataPlane("GetItem", read, "us-east-1").getStatus());

        // Both the original drop and the retry carried the owning account.
        verify(client, times(2)).call(eq("111111111111"), eq("DeleteTable"), any(), eq("us-east-1"));
    }


    @Test
    void aReadArrivingWhileTheDropIsInFlightIsRefusedNotForwarded() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");

        // The stub answers the container's DeleteTable by re-entering the backend, standing in for
        // a read that arrives after Floci dropped its metadata but before the drop comes back.
        // Marking only on failure would leave orphanedTables empty at this point and forward it.
        AwsException[] seen = new AwsException[1];
        boolean[] reentered = {false};
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any())).thenAnswer(invocation -> {
            // Guard set before the nested call: the read retries the drop, which re-enters here.
            if (!reentered[0]) {
                reentered[0] = true;
                seen[0] = org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                        () -> backend.forwardIfDataPlane("GetItem", read, "us-east-1"));
            }
            return new DynamoDbLocalClient.Result(500, error);
        });

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        assertEquals("ResourceNotFoundException", seen[0].getErrorCode());
        verify(client, never()).call(eq("GetItem"), any(), any());
    }

    @Test
    void theMarkerIsClearedOnceTheDropSucceeds() {
        DynamoDbContainerBackend backend = backendWith("container");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));
        when(client.call(eq("GetItem"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null);

        // The table is genuinely gone, so the container may answer for itself again.
        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        assertEquals(200, backend.forwardIfDataPlane("GetItem", read, "us-east-1").getStatus());
    }


    @Test
    void beginDeleteRefusesForwardsBeforeFlociHasDroppedItsOwnCopy() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        assertNotNull(backend.beginDelete(request, "us-east-1"));

        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, MAPPER.createObjectNode()));
        AwsException thrown = org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("GetItem", read, "us-east-1"));
        assertEquals("ResourceNotFoundException", thrown.getErrorCode());
        verify(client, never()).call(eq("GetItem"), any(), any());
    }

    @Test
    void abandonDeleteRestoresForwardingWhenFlociRejectsTheDelete() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");
        when(client.call(eq("GetItem"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        String marker = backend.beginDelete(request, "us-east-1");
        assertNotNull(marker);
        // Floci refused the delete, for instance deletion protection, so the table still exists.
        backend.abandonDelete(request, "us-east-1", marker);

        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        assertEquals(200, backend.forwardIfDataPlane("GetItem", read, "us-east-1").getStatus());
    }

    @Test
    void beginDeleteDoesNotClaimAMarkerLeftByAnEarlierFailedDrop() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        // A second DeleteTable that Floci rejects must not clear the genuine orphan, so its
        // abandon is never invoked: beginDelete reports it did not install the marker.
        assertNull(backend.beginDelete(deleteRequest, "us-east-1"));
    }


    @Test
    void anUnwindDoesNotWithdrawAnOrphanConfirmedByAConcurrentDelete() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        // Request A marks first, then loses the race to delete Floci's copy.
        String markerA = backend.beginDelete(request, "us-east-1");
        assertNotNull(markerA);

        // Request B wins, deletes Floci's copy, and its container drop fails: a genuine orphan.
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", request, "us-east-1", null));

        // A now unwinds. It must not take B's orphan with it.
        backend.abandonDelete(request, "us-east-1", markerA);

        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");
        AwsException thrown = org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("GetItem", read, "us-east-1"));
        assertEquals("ResourceNotFoundException", thrown.getErrorCode());
        verify(client, never()).call(eq("GetItem"), any(), any());
    }

}
