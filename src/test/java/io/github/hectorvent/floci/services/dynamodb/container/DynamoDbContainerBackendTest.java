package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbService;
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
    private final DynamoDbService dynamoDbService = mock(DynamoDbService.class);

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
        return new DynamoDbContainerBackend(
                config, client, streamPump, MAPPER, regionResolver, dynamoDbService);
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
    void theOutstandingDropRetryIsIssuedUnderTheAccountThatOwnsTheTable() {
        DynamoDbContainerBackend backend = backendWith("container", "111111111111");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq("111111111111"), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, error))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));
        when(client.call(eq("ExecuteStatement"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode deleteRequest = MAPPER.createObjectNode();
        deleteRequest.put("TableName", "Users");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("DeleteTable", deleteRequest, "us-east-1", null));

        // PartiQL is the path that retries the outstanding drop, and the retry must carry the
        // account that owns the table rather than whoever happens to be calling.
        ObjectNode statement = MAPPER.createObjectNode();
        statement.put("Statement", "SELECT * FROM \"Users\"");
        assertEquals(200,
                backend.forwardIfDataPlane("ExecuteStatement", statement, "us-east-1").getStatus());

        verify(client, times(2)).call(eq("111111111111"), eq("DeleteTable"), any(), eq("us-east-1"));
    }

    @Test
    void aForwardForATableFlociDoesNotKnowIsRefusedAndNeverReachesTheContainer() {
        DynamoDbContainerBackend backend = backendWith("container");
        // Floci deleted the table. Whether the container still holds it is irrelevant: the control
        // plane is the authority, so the read must not be served from it.
        when(dynamoDbService.describeTable(eq("Users"), eq("us-east-1")))
                .thenThrow(new AwsException("ResourceNotFoundException",
                        "Requested resource not found", 400));

        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");

        AwsException thrown = org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("GetItem", read, "us-east-1"));
        assertEquals("ResourceNotFoundException", thrown.getErrorCode());
        verify(client, never()).call(eq("GetItem"), any(), any());
    }

    @Test
    void aForwardForATableFlociKnowsProceeds() {
        DynamoDbContainerBackend backend = backendWith("container");
        when(client.call(eq("GetItem"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, MAPPER.createObjectNode()));

        ObjectNode read = MAPPER.createObjectNode();
        read.put("TableName", "Users");

        assertEquals(200, backend.forwardIfDataPlane("GetItem", read, "us-east-1").getStatus());
    }

    @Test
    void theMetadataCheckCoversEveryTableABatchOrTransactNames() {
        DynamoDbContainerBackend backend = backendWith("container");
        when(dynamoDbService.describeTable(eq("Gone"), eq("us-east-1")))
                .thenThrow(new AwsException("ResourceNotFoundException",
                        "Requested resource not found", 400));

        ObjectNode batch = MAPPER.createObjectNode();
        ObjectNode items = batch.putObject("RequestItems");
        items.putObject("Users");
        items.putObject("Gone");
        org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("BatchGetItem", batch, "us-east-1"));

        ObjectNode transact = MAPPER.createObjectNode();
        transact.putArray("TransactItems").addObject().putObject("Put").put("TableName", "Gone");
        org.junit.jupiter.api.Assertions.assertThrows(AwsException.class,
                () -> backend.forwardIfDataPlane("TransactWriteItems", transact, "us-east-1"));

        verify(client, never()).call(eq("BatchGetItem"), any(), any());
        verify(client, never()).call(eq("TransactWriteItems"), any(), any());
    }


    @Test
    void aFailedCreateMirrorWithdrawsTheTableFlociAlreadyCommitted() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode error = MAPPER.createObjectNode();
        error.put("__type", "com.amazon.coral.validate#ValidationException");
        error.put("message", "nope");
        when(client.call(eq("CreateTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(400, error));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("CreateTable", request, "us-east-1", table("Users")));

        // Forwards are gated on Floci knowing the table, so the metadata must not survive a mirror
        // it could not establish: otherwise reads land on whatever the container still holds.
        verify(dynamoDbService).deleteTable("Users", "us-east-1");
    }

    @Test
    void aFailedStaleDropDuringRecreateAlsoWithdrawsTheTable() {
        DynamoDbContainerBackend backend = backendWith("container");
        ObjectNode inUse = MAPPER.createObjectNode();
        inUse.put("__type", "com.amazonaws.dynamodb.v20120810#ResourceInUseException");
        ObjectNode dropError = MAPPER.createObjectNode();
        dropError.put("__type", "com.amazon.coral.service#InternalFailure");
        when(client.call(eq("CreateTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(400, inUse));
        when(client.call(eq(ACCOUNT), eq("DeleteTable"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(500, dropError));

        ObjectNode request = MAPPER.createObjectNode();
        request.put("TableName", "Users");

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> backend.mirrorControlPlane("CreateTable", request, "us-east-1", table("Users")));

        verify(dynamoDbService).deleteTable("Users", "us-east-1");
    }

}
