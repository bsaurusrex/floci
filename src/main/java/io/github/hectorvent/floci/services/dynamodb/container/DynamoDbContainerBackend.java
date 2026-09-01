package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbStreamService;
import io.github.hectorvent.floci.services.dynamodb.model.TableDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes the DynamoDB data plane to the backing {@code amazon/dynamodb-local} container when
 * {@code floci.services.dynamodb.backend=container}.
 *
 * <p>The split is deliberate and narrow:
 *
 * <ul>
 *   <li><b>Container</b> — item storage, expression evaluation, PartiQL, transactions. These are
 *       forwarded verbatim so their semantics, error codes and messages come from AWS's own
 *       downloadable engine rather than a reimplementation.</li>
 *   <li><b>Floci</b> — everything else: table metadata, ARNs, tags, PITR, exports, Kinesis
 *       streaming destinations and stream fan-out to Lambda event source mappings and Pipes.
 *       The container never becomes the source of truth for the control plane, so every
 *       CreateTable parameter Floci already accepts keeps round-tripping unchanged.</li>
 * </ul>
 *
 * <p>Table shape is mirrored into the container on CreateTable; the mirror carries only what the
 * container models. Anything it would reject — tags, SSE, table class, deletion protection — is
 * stripped, because Floci is still the thing that answers DescribeTable.
 */
@ApplicationScoped
public class DynamoDbContainerBackend {

    private static final Logger LOG = Logger.getLogger(DynamoDbContainerBackend.class);

    /**
     * Actions whose semantics the container owns. Everything absent from this set stays in Floci.
     */
    private static final Set<String> DATA_PLANE_ACTIONS = Set.of(
            "PutItem", "GetItem", "UpdateItem", "DeleteItem",
            "Query", "Scan",
            "BatchWriteItem", "BatchGetItem",
            "TransactWriteItems", "TransactGetItems",
            "ExecuteStatement", "ExecuteTransaction", "BatchExecuteStatement");

    /**
     * CreateTable members DynamoDB local does not model. Floci keeps serving all of them.
     */
    private static final List<String> UNSUPPORTED_CREATE_MEMBERS = List.of(
            "Tags", "SSESpecification", "TableClass", "DeletionProtectionEnabled",
            "ResourcePolicy", "WarmThroughput", "OnDemandThroughput");

    private final EmulatorConfig config;
    private final DynamoDbLocalClient client;
    private final DynamoDbStreamPump streamPump;
    private final ObjectMapper objectMapper;

    /** region + "/" + tableName of tables already mirrored, so UpdateTable can tell new from known. */
    private final Map<String, String> mirroredStreamArns = new ConcurrentHashMap<>();

    @Inject
    public DynamoDbContainerBackend(EmulatorConfig config,
                                    DynamoDbLocalClient client,
                                    DynamoDbStreamPump streamPump,
                                    ObjectMapper objectMapper) {
        this.config = config;
        this.client = client;
        this.streamPump = streamPump;
        this.objectMapper = objectMapper;
    }

    public boolean isEnabled() {
        return EmulatorConfig.DynamoDbServiceConfig.BACKEND_CONTAINER
                .equalsIgnoreCase(config.services().dynamodb().backend());
    }

    /**
     * Forwards {@code action} to the container when it owns that action's semantics.
     *
     * @return the container's response, or null when Floci should handle the action itself
     */
    public Response forwardIfDataPlane(String action, JsonNode request, String region) {
        if (!DATA_PLANE_ACTIONS.contains(action)) {
            return null;
        }
        DynamoDbLocalClient.Result result = client.call(action, request, region);
        // Errors are passed through verbatim: the container's codes and messages are the
        // reason for routing here in the first place.
        return Response.status(result.statusCode()).entity(result.body()).build();
    }

    /**
     * Mirrors a control-plane change into the container after Floci has applied it.
     *
     * @param table Floci's authoritative definition, already updated
     */
    public void mirrorControlPlane(String action, JsonNode request, String region, TableDefinition table) {
        try {
            switch (action) {
                case "CreateTable" -> mirrorCreateTable(request, region, table);
                case "DeleteTable" -> mirrorDeleteTable(request, region);
                case "UpdateTable" -> mirrorUpdateTable(request, region, table);
                case "UpdateTimeToLive" -> mirrorUpdateTimeToLive(request, region);
                default -> { }
            }
        } catch (RuntimeException e) {
            // A mirror failure must not corrupt Floci's own view of the table, which is already
            // committed. Surface it loudly instead: the data plane for this table is now broken.
            LOG.errorv(e, "Failed to mirror {0} into the DynamoDB container backend", action);
            throw e;
        }
    }

    private void mirrorCreateTable(JsonNode request, String region, TableDefinition table) {
        ObjectNode mirror = request.deepCopy();
        for (String member : UNSUPPORTED_CREATE_MEMBERS) {
            mirror.remove(member);
        }
        // The container's stream is the source of the events Floci fans out, so it must carry
        // both images regardless of the view type the caller asked for. Floci applies the
        // caller's StreamViewType when it re-emits, in DynamoDbStreamService.captureEvent.
        boolean streamed = request.path("StreamSpecification").path("StreamEnabled").asBoolean(false);
        if (streamed) {
            ObjectNode spec = objectMapper.createObjectNode();
            spec.put("StreamEnabled", true);
            spec.put("StreamViewType", "NEW_AND_OLD_IMAGES");
            mirror.set("StreamSpecification", spec);
        }

        DynamoDbLocalClient.Result result = client.call("CreateTable", mirror, region);
        if (!result.isSuccess()) {
            throw new IllegalStateException("DynamoDB container backend rejected CreateTable for "
                    + table.getTableName() + ": " + result.errorCode() + " " + result.errorMessage());
        }

        if (streamed) {
            String streamArn = result.body().path("TableDescription").path("LatestStreamArn").asText(null);
            if (streamArn != null) {
                mirroredStreamArns.put(key(region, table.getTableName()), streamArn);
                streamPump.register(table, region, streamArn);
            }
        }
    }

    private void mirrorDeleteTable(JsonNode request, String region) {
        String tableName = request.path("TableName").asText(null);
        if (tableName == null) {
            return;
        }
        streamPump.deregister(region, tableName);
        mirroredStreamArns.remove(key(region, tableName));

        ObjectNode mirror = objectMapper.createObjectNode();
        mirror.put("TableName", tableName);
        DynamoDbLocalClient.Result result = client.call("DeleteTable", mirror, region);
        if (!result.isSuccess() && !"ResourceNotFoundException".equals(result.errorCode())) {
            LOG.warnv("DynamoDB container backend could not drop {0}: {1} {2}",
                    tableName, result.errorCode(), result.errorMessage());
        }
    }

    private void mirrorUpdateTable(JsonNode request, String region, TableDefinition table) {
        ObjectNode mirror = request.deepCopy();
        for (String member : UNSUPPORTED_CREATE_MEMBERS) {
            mirror.remove(member);
        }
        boolean enablingStream = request.path("StreamSpecification").path("StreamEnabled").asBoolean(false);
        if (enablingStream) {
            ObjectNode spec = objectMapper.createObjectNode();
            spec.put("StreamEnabled", true);
            spec.put("StreamViewType", "NEW_AND_OLD_IMAGES");
            mirror.set("StreamSpecification", spec);
        }
        if (mirror.size() <= 1) {
            // TableName only: nothing the container models changed.
            return;
        }

        DynamoDbLocalClient.Result result = client.call("UpdateTable", mirror, region);
        if (!result.isSuccess()) {
            LOG.warnv("DynamoDB container backend could not apply UpdateTable to {0}: {1} {2}",
                    table.getTableName(), result.errorCode(), result.errorMessage());
            return;
        }
        if (enablingStream) {
            String streamArn = result.body().path("TableDescription").path("LatestStreamArn").asText(null);
            if (streamArn != null) {
                mirroredStreamArns.put(key(region, table.getTableName()), streamArn);
                streamPump.register(table, region, streamArn);
            }
        } else if (request.path("StreamSpecification").has("StreamEnabled")) {
            streamPump.deregister(region, table.getTableName());
        }
    }

    private void mirrorUpdateTimeToLive(JsonNode request, String region) {
        DynamoDbLocalClient.Result result = client.call("UpdateTimeToLive", request, region);
        if (!result.isSuccess()) {
            LOG.warnv("DynamoDB container backend could not apply UpdateTimeToLive: {0} {1}",
                    result.errorCode(), result.errorMessage());
        }
    }

    private static String key(String region, String tableName) {
        return region + "/" + tableName;
    }

    /**
     * Exposed so {@link DynamoDbStreamService} consumers see events for tables that already
     * existed when a stream was enabled.
     */
    public boolean isMirrored(String region, String tableName) {
        return mirroredStreamArns.containsKey(key(region, tableName));
    }
}
