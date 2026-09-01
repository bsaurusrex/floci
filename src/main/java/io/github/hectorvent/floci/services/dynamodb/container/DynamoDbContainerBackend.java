package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbService;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbStreamService;
import io.github.hectorvent.floci.services.dynamodb.model.TableDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.LinkedHashSet;
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
 *   <li><b>Container</b>: item storage, expression evaluation, PartiQL, transactions. These are
 *       forwarded verbatim so their semantics, error codes and messages come from AWS's own
 *       downloadable engine rather than a reimplementation.</li>
 *   <li><b>Floci</b>: everything else, namely table metadata, ARNs, tags, PITR, exports, Kinesis
 *       streaming destinations and stream fan-out to Lambda event source mappings and Pipes.
 *       The container never becomes the source of truth for the control plane, so every
 *       CreateTable parameter Floci already accepts keeps round-tripping unchanged.</li>
 * </ul>
 *
 * <p>Table shape is mirrored into the container on CreateTable; the mirror carries only what the
 * container models. Anything it would reject (tags, SSE, table class, deletion protection) is
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
     * Actions that name their tables inside PartiQL statement text rather than a TableName member.
     */
    private static final Set<String> PARTIQL_ACTIONS = Set.of(
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
    private final RegionResolver regionResolver;
    private final DynamoDbService dynamoDbService;

    /** Tables already mirrored, so UpdateTable can tell new from known. Keyed by account. */
    private final Map<String, String> mirroredStreamArns = new ConcurrentHashMap<>();

    /**
     * Drops the container refused, recorded so PartiQL can be held off until they go through.
     *
     * <p>Keyed and retried by the account that owns them. The account selects the store inside the
     * container, so retrying under a later caller's account would drop that caller's live table.
     * Everything other than PartiQL is gated on Floci's own metadata instead, which needs no
     * bookkeeping to be correct.
     */
    private final Map<String, OrphanedTable> failedDrops = new ConcurrentHashMap<>();

    /** A table left in the container after Floci deleted its own copy. */
    private record OrphanedTable(String accountId, String region, String tableName) { }

    @Inject
    public DynamoDbContainerBackend(EmulatorConfig config,
                                    DynamoDbLocalClient client,
                                    DynamoDbStreamPump streamPump,
                                    ObjectMapper objectMapper,
                                    RegionResolver regionResolver,
                                    DynamoDbService dynamoDbService) {
        this.config = config;
        this.client = client;
        this.streamPump = streamPump;
        this.objectMapper = objectMapper;
        this.regionResolver = regionResolver;
        this.dynamoDbService = dynamoDbService;
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
        if (PARTIQL_ACTIONS.contains(action)) {
            refuseWhileAnyDropIsOutstanding(region);
        } else {
            requireTablesKnownToFloci(request, region);
        }
        DynamoDbLocalClient.Result result = client.call(action, request, region);
        // Errors are passed through verbatim: the container's codes and messages are the
        // reason for routing here in the first place.
        return Response.status(result.statusCode()).entity(result.body()).build();
    }

    /**
     * Refuses a forward Floci's own metadata says should not succeed.
     *
     * <p>Forwarded calls would otherwise never consult the control plane, so a table the container
     * still holds after a failed drop would keep answering reads. Floci's table store is the
     * authority and its delete is atomic, so asking it is both correct and free of the races that
     * tracking in-flight deletions separately invites.
     */
    private void requireTablesKnownToFloci(JsonNode request, String region) {
        for (String tableName : referencedTables(request)) {
            // Throws ResourceNotFoundException when Floci does not have the table.
            dynamoDbService.describeTable(tableName, region);
        }
    }

    /**
     * Refuses PartiQL while a drop is outstanding, after retrying the outstanding drops.
     *
     * <p>PartiQL names its table inside the statement text, so the metadata check above cannot see
     * it. Floci's own parser could be pointed at the statement, but that parser is part of the
     * engine this backend exists to bypass: a form it does not accept but the container does would
     * silently let the statement through. Refusing every statement in the affected account and
     * region while a drop is outstanding cannot be defeated that way, and clears as soon as a
     * retry succeeds.
     */
    private void refuseWhileAnyDropIsOutstanding(String region) {
        if (failedDrops.isEmpty()) {
            return;
        }
        String accountId = regionResolver.getAccountId();
        for (Map.Entry<String, OrphanedTable> entry : Map.copyOf(failedDrops).entrySet()) {
            OrphanedTable outstanding = entry.getValue();
            if (!outstanding.accountId().equals(accountId) || !outstanding.region().equals(region)) {
                continue;
            }
            try {
                dropAs(outstanding);
            } catch (RuntimeException e) {
                LOG.debugv("Still cannot drop {0}, refusing PartiQL", entry.getKey());
            }
        }
        boolean stillOutstanding = failedDrops.values().stream()
                .anyMatch(o -> o.accountId().equals(accountId) && o.region().equals(region));
        if (stillOutstanding) {
            throw new AwsException("ResourceNotFoundException",
                    "Requested resource not found", 400);
        }
    }

    /**
     * Table names a data-plane request names, across the single-table, batch and transact shapes.
     */
    private static Set<String> referencedTables(JsonNode request) {
        Set<String> tables = new LinkedHashSet<>();
        JsonNode single = request.get("TableName");
        if (single != null && single.isTextual()) {
            tables.add(single.asText());
        }
        JsonNode requestItems = request.get("RequestItems");
        if (requestItems != null && requestItems.isObject()) {
            requestItems.fieldNames().forEachRemaining(tables::add);
        }
        JsonNode transactItems = request.get("TransactItems");
        if (transactItems != null && transactItems.isArray()) {
            for (JsonNode item : transactItems) {
                // Each entry wraps exactly one of Put, Update, Delete, Get or ConditionCheck.
                item.forEach(operation -> {
                    JsonNode name = operation.get("TableName");
                    if (name != null && name.isTextual()) {
                        tables.add(name.asText());
                    }
                });
            }
        }
        return tables;
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

        DynamoDbLocalClient.Result result;
        try {
            result = client.call("CreateTable", mirror, region);
            if ("ResourceInUseException".equals(result.errorCode()) && !result.isSuccess()) {
                // A table of this name survived in the container, which means an earlier
                // DeleteTable mirror did not land. Floci has already committed the new table, so
                // the stale one and its items must go, otherwise reads would serve data from the
                // previous generation.
                LOG.warnv("Dropping a stale container table for {0} left by a failed delete",
                        table.getTableName());
                dropFromContainer(table.getTableName(), region);
                result = client.call("CreateTable", mirror, region);
            }
            if (!result.isSuccess()) {
                throw new IllegalStateException("DynamoDB container backend rejected CreateTable "
                        + "for " + table.getTableName() + ": " + result.errorCode() + " "
                        + result.errorMessage());
            }
        } catch (RuntimeException e) {
            // Floci committed this table before the mirror ran, and forwards are gated on Floci
            // knowing it. Leaving the metadata in place would point every request at whatever the
            // container still holds under that name, which is the previous generation. Withdraw it
            // so the control plane keeps telling the truth about what is usable.
            withdrawFlociTable(table.getTableName(), region);
            throw e;
        }

        failedDrops.remove(key(region, table.getTableName()));
        if (streamed) {
            String streamArn = result.body().path("TableDescription").path("LatestStreamArn").asText(null);
            if (streamArn != null) {
                mirroredStreamArns.put(key(region, table.getTableName()), streamArn);
                streamPump.register(regionResolver.getAccountId(), table, region, streamArn);
            }
        }
    }

    /**
     * Removes a table Floci committed whose container mirror could not be established.
     *
     * <p>Best effort: the mirror failure is already being reported, and masking it with a
     * secondary failure from the withdrawal would lose the actionable error.
     */
    private void withdrawFlociTable(String tableName, String region) {
        try {
            dynamoDbService.deleteTable(tableName, region);
        } catch (RuntimeException e) {
            LOG.warnv(e, "Could not withdraw {0} after its container mirror failed", tableName);
        }
    }

    private void mirrorDeleteTable(JsonNode request, String region) {
        String tableName = request.path("TableName").asText(null);
        if (tableName == null) {
            return;
        }
        streamPump.deregister(regionResolver.getAccountId(), region, tableName);
        mirroredStreamArns.remove(key(region, tableName));
        dropFromContainer(tableName, region);
    }

    /**
     * Removes a table from the container, tolerating one that is already gone.
     *
     * <p>Anything else is fatal rather than logged. Floci has already dropped its own copy by this
     * point, so a surviving container table would keep the deleted items reachable through
     * forwarded reads, and would collide with the next table of the same name.
     */
    private void dropFromContainer(String tableName, String region) {
        dropAs(new OrphanedTable(regionResolver.getAccountId(), region, tableName));
    }

    /** Drops a table under the account that owns it, not the account making the current request. */
    private void dropAs(OrphanedTable orphan) {
        String tableName = orphan.tableName();
        String region = orphan.region();
        String orphanKey = key(orphan.accountId(), region, tableName);
        ObjectNode mirror = objectMapper.createObjectNode();
        mirror.put("TableName", tableName);
        DynamoDbLocalClient.Result result =
                client.call(orphan.accountId(), "DeleteTable", mirror, region);
        if (result.isSuccess() || "ResourceNotFoundException".equals(result.errorCode())) {
            failedDrops.remove(orphanKey);
            return;
        }
        failedDrops.put(orphanKey, orphan);
        throw new IllegalStateException("DynamoDB container backend could not drop " + tableName
                + ", its items would stay readable: " + result.errorCode() + " "
                + result.errorMessage());
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
                streamPump.register(regionResolver.getAccountId(), table, region, streamArn);
            }
        } else if (request.path("StreamSpecification").has("StreamEnabled")) {
            streamPump.deregister(regionResolver.getAccountId(), region, table.getTableName());
        }
    }

    private void mirrorUpdateTimeToLive(JsonNode request, String region) {
        DynamoDbLocalClient.Result result = client.call("UpdateTimeToLive", request, region);
        if (!result.isSuccess()) {
            LOG.warnv("DynamoDB container backend could not apply UpdateTimeToLive: {0} {1}",
                    result.errorCode(), result.errorMessage());
        }
    }

    private static String key(String accountId, String region, String tableName) {
        return accountId + "/" + region + "/" + tableName;
    }

    private String key(String region, String tableName) {
        return key(regionResolver.getAccountId(), region, tableName);
    }

    /**
     * Exposed so {@link DynamoDbStreamService} consumers see events for tables that already
     * existed when a stream was enabled.
     */
    public boolean isMirrored(String region, String tableName) {
        return mirroredStreamArns.containsKey(key(region, tableName));
    }
}
