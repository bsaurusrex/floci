package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbStreamService;
import io.github.hectorvent.floci.services.dynamodb.model.TableDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Re-emits the backing container's DynamoDB stream into Floci's own {@link DynamoDbStreamService}.
 *
 * <p>When the container owns the data plane, Floci no longer sees writes as they happen, so the
 * before and after images its stream fan-out depends on cannot be captured inline. They cannot be
 * reconstructed from the forwarded calls either: {@code ReturnValues=ALL_OLD} does not exist on
 * BatchWriteItem or TransactWriteItems. The container implements the Streams API natively, so this
 * pump reads it and replays each record through {@code captureEvent}. Everything downstream —
 * Lambda event source mappings, EventBridge Pipes, {@code GetRecords} against Floci — keeps working
 * unchanged and still honours the caller's own StreamViewType.
 */
@ApplicationScoped
public class DynamoDbStreamPump {

    private static final Logger LOG = Logger.getLogger(DynamoDbStreamPump.class);

    private static final long POLL_INTERVAL_MS = 500;
    private static final int BATCH_LIMIT = 100;

    private final DynamoDbLocalClient client;
    private final DynamoDbStreamService streamService;
    private final ObjectMapper objectMapper;

    private final Map<String, StreamCursor> cursors = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private volatile ScheduledExecutorService poller;

    @Inject
    public DynamoDbStreamPump(DynamoDbLocalClient client,
                              DynamoDbStreamService streamService,
                              ObjectMapper objectMapper) {
        this.client = client;
        this.streamService = streamService;
        this.objectMapper = objectMapper;
    }

    /**
     * Starts replaying the container stream backing {@code table}.
     */
    public void register(TableDefinition table, String region, String containerStreamArn) {
        String key = key(region, table.getTableName());
        cursors.put(key, new StreamCursor(table, region, containerStreamArn));
        ensurePollerRunning();
        LOG.infov("Pumping DynamoDB container stream for {0} ({1})", table.getTableName(), containerStreamArn);
    }

    public void deregister(String region, String tableName) {
        cursors.remove(key(region, tableName));
    }

    private void ensurePollerRunning() {
        if (poller != null) {
            return;
        }
        synchronized (lifecycleLock) {
            if (poller != null) {
                return;
            }
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "ddb-container-stream-pump");
                thread.setDaemon(true);
                return thread;
            });
            executor.scheduleWithFixedDelay(
                    this::drainAll, POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            poller = executor;
        }
    }

    private void drainAll() {
        for (StreamCursor cursor : cursors.values()) {
            try {
                drain(cursor);
            } catch (RuntimeException e) {
                // One bad table must not stop the others; the next tick retries.
                LOG.debugv(e, "DynamoDB container stream pump failed for {0}", cursor.table.getTableName());
            }
        }
    }

    void drain(StreamCursor cursor) {
        String iterator = cursor.shardIterator;
        if (iterator == null) {
            iterator = openIterator(cursor);
            if (iterator == null) {
                return;
            }
        }

        ObjectNode request = objectMapper.createObjectNode();
        request.put("ShardIterator", iterator);
        request.put("Limit", BATCH_LIMIT);

        DynamoDbLocalClient.Result result = client.callStreams("GetRecords", request, cursor.region);
        if (!result.isSuccess()) {
            // A trimmed or expired iterator is recoverable: reopen on the next tick.
            LOG.debugv("GetRecords on the container stream for {0} failed: {1}",
                    cursor.table.getTableName(), result.errorCode());
            cursor.shardIterator = null;
            return;
        }

        for (JsonNode record : result.body().path("Records")) {
            replay(cursor, record);
            String sequenceNumber = record.path("dynamodb").path("SequenceNumber").asText(null);
            if (sequenceNumber != null) {
                cursor.lastSequenceNumber = sequenceNumber;
            }
        }

        JsonNode next = result.body().get("NextShardIterator");
        cursor.shardIterator = next != null && next.isTextual() ? next.asText() : null;
    }

    private String openIterator(StreamCursor cursor) {
        ObjectNode describe = objectMapper.createObjectNode();
        describe.put("StreamArn", cursor.containerStreamArn);
        DynamoDbLocalClient.Result described = client.callStreams("DescribeStream", describe, cursor.region);
        if (!described.isSuccess()) {
            return null;
        }
        JsonNode shards = described.body().path("StreamDescription").path("Shards");
        if (!shards.isArray() || shards.isEmpty()) {
            return null;
        }
        String shardId = shards.get(shards.size() - 1).path("ShardId").asText(null);
        if (shardId == null) {
            return null;
        }

        ObjectNode iteratorRequest = objectMapper.createObjectNode();
        iteratorRequest.put("StreamArn", cursor.containerStreamArn);
        iteratorRequest.put("ShardId", shardId);
        if (cursor.lastSequenceNumber == null) {
            // TRIM_HORIZON, not LATEST: the table is registered at CreateTable, so starting at the
            // oldest record cannot replay writes that predate Floci knowing about the table.
            iteratorRequest.put("ShardIteratorType", "TRIM_HORIZON");
        } else {
            // Reopening after an expired or trimmed iterator. Resuming from the horizon would
            // replay everything already emitted, and each replayed record is another event source
            // mapping invocation.
            iteratorRequest.put("ShardIteratorType", "AFTER_SEQUENCE_NUMBER");
            iteratorRequest.put("SequenceNumber", cursor.lastSequenceNumber);
        }
        DynamoDbLocalClient.Result opened = client.callStreams("GetShardIterator", iteratorRequest, cursor.region);
        if (!opened.isSuccess()) {
            return null;
        }
        String iterator = opened.body().path("ShardIterator").asText(null);
        cursor.shardIterator = iterator;
        return iterator;
    }

    private void replay(StreamCursor cursor, JsonNode record) {
        String eventName = record.path("eventName").asText(null);
        if (eventName == null) {
            return;
        }
        JsonNode payload = record.path("dynamodb");
        JsonNode oldImage = payload.hasNonNull("OldImage") ? payload.get("OldImage") : null;
        JsonNode newImage = payload.hasNonNull("NewImage") ? payload.get("NewImage") : null;
        streamService.captureEvent(
                cursor.table.getTableName(), eventName, oldImage, newImage, cursor.table, cursor.region);
    }

    /**
     * Stops polling. Safe to call when nothing was ever registered.
     */
    public void stopAll() {
        synchronized (lifecycleLock) {
            cursors.clear();
            if (poller != null) {
                poller.shutdownNow();
                poller = null;
            }
        }
    }

    private static String key(String region, String tableName) {
        return region + "/" + tableName;
    }

    static final class StreamCursor {
        private final TableDefinition table;
        private final String region;
        private final String containerStreamArn;
        private volatile String shardIterator;
        /** Last record handed to Floci, so a reopened iterator resumes instead of replaying. */
        private volatile String lastSequenceNumber;

        StreamCursor(TableDefinition table, String region, String containerStreamArn) {
            this.table = table;
            this.region = region;
            this.containerStreamArn = containerStreamArn;
        }
    }
}
