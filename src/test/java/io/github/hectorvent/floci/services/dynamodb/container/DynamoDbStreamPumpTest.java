package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbStreamService;
import io.github.hectorvent.floci.services.dynamodb.model.TableDefinition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDbStreamPumpTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DynamoDbLocalClient client = mock(DynamoDbLocalClient.class);
    private final DynamoDbStreamService streamService = mock(DynamoDbStreamService.class);
    private final DynamoDbStreamPump pump = new DynamoDbStreamPump(client, streamService, MAPPER);

    private static TableDefinition table() {
        TableDefinition definition = new TableDefinition();
        definition.setTableName("Users");
        return definition;
    }

    private void stubShardDiscovery() {
        ObjectNode described = MAPPER.createObjectNode();
        described.putObject("StreamDescription").putArray("Shards")
                .addObject().put("ShardId", "shard-1");
        when(client.callStreams(eq("DescribeStream"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, described));

        ObjectNode opened = MAPPER.createObjectNode();
        opened.put("ShardIterator", "iterator-1");
        when(client.callStreams(eq("GetShardIterator"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, opened));
    }

    private static ObjectNode recordsResponse(String sequenceNumber, String nextIterator) {
        ObjectNode body = MAPPER.createObjectNode();
        ObjectNode record = body.putArray("Records").addObject();
        record.put("eventName", "INSERT");
        ObjectNode payload = record.putObject("dynamodb");
        payload.put("SequenceNumber", sequenceNumber);
        payload.putObject("NewImage").putObject("pk").put("S", "a");
        if (nextIterator != null) {
            body.put("NextShardIterator", nextIterator);
        }
        return body;
    }

    @Test
    void recordsAreReplayedIntoFlociStreamServiceWithBothImages() {
        stubShardDiscovery();
        when(client.callStreams(eq("GetRecords"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, recordsResponse("001", "iterator-2")));

        TableDefinition definition = table();
        pump.drain(new DynamoDbStreamPump.StreamCursor(definition, "us-east-1", "arn:stream/1"));

        ArgumentCaptor<JsonNode> newImage = ArgumentCaptor.forClass(JsonNode.class);
        verify(streamService).captureEvent(
                eq("Users"), eq("INSERT"), eq(null), newImage.capture(), eq(definition), eq("us-east-1"));
        assertEquals("a", newImage.getValue().path("pk").path("S").asText());
    }

    @Test
    void aLostIteratorResumesAfterTheLastRecordInsteadOfReplayingTheStream() {
        stubShardDiscovery();
        DynamoDbStreamPump.StreamCursor cursor =
                new DynamoDbStreamPump.StreamCursor(table(), "us-east-1", "arn:stream/1");

        // First drain consumes one record, then the iterator is lost.
        when(client.callStreams(eq("GetRecords"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(200, recordsResponse("042", null)));
        pump.drain(cursor);

        // Second drain has to reopen. Resuming at TRIM_HORIZON would re-deliver record 042 and
        // fire every event source mapping again.
        pump.drain(cursor);

        ArgumentCaptor<JsonNode> iteratorRequest = ArgumentCaptor.forClass(JsonNode.class);
        verify(client, times(2)).callStreams(eq("GetShardIterator"), iteratorRequest.capture(), any());
        JsonNode reopen = iteratorRequest.getAllValues().get(1);
        assertEquals("AFTER_SEQUENCE_NUMBER", reopen.path("ShardIteratorType").asText());
        assertEquals("042", reopen.path("SequenceNumber").asText());

        JsonNode first = iteratorRequest.getAllValues().get(0);
        assertEquals("TRIM_HORIZON", first.path("ShardIteratorType").asText(),
                "the very first open must not skip writes made before the first poll");
    }

    @Test
    void aFailedGetRecordsDropsTheIteratorRatherThanKillingThePump() {
        stubShardDiscovery();
        ObjectNode expired = MAPPER.createObjectNode();
        expired.put("__type", "com.amazonaws.dynamodb.v20120810#ExpiredIteratorException");
        when(client.callStreams(eq("GetRecords"), any(), any()))
                .thenReturn(new DynamoDbLocalClient.Result(400, expired));

        DynamoDbStreamPump.StreamCursor cursor =
                new DynamoDbStreamPump.StreamCursor(table(), "us-east-1", "arn:stream/1");
        pump.drain(cursor);
        pump.drain(cursor);

        // Two drains, two reopen attempts: the failure is recoverable, not terminal.
        verify(client, times(2)).callStreams(eq("GetShardIterator"), any(), any());
    }
}
