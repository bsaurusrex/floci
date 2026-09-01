package io.github.hectorvent.floci.services.dynamodb.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.RegionResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Forwards DynamoDB JSON 1.0 requests to the backing {@code amazon/dynamodb-local} container.
 *
 * <p>DynamoDB local does not verify request signatures, but it does read the access key id and
 * region out of the SigV4 credential scope to select which store to serve. Sending Floci's
 * resolved account id as the access key therefore reproduces Floci's own account plus region
 * partitioning inside the container, with no {@code -sharedDb} and no name mangling. The
 * signature itself is a fixed placeholder because nothing validates it.
 *
 * <p>The access key id must be alphanumeric. DynamoDB local rejects anything else with
 * {@code UnrecognizedClientException}, and a 12-digit AWS account id already satisfies it.
 */
@ApplicationScoped
public class DynamoDbLocalClient {

    private static final Logger LOG = Logger.getLogger(DynamoDbLocalClient.class);

    private static final String TARGET_PREFIX = "DynamoDB_20120810.";
    private static final String STREAMS_TARGET_PREFIX = "DynamoDBStreams_20120810.";
    private static final String CONTENT_TYPE = "application/x-amz-json-1.0";
    private static final String PLACEHOLDER_SIGNATURE = "0".repeat(64);
    private static final DateTimeFormatter SCOPE_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final DynamoDbLocalContainerManager containerManager;
    private final RegionResolver regionResolver;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Inject
    public DynamoDbLocalClient(DynamoDbLocalContainerManager containerManager,
                               RegionResolver regionResolver,
                               ObjectMapper objectMapper) {
        this.containerManager = containerManager;
        this.regionResolver = regionResolver;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Sends a DynamoDB action to the container and returns the raw result.
     */
    public Result call(String action, JsonNode body, String region) {
        return send(TARGET_PREFIX + action, body, region);
    }

    /**
     * Sends a DynamoDB Streams action to the container and returns the raw result.
     */
    public Result callStreams(String action, JsonNode body, String region) {
        return send(STREAMS_TARGET_PREFIX + action, body, region);
    }

    private Result send(String target, JsonNode body, String region) {
        String effectiveRegion = region != null ? region : regionResolver.getDefaultRegion();
        String payload;
        try {
            payload = objectMapper.writeValueAsString(body);
        } catch (IOException e) {
            throw new IllegalStateException("Could not serialize DynamoDB request for " + target, e);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(containerManager.baseUrl() + "/"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", CONTENT_TYPE)
                .header("X-Amz-Target", target)
                .header("Authorization", authorizationHeader(effectiveRegion))
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("DynamoDB local request failed for " + target, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during DynamoDB local request " + target, e);
        }

        JsonNode parsed;
        try {
            parsed = response.body() == null || response.body().isBlank()
                    ? objectMapper.createObjectNode()
                    : objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new IllegalStateException(
                    "DynamoDB local returned an unparseable body for " + target + ": " + response.body(), e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debugv("{0} -> HTTP {1}", target, response.statusCode());
        }
        return new Result(response.statusCode(), parsed);
    }

    private String authorizationHeader(String region) {
        String scopeDate = SCOPE_DATE.format(LocalDate.now(ZoneOffset.UTC));
        return "AWS4-HMAC-SHA256 Credential=" + regionResolver.getAccountId() + "/" + scopeDate
                + "/" + region + "/dynamodb/aws4_request, "
                + "SignedHeaders=content-type;host;x-amz-target, "
                + "Signature=" + PLACEHOLDER_SIGNATURE;
    }

    /**
     * A raw response from the container.
     *
     * @param statusCode HTTP status; anything outside 2xx carries an AWS JSON error body
     * @param body parsed response body, never null
     */
    public record Result(int statusCode, JsonNode body) {

        public boolean isSuccess() {
            return statusCode >= 200 && statusCode < 300;
        }

        /**
         * Returns the AWS error code from a failure body, for example {@code ValidationException}.
         * DynamoDB reports it in {@code __type} as {@code <namespace>#<Code>}.
         */
        public String errorCode() {
            JsonNode type = body.get("__type");
            if (type == null || !type.isTextual()) {
                return "InternalFailure";
            }
            String raw = type.asText();
            int hash = raw.lastIndexOf('#');
            return hash >= 0 ? raw.substring(hash + 1) : raw;
        }

        public String errorMessage() {
            for (String field : new String[]{"message", "Message"}) {
                JsonNode node = body.get(field);
                if (node != null && node.isTextual()) {
                    return node.asText();
                }
            }
            return "DynamoDB local request failed";
        }
    }
}
