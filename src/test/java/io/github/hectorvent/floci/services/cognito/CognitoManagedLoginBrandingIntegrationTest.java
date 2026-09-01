package io.github.hectorvent.floci.services.cognito;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.UUID;

import static io.github.hectorvent.floci.services.cognito.CognitoRestAssuredUtils.cognitoAction;
import static io.github.hectorvent.floci.services.cognito.CognitoRestAssuredUtils.cognitoJson;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers CreateManagedLoginBranding, DescribeManagedLoginBranding,
 * DescribeManagedLoginBrandingByClient, UpdateManagedLoginBranding and
 * DeleteManagedLoginBranding.
 *
 * <p>Response shapes and error messages were measured against the live Cognito API. In
 * particular {@code Settings} is omitted entirely when the caller supplied none, while
 * {@code Assets} is always returned.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CognitoManagedLoginBrandingIntegrationTest {

    private static String poolId;
    private static String clientId;
    private static String brandingId;

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    @Test
    @Order(1)
    void createPoolAndClient() throws Exception {
        poolId = cognitoJson("CreateUserPool", """
                {
                  "PoolName": "BrandingTestPool"
                }
                """).path("UserPool").path("Id").asText();

        clientId = cognitoJson("CreateUserPoolClient", """
                {
                  "UserPoolId": "%s",
                  "ClientName": "branding-client"
                }
                """.formatted(poolId)).path("UserPoolClient").path("ClientId").asText();
    }

    @Test
    @Order(2)
    void describeByClientBeforeAnyBrandingExists() {
        cognitoAction("DescribeManagedLoginBrandingByClient", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s"
                }
                """.formatted(poolId, clientId))
                .then()
                .statusCode(400)
                .body("__type", equalTo("ResourceNotFoundException"))
                .body("message", equalTo(
                        "ManagedLoginBranding for client " + clientId + " does not exist."));
    }

    @Test
    @Order(3)
    void createWithCognitoProvidedValuesOmitsSettings() throws Exception {
        JsonNode branding = cognitoJson("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s",
                  "UseCognitoProvidedValues": true
                }
                """.formatted(poolId, clientId)).path("ManagedLoginBranding");

        brandingId = branding.path("ManagedLoginBrandingId").asText();
        assertTrue(branding.path("Settings").isMissingNode(),
                "AWS omits Settings when the caller supplied none");
        assertTrue(branding.path("Assets").isArray(), "Assets is always returned");
        assertEquals(0, branding.path("Assets").size());
        assertTrue(branding.path("UseCognitoProvidedValues").asBoolean());
        assertEquals(poolId, branding.path("UserPoolId").asText());
    }

    @Test
    @Order(4)
    void createRejectsASecondBrandingForTheSameClient() {
        cognitoAction("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s",
                  "UseCognitoProvidedValues": true
                }
                """.formatted(poolId, clientId))
                .then()
                .statusCode(400)
                .body("__type", equalTo("ManagedLoginBrandingExistsException"))
                .body("message", equalTo(
                        "A ManagedLoginBranding already exists for client " + clientId));
    }

    @Test
    @Order(5)
    void describeByIdReturnsTheSameBranding() throws Exception {
        JsonNode branding = cognitoJson("DescribeManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s"
                }
                """.formatted(poolId, brandingId)).path("ManagedLoginBranding");

        assertEquals(brandingId, branding.path("ManagedLoginBrandingId").asText());
        assertTrue(branding.path("Settings").isMissingNode());
    }

    @Test
    @Order(6)
    void describeRejectsAnIdThatIsNotAVersion4Uuid() {
        cognitoAction("DescribeManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "00000000-0000-0000-0000-000000000000"
                }
                """.formatted(poolId))
                .then()
                .statusCode(400)
                .body("__type", equalTo("InvalidParameterException"));
    }

    @Test
    @Order(7)
    void describeReportsAWellFormedButUnknownId() {
        cognitoAction("DescribeManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s"
                }
                """.formatted(poolId, UUID.randomUUID()))
                .then()
                .statusCode(400)
                .body("__type", equalTo("ResourceNotFoundException"))
                .body("message", equalTo("ManagedLoginBranding does not exist."));
    }

    @Test
    @Order(8)
    void updateSetsSettingsAndAssets() throws Exception {
        JsonNode branding = cognitoJson("UpdateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s",
                  "UseCognitoProvidedValues": false,
                  "Settings": {"components": {}},
                  "Assets": [
                    {"Category": "FAVICON_SVG", "ColorMode": "DARK", "Extension": "SVG", "Bytes": "PHN2Zy8+"}
                  ]
                }
                """.formatted(poolId, brandingId)).path("ManagedLoginBranding");

        assertTrue(branding.path("Settings").isObject());
        assertEquals(1, branding.path("Assets").size());
        assertEquals("FAVICON_SVG", branding.path("Assets").get(0).path("Category").asText());

        JsonNode readBack = cognitoJson("DescribeManagedLoginBrandingByClient", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s"
                }
                """.formatted(poolId, clientId)).path("ManagedLoginBranding");
        assertEquals(1, readBack.path("Assets").size());
    }

    @Test
    @Order(9)
    void updateLeavesOmittedMembersAlone() throws Exception {
        cognitoJson("UpdateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s",
                  "UseCognitoProvidedValues": false
                }
                """.formatted(poolId, brandingId));

        JsonNode branding = cognitoJson("DescribeManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s"
                }
                """.formatted(poolId, brandingId)).path("ManagedLoginBranding");

        assertEquals(1, branding.path("Assets").size(), "omitting Assets must not clear them");
        assertTrue(branding.path("Settings").isObject(), "omitting Settings must not clear it");
    }

    @Test
    @Order(10)
    void createRejectsAnUnknownClient() {
        cognitoAction("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "nosuchclientid",
                  "UseCognitoProvidedValues": true
                }
                """.formatted(poolId))
                .then()
                .statusCode(400)
                .body("__type", equalTo("ResourceNotFoundException"));
    }

    /**
     * Measured against AWS: a create naming neither member is rejected, a wrongly typed Assets
     * is a deserialization failure, and a non-boolean UseCognitoProvidedValues is accepted
     * rather than rejected.
     */
    @Test
    @Order(11)
    void malformedCreateMembersMatchAwsHandling() throws Exception {
        String otherClient = cognitoJson("CreateUserPoolClient", """
                {
                  "UserPoolId": "%s",
                  "ClientName": "branding-client-2"
                }
                """.formatted(poolId)).path("UserPoolClient").path("ClientId").asText();

        cognitoAction("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s"
                }
                """.formatted(poolId, otherClient))
                .then()
                .statusCode(400)
                .body("__type", equalTo("InvalidParameterException"))
                .body("message", equalTo(
                        "useCognitoProvidedValues or settings should be specified (but not both)"));

        cognitoAction("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s",
                  "UseCognitoProvidedValues": true,
                  "Assets": "nope"
                }
                """.formatted(poolId, otherClient))
                .then()
                .statusCode(400)
                .body("__type", equalTo("SerializationException"));

        cognitoAction("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s",
                  "UseCognitoProvidedValues": true,
                  "Assets": [1, 2]
                }
                """.formatted(poolId, otherClient))
                .then()
                .statusCode(400)
                .body("__type", equalTo("SerializationException"))
                .body("message", equalTo("Unexpected value type in payload"));

        cognitoAction("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s",
                  "UseCognitoProvidedValues": true,
                  "Assets": [null]
                }
                """.formatted(poolId, otherClient))
                .then()
                .statusCode(400)
                .body("__type", equalTo("InvalidParameterException"))
                .body("message", equalTo(
                        "1 validation error detected: Value '[null]' at 'assets' failed to satisfy "
                                + "constraint: Member must satisfy constraint: [Member must not be null]"));

        JsonNode coerced = cognitoJson("CreateManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ClientId": "%s",
                  "UseCognitoProvidedValues": "yes"
                }
                """.formatted(poolId, otherClient)).path("ManagedLoginBranding");
        assertTrue(coerced.path("ManagedLoginBrandingId").isTextual(),
                "AWS accepts a non-boolean UseCognitoProvidedValues rather than rejecting it");

        cognitoAction("DeleteManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s"
                }
                """.formatted(poolId, coerced.path("ManagedLoginBrandingId").asText()))
                .then()
                .statusCode(200);
    }

    @Test
    @Order(12)
    void deleteRemovesTheBranding() {
        cognitoAction("DeleteManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s"
                }
                """.formatted(poolId, brandingId))
                .then()
                .statusCode(200);

        cognitoAction("DescribeManagedLoginBranding", """
                {
                  "UserPoolId": "%s",
                  "ManagedLoginBrandingId": "%s"
                }
                """.formatted(poolId, brandingId))
                .then()
                .statusCode(400)
                .body("__type", equalTo("ResourceNotFoundException"));
    }

    @Test
    @Order(13)
    void deletePool() {
        cognitoAction("DeleteUserPool", """
                {
                  "UserPoolId": "%s"
                }
                """.formatted(poolId))
                .then()
                .statusCode(200);
    }
}
