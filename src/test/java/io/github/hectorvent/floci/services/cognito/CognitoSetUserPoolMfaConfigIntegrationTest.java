package io.github.hectorvent.floci.services.cognito;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.github.hectorvent.floci.services.cognito.CognitoRestAssuredUtils.cognitoAction;
import static io.github.hectorvent.floci.services.cognito.CognitoRestAssuredUtils.cognitoJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SetUserPoolMfaConfig, and the round trip through GetUserPoolMfaConfig.
 *
 * <p>The response shape was measured against the live service, which returns only the
 * factors that have been configured — a pool with software-token MFA answers
 * {@code {"SoftwareTokenMfaConfiguration":{"Enabled":true},"MfaConfiguration":"OPTIONAL"}}
 * and omits the SMS, email and WebAuthn members entirely.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CognitoSetUserPoolMfaConfigIntegrationTest {

    private static String poolId;

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    @Test
    @Order(1)
    void createPool() throws Exception {
        JsonNode pool = cognitoJson("CreateUserPool", """
                {"PoolName":"MfaConfigTestPool"}
                """);
        poolId = pool.path("UserPool").path("Id").asText();
        assertTrue(!poolId.isEmpty(), "pool id");
    }

    @Test
    @Order(2)
    void softwareTokenConfigurationIsOmittedUntilItIsSet() throws Exception {
        // Measured: the live service returns only configured factors, so an untouched
        // pool carries no SoftwareTokenMfaConfiguration member at all.
        JsonNode config = cognitoJson("GetUserPoolMfaConfig",
                """
                {"UserPoolId":"%s"}
                """.formatted(poolId));
        assertEquals("OFF", config.path("MfaConfiguration").asText());
        assertTrue(config.path("SoftwareTokenMfaConfiguration").isMissingNode(),
                "SoftwareTokenMfaConfiguration should be absent before it is configured");
    }

    @Test
    @Order(3)
    void setsMfaConfigurationAndSoftwareTokenAndReturnsThem() throws Exception {
        JsonNode set = cognitoJson("SetUserPoolMfaConfig", """
                {
                    "UserPoolId":"%s",
                    "MfaConfiguration":"OPTIONAL",
                    "SoftwareTokenMfaConfiguration":{"Enabled":true}
                }
                """.formatted(poolId));
        assertEquals("OPTIONAL", set.path("MfaConfiguration").asText());
        assertTrue(set.path("SoftwareTokenMfaConfiguration").path("Enabled").asBoolean());

        // The round trip is what the Terraform provider reads to decide whether
        // mfa_configuration / software_token_mfa_configuration have drifted.
        JsonNode got = cognitoJson("GetUserPoolMfaConfig", """
                {"UserPoolId":"%s"}
                """.formatted(poolId));
        assertEquals("OPTIONAL", got.path("MfaConfiguration").asText());
        assertTrue(got.path("SoftwareTokenMfaConfiguration").path("Enabled").asBoolean());
    }

    @Test
    @Order(4)
    void describeUserPoolReflectsTheNewMfaMode() throws Exception {
        cognitoJson("SetUserPoolMfaConfig", """
                {"UserPoolId":"%s","MfaConfiguration":"ON"}
                """.formatted(poolId));

        JsonNode pool = cognitoJson("DescribeUserPool", """
                {"UserPoolId":"%s"}
                """.formatted(poolId));
        assertEquals("ON", pool.path("UserPool").path("MfaConfiguration").asText());
    }

    @Test
    @Order(5)
    void omittingMfaConfigurationLeavesTheExistingModeAlone() throws Exception {
        JsonNode set = cognitoJson("SetUserPoolMfaConfig", """
                {"UserPoolId":"%s","SoftwareTokenMfaConfiguration":{"Enabled":false}}
                """.formatted(poolId));
        assertEquals("ON", set.path("MfaConfiguration").asText());
        assertEquals(false, set.path("SoftwareTokenMfaConfiguration").path("Enabled").asBoolean());
    }

    @Test
    @Order(6)
    void invalidMfaConfigurationIsRejected() {
        String body = cognitoAction("SetUserPoolMfaConfig", """
                {"UserPoolId":"%s","MfaConfiguration":"SOMETIMES"}
                """.formatted(poolId))
                .then().statusCode(400).extract().asString();
        assertTrue(body.contains("InvalidParameterException"), body);
        assertTrue(body.contains("Member must satisfy enum value set"), body);
    }

    @Test
    @Order(7)
    void unknownPoolIsResourceNotFound() {
        String body = cognitoAction("SetUserPoolMfaConfig", """
                {"UserPoolId":"ap-southeast-1_nosuchpool","MfaConfiguration":"OFF"}
                """)
                .then().statusCode(400).extract().asString();
        assertTrue(body.contains("ResourceNotFoundException"), body);
    }

    @Test
    @Order(8)
    void cleanup_deletePool() {
        cognitoAction("DeleteUserPool", """
                {"UserPoolId":"%s"}
                """.formatted(poolId))
                .then().statusCode(200);
    }
}
