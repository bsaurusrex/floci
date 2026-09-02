package io.github.hectorvent.floci.services.cognito;

import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.github.hectorvent.floci.services.cognito.CognitoRestAssuredUtils.cognitoAction;
import static org.hamcrest.Matchers.equalTo;

/**
 * A missing user pool is named in the error message.
 *
 * <p>Measured against the live service in ap-southeast-1, which answers DescribeUserPool,
 * ListUsers, ListGroups and ListUserPoolClients on an id that does not exist with the same
 * string: {@code User pool ap-southeast-1_ZZZZZZZZZ does not exist.} The error code was already
 * correct; only the message diverged.
 */
@QuarkusTest
class CognitoUserPoolNotFoundMessageIntegrationTest {

    private static final String ABSENT_POOL = "us-east-1_ZZZZZZZZZ";
    private static final String EXPECTED = "User pool " + ABSENT_POOL + " does not exist.";

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    private static void assertNamesThePool(String action, String body) {
        cognitoAction(action, body)
            .then()
            .statusCode(400)
            .body("__type", equalTo("ResourceNotFoundException"))
            .body("message", equalTo(EXPECTED));
    }

    @Test
    void describeUserPoolNamesTheMissingPool() {
        assertNamesThePool("DescribeUserPool", """
            { "UserPoolId": "%s" }
            """.formatted(ABSENT_POOL));
    }

    // ListUsers and ListUserPoolClients are deliberately absent: they answer an absent pool with
    // an empty list rather than resolving it at all, so they never reach this message. That is a
    // separate divergence from the live service and is filed on its own.

    @Test
    void listResourceServersNamesTheMissingPool() {
        assertNamesThePool("ListResourceServers", """
            {
                "UserPoolId": "%s",
                "MaxResults": 10
            }
            """.formatted(ABSENT_POOL));
    }

    @Test
    void listGroupsNamesTheMissingPool() {
        assertNamesThePool("ListGroups", """
            { "UserPoolId": "%s" }
            """.formatted(ABSENT_POOL));
    }

    @Test
    void adminGetUserNamesTheMissingPool() {
        assertNamesThePool("AdminGetUser", """
            {
                "UserPoolId": "%s",
                "Username": "nobody"
            }
            """.formatted(ABSENT_POOL));
    }
}
