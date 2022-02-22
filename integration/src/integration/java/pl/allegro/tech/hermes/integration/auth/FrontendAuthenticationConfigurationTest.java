package pl.allegro.tech.hermes.integration.auth;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.undertow.security.impl.BasicAuthenticationMechanism;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.frontend.HermesFrontend;
import pl.allegro.tech.hermes.frontend.server.HermesServer;
import pl.allegro.tech.hermes.frontend.server.auth.AuthenticationConfiguration;
import pl.allegro.tech.hermes.integration.IntegrationTest;
import pl.allegro.tech.hermes.integration.env.FrontendStarter;
import pl.allegro.tech.hermes.test.helper.config.MutableConfigFactory;
import pl.allegro.tech.hermes.test.helper.endpoint.HermesPublisher;
import pl.allegro.tech.hermes.test.helper.message.TestMessage;
import pl.allegro.tech.hermes.test.helper.util.Ports;

import javax.ws.rs.core.Response;
import java.util.Map;

import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static pl.allegro.tech.hermes.integration.auth.SingleUserAwareIdentityManager.getHeadersWithAuthentication;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class FrontendAuthenticationConfigurationTest extends IntegrationTest {

    public static final int FRONTEND_PORT = Ports.nextAvailable();
    public static final String FRONTEND_URL = "http://127.0.0.1:" + FRONTEND_PORT;

    private static final Logger logger = LoggerFactory.getLogger(FrontendAuthenticationConfigurationTest.class);
    private static final String USERNAME = "someUser"; //TODO: share username and password between bean and test
    private static final String PASSWORD = "somePassword123";
    private static final String MESSAGE = TestMessage.of("hello", "world").body();

    protected HermesPublisher publisher;
    protected HermesServer hermesServer;

//    private HermesFrontend hermesFrontend;
    private FrontendStarter frontendStarter;

    @BeforeClass
    public void setup() throws Exception {
        frontendStarter = new FrontendStarter();
        frontendStarter.addSpringProfiles("authRequired");
        frontendStarter.overrideProperty(Configs.FRONTEND_PORT, FRONTEND_PORT);
        frontendStarter.overrideProperty(Configs.FRONTEND_SSL_ENABLED, false);
        frontendStarter.overrideProperty(Configs.FRONTEND_AUTHENTICATION_MODE, "constraint_driven");//the only one specific property
        frontendStarter.overrideProperty(Configs.FRONTEND_AUTHENTICATION_ENABLED, true);
        frontendStarter.overrideProperty(Configs.KAFKA_AUTHORIZATION_ENABLED, false);
        frontendStarter.overrideProperty(Configs.KAFKA_BROKER_LIST, kafkaClusterOne.getBootstrapServersForExternalClients());
        frontendStarter.overrideProperty(Configs.ZOOKEEPER_CONNECT_STRING, hermesZookeeperOne.getConnectionString());
        frontendStarter.overrideProperty(Configs.SCHEMA_REPOSITORY_SERVER_URL, schemaRegistry.getUrl());
        frontendStarter.overrideProperty(Configs.MESSAGES_LOCAL_STORAGE_DIRECTORY, Files.createTempDir().getAbsolutePath());

//        ConfigFactory configFactory = new MutableConfigFactory()
//                .overrideProperty(Configs.FRONTEND_PORT, FRONTEND_PORT)
//                .overrideProperty(Configs.FRONTEND_SSL_ENABLED, false)
//                .overrideProperty(Configs.FRONTEND_AUTHENTICATION_MODE, "constraint_driven")//the only one specific property
//                .overrideProperty(Configs.FRONTEND_AUTHENTICATION_ENABLED, true)
//                .overrideProperty(Configs.KAFKA_AUTHORIZATION_ENABLED, false)
//                .overrideProperty(Configs.KAFKA_BROKER_LIST, kafkaClusterOne.getBootstrapServersForExternalClients())
//                .overrideProperty(Configs.ZOOKEEPER_CONNECT_STRING, hermesZookeeperOne.getConnectionString())
//                .overrideProperty(Configs.SCHEMA_REPOSITORY_SERVER_URL, schemaRegistry.getUrl())
//                .overrideProperty(Configs.MESSAGES_LOCAL_STORAGE_DIRECTORY, Files.createTempDir().getAbsolutePath());

//        AuthenticationConfiguration authConfig = new AuthenticationConfiguration(
//                exchange -> true,
//                Lists.newArrayList(new BasicAuthenticationMechanism("basicAuthRealm")),
//                new SingleUserAwareIdentityManager(username, password));

//        hermesFrontend = HermesFrontend.frontend()
//                .withBinding(configFactory, ConfigFactory.class)
//                .withAuthenticationConfiguration(authConfig)
//                .build();

//        hermesFrontend.start();
        frontendStarter.start();

//        hermesServer = hermesFrontend.getService(HermesServer.class);
        hermesServer = frontendStarter.instance().getBean(HermesServer.class);
        publisher = new HermesPublisher(FRONTEND_URL);
    }

    @BeforeMethod
    public void after() {
        operations.buildTopic("someGroup", "topicWithAuthorization");
    }

    @AfterClass
    public void tearDown() throws Exception {
        frontendStarter.stop();
    }

    @Test
    public void shouldAuthenticateUsingBasicAuth() throws Throwable {
        //given
        Map<String, String> headers = getHeadersWithAuthentication(USERNAME, PASSWORD);

        //when
        Response response = publisher.publish("someGroup.topicWithAuthorization", MESSAGE, headers);

        //then
        logger.info("Expecting SUCCESSFUL status. Actual {}", response.getStatusInfo().getStatusCode());
        assertThat(response.getStatusInfo().getFamily()).isEqualTo(SUCCESSFUL);
    }

    @Test
    public void shouldNotAuthenticateUserWithInvalidCredentials() throws Throwable {
        //given
        Map<String, String> headers = getHeadersWithAuthentication(USERNAME, "someInvalidPassword");

        //when
        Response response = publisher.publish("someGroup.topicWithAuthorization", MESSAGE, headers);

        //then
        assertThat(response.getStatus()).isEqualTo(StatusCodes.UNAUTHORIZED);
    }

    @Test
    public void shouldNotAuthenticateUserWithoutCredentials() throws Throwable {
        //when
        Response response = publisher.publish("someGroup.topicWithAuthorization", MESSAGE);

        //then
        assertThat(response.getStatus()).isEqualTo(StatusCodes.UNAUTHORIZED);
    }
}
