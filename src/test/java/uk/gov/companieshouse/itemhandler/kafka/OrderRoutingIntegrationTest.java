package uk.gov.companieshouse.itemhandler.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord;

import email.email_send;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.JsonBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.mockserver.MockServerContainer;
import org.testcontainers.utility.DockerImageName;
import uk.gov.companieshouse.itemgroupordered.ItemGroupOrdered;
import uk.gov.companieshouse.itemhandler.config.EmbeddedKafkaBrokerConfiguration;
import uk.gov.companieshouse.itemhandler.config.TestEnvironmentSetupHelper;
import uk.gov.companieshouse.itemhandler.service.ChdItemSenderServiceAspect;
import uk.gov.companieshouse.itemhandler.service.DigitalItemGroupSenderServiceAspect;
import uk.gov.companieshouse.itemhandler.service.EmailServiceAspect;
import uk.gov.companieshouse.itemhandler.service.SenderServiceAspect;
import uk.gov.companieshouse.orders.OrderReceived;
import uk.gov.companieshouse.orders.items.ChdItemOrdered;

@SpringBootTest
@Import(EmbeddedKafkaBrokerConfiguration.class)
@EmbeddedKafka
@TestPropertySource(locations = "classpath:application.properties",
        properties={"uk.gov.companieshouse.item-handler.error-consumer=false"})
class OrderRoutingIntegrationTest {

    private static MockServerContainer container;
    private MockServerClient client;

    @Autowired
    private KafkaProducer<String, OrderReceived> orderReceivedProducer;

    @Autowired
    private KafkaTopics kafkaTopics;

    @Autowired
    private ChdItemSenderServiceAspect chdItemSenderService;

    @Autowired
    private EmailServiceAspect emailService;

    @Autowired
    private DigitalItemGroupSenderServiceAspect digitalItemGroupSenderService;

    @Autowired
    private KafkaConsumer<String, email_send> emailSendConsumer;

    @Autowired
    private KafkaConsumer<String, ChdItemOrdered> chdItemOrderedConsumer;

    @Autowired
    private KafkaConsumer<String, ItemGroupOrdered> itemGroupOrderedConsumer;

    @BeforeAll
    static void before() {
        container = new MockServerContainer(DockerImageName.parse("mockserver/mockserver:mockserver-7.5.0"));
        container.start();
        TestEnvironmentSetupHelper.setEnvironmentVariable("API_URL",
                "http://" + container.getHost() + ":" + container.getServerPort());
        TestEnvironmentSetupHelper.setEnvironmentVariable("CHS_API_KEY", "123");
        TestEnvironmentSetupHelper.setEnvironmentVariable("PAYMENTS_API_URL",
                "http://" + container.getHost() + ":" + container.getServerPort());
        TestEnvironmentSetupHelper.setEnvironmentVariable("DOCUMENT_API_LOCAL_URL",
                "http://" + container.getHost() + ":" + container.getServerPort());
        TestEnvironmentSetupHelper.setEnvironmentVariable("ORACLE_QUERY_API_URL",
                "http://" + container.getHost() + ":" + container.getServerPort());
    }

    @AfterAll
    static void after() {
        container.stop();
    }

    @BeforeEach
    void setup() {
        client = new MockServerClient(container.getHost(), container.getServerPort());
        chdItemSenderService.resetLatch();
        emailService.resetLatch();
        digitalItemGroupSenderService.resetLatch();
    }

    @AfterEach
    void teardown() {
        client.reset();
    }

    @Test
    @Disabled("See https://companieshouse.atlassian.net/browse/DCAC-282.")
    @DisplayName("All items within order are routed correctly")
    void orderItemsRoutedCorrectly() throws ExecutionException, InterruptedException, IOException {
        // given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(
                                "/fixtures/mixed-order.json",
                                StandardCharsets.UTF_8))));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();

        // then
        verifyItemIsSentToService("MID-107116-962328", chdItemSenderService);
        verifyItemIsSentToService("CRT-113516-962308", emailService);
        verifyItemIsSentToService("CCD-289716-962308", digitalItemGroupSenderService);

        getSingleRecord(chdItemOrderedConsumer, kafkaTopics.getChdItemOrdered());
        getSingleRecord(emailSendConsumer, kafkaTopics.getEmailSend());
        getSingleRecord(itemGroupOrderedConsumer, kafkaTopics.getItemGroupOrdered());
    }

    private void verifyItemIsSentToService(final String itemId, final SenderServiceAspect senderService)
            throws InterruptedException {
        senderService.getLatch().await(30, SECONDS);
        assertEquals(0, senderService.getLatch().getCount());
        assertNotNull(senderService.getItemGroupSent());
        assertThat(senderService.getItemGroupSent().getItems().size(), is(1));
        assertThat(senderService.getItemGroupSent().getItems().getFirst().getId(), is(itemId));
    }

    private OrderReceived getOrderReceived() {
        OrderReceived orderReceived = new OrderReceived();
        orderReceived.setOrderUri(getOrderReference());
        return orderReceived;
    }

    private String getOrderReference() {
        return "/orders/ORD-111111-123123";
    }

}