package uk.gov.companieshouse.itemhandler.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.JsonBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.mockserver.MockServerContainer;
import org.testcontainers.utility.DockerImageName;
import uk.gov.companieshouse.itemhandler.config.EmbeddedKafkaBrokerConfiguration;
import uk.gov.companieshouse.itemhandler.config.TestEnvironmentSetupHelper;
import uk.gov.companieshouse.orders.OrderReceived;
import uk.gov.companieshouse.orders.items.ChdItemOrdered;

@SpringBootTest
@Import(EmbeddedKafkaBrokerConfiguration.class)
@EmbeddedKafka
@TestPropertySource(locations = "classpath:application.properties",
        properties={"uk.gov.companieshouse.item-handler.error-consumer=false"})
class OrderMessageRetryConsumerIntegrationTest {

    private static int orderId = 123456;
    private static MockServerContainer container;
    private MockServerClient client;

    @Autowired
    private KafkaConsumer<String, ChdItemOrdered> chsItemOrderedConsumer;

    @Autowired
    private KafkaProducer<String, OrderReceived> orderReceivedProducer;

    @Autowired
    private KafkaTopics kafkaTopics;

    @Autowired
    private KafkaConsumer<String, OrderReceived> orderReceivedErrorConsumer;

    @Autowired
    private OrderProcessResponseHandlerAspect orderProcessResponseHandlerAspect;

    @Autowired
    private OrderMessageDefaultConsumerAspect orderMessageDefaultConsumerAspect;

    @Autowired
    private OrderMessageRetryConsumerAspect orderMessageRetryConsumerAspect;

    @Value("${response.handler.maximumRetryAttempts}")
    private int maximumRetryEvents;

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
    }

    @AfterEach
    void teardown() {
        client.reset();
        ++orderId;
    }

    @Test
    void testConsumesCertificateOrderReceivedFromRetryTopic() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(
                                "/fixtures/certified-certificate.json",
                                StandardCharsets.UTF_8))));
        orderMessageRetryConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceivedRetry(),
                kafkaTopics.getOrderReceivedRetry(),
                getOrderReceived())).get();
        orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // then
        assertEquals(0, orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @Test
    void testConsumesCertifiedDocumentOrderReceivedFromRetryTopic() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(
                                "/fixtures/certified-copy.json",
                                StandardCharsets.UTF_8))));
        orderMessageRetryConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceivedRetry(),
                kafkaTopics.getOrderReceivedRetry(),
                getOrderReceived())).get();
        orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // then
        assertEquals(0, orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @Test
    void testConsumesMissingImageDeliveryOrderReceivedFromRetryTopic() throws ExecutionException, InterruptedException, IOException {
        // given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(
                                "/fixtures/missing-image-delivery.json",
                                StandardCharsets.UTF_8))));
        orderMessageRetryConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceivedRetry(),
                kafkaTopics.getOrderReceivedRetry(),
                getOrderReceived())).get();
        orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);
        ChdItemOrdered actual = KafkaTestUtils.getSingleRecord(chsItemOrderedConsumer, kafkaTopics.getChdItemOrdered()).value();

        // then
        assertEquals(0, orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
        assertEquals("ORD-123123-123123", actual.getReference());
        assertNotNull(actual.getItem());
    }

    @Test
    void testPublishesOrderReceivedToRetryTopicWhenOrdersApiIsUnavailable() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value()));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));
        orderMessageRetryConsumerAspect.setBeforeProcessOrderReceivedEventLatch(new CountDownLatch(1));
        orderMessageRetryConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();

        // Wait for order received to be processed and republished to retry topic
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // Restore OrdersApi
        client.reset();
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(
                                "/fixtures/certified-certificate.json",
                                StandardCharsets.UTF_8))));

        // Start consuming from retry topic
        orderMessageRetryConsumerAspect.getBeforeProcessOrderReceivedEventLatch().countDown();
        // Wait for order received to be processed from retry topic
        orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // when
        // sync so that retry consumer has completed and app has published email onto email send topic

        // then
        assertEquals(0, orderMessageRetryConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @Test
    void testPublishesOrderReceivedToErrorTopicAfterMaxRetryAttempts() throws ExecutionException, InterruptedException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value()));
        orderProcessResponseHandlerAspect.setPostServiceUnavailableLatch(new CountDownLatch(maximumRetryEvents + 1));

        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();

        // when
        // sync so that retry consumer has completed and app has published email onto email send topic
        orderProcessResponseHandlerAspect.getPostServiceUnavailableLatch().await(30, TimeUnit.SECONDS);
        OrderReceived actual = KafkaTestUtils.getSingleRecord(orderReceivedErrorConsumer, kafkaTopics.getOrderReceivedError()).value();

        // then
        assertEquals(0, orderProcessResponseHandlerAspect.getPostServiceUnavailableLatch().getCount());
        assertEquals(getOrderReference(), actual.getOrderUri());
        assertEquals(0, actual.getAttempt());
    }

    private OrderReceived getOrderReceived() {
        OrderReceived orderReceived = new OrderReceived();
        orderReceived.setOrderUri(getOrderReference());
        return orderReceived;
    }

    private String getOrderReference() {
        return "/orders/ORD-111111-" + orderId;
    }
}