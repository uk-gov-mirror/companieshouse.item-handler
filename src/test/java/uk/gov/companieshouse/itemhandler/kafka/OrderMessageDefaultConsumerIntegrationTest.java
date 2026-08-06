package uk.gov.companieshouse.itemhandler.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.verify;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static uk.gov.companieshouse.itemhandler.kafka.ItemGroupOrderedFactory.FILING_HISTORY_DESCRIPTION;
import static uk.gov.companieshouse.itemhandler.kafka.ItemGroupOrderedFactory.FILING_HISTORY_DESCRIPTION_VALUES;
import static uk.gov.companieshouse.itemhandler.kafka.ItemGroupOrderedFactory.FILING_HISTORY_ID;
import static uk.gov.companieshouse.itemhandler.kafka.ItemGroupOrderedFactory.FILING_HISTORY_TYPE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.JsonBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.itemgroupordered.ItemGroupOrdered;
import uk.gov.companieshouse.itemhandler.config.EmbeddedKafkaBrokerConfiguration;
import uk.gov.companieshouse.itemhandler.config.TestEnvironmentSetupHelper;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.orders.OrderReceived;
import uk.gov.companieshouse.orders.items.ChdItemOrdered;

@SpringBootTest
@Import(EmbeddedKafkaBrokerConfiguration.class)
@EmbeddedKafka
@TestPropertySource(
        locations = "classpath:application.properties",
        properties={"uk.gov.companieshouse.item-handler.error-consumer=false"}
)
class OrderMessageDefaultConsumerIntegrationTest {

    private static int orderId = 123456;
    private static MockServerContainer container;
    private MockServerClient client;

    @Autowired
    private KafkaConsumer<String, ChdItemOrdered> chsItemOrderedConsumer;

    @Autowired
    private KafkaConsumer<String, ItemGroupOrdered> itemGroupOrderedConsumer;

    @Autowired
    private KafkaProducer<String, OrderReceived> orderReceivedProducer;

    @Autowired
    private KafkaTopics kafkaTopics;

    @MockitoSpyBean
    private OrderProcessResponseHandler orderProcessResponseHandler;

    @MockitoSpyBean
    private Logger logger;

    @Autowired
    private OrderMessageDefaultConsumerAspect orderMessageDefaultConsumerAspect;

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

    @ParameterizedTest(name = "{1}")
    @MethodSource("certificateTestParameters")
    @DisplayName("Process an order containing certified certificates")
    void testConsumesCertificateOrderReceivedFromEmailSendTopic(String fixture, String description) throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(fixture,
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        ProducerRecord<String, OrderReceived> record = new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived());

        Future<RecordMetadata> future = orderReceivedProducer.send(record);

        // Log the result once the message has been sent
        RecordMetadata metadata = future.get(); // This will block until the message is sent
        logger.info("Message produced to topic " + metadata.topic() + " partition " + metadata.partition() +" at offset " + metadata.offset());

        boolean completed = orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        if (!completed) {
            // Handle the case where the latch didn't reach zero in time (timeout occurred)
            throw new AssertionError("Timed out waiting for message processing");
        }

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @Test
    @DisplayName("Process an order containing a single digital certificate")
    void testDigitalCertificateResultsInItemGroupOrderedMessage() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod("GET"))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString("/fixtures/digital-certificate.json",
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);
        final ItemGroupOrdered itemGroupOrdered = KafkaTestUtils.getSingleRecord(itemGroupOrderedConsumer,
                kafkaTopics.getItemGroupOrdered()).value();

        // then
        assertThat(orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount(), is(0L));

        assertItemGroupOrderedMessageIsAsExpected(itemGroupOrdered, "ORD-123123-123123", "CRT-123123-123123");

        assertThat(itemGroupOrdered.getItems().get(0).getItemOptions(), is(nullValue()));
    }

    @Test
    @DisplayName("Process an order containing certified certificates with different delivery timescales")
    void testConsumesCertsOrderWithDifferentDeliveryTimescalesReceivedFromEmailSendTopic() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString("/fixtures/multi-certified-certificate-timescales.json",
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("certifiedCopyTestParameters")
    @DisplayName("Process an order containing certified copies")
    void testConsumesCertifiedCopyOrderReceivedFromEmailSendTopic(String fixture, String description) throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(fixture,
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();

        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @Test
    @DisplayName("Process an order containing a single digital copy")
    void testDigitalCopyResultsInItemGroupOrderedMessage() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString("/fixtures/digital-copy.json",
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);
        final ItemGroupOrdered itemGroupOrdered = KafkaTestUtils.getSingleRecord(itemGroupOrderedConsumer,
                kafkaTopics.getItemGroupOrdered()).value();

        // then
        assertThat(orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount(), is(0L));

        assertItemGroupOrderedMessageIsAsExpected(itemGroupOrdered, "ORD-123123-123123", "CCD-123123-123123");

        final Map<String, String> options = itemGroupOrdered.getItems().get(0).getItemOptions();
        assertThat(options, is(notNullValue()));
        assertThat(options.get(FILING_HISTORY_ID), is(notNullValue()));
        assertThat(options.get(FILING_HISTORY_ID), is("F00DFACE"));
        assertThat(options.get(FILING_HISTORY_TYPE), is(notNullValue()));
        assertThat(options.get(FILING_HISTORY_TYPE), is("SH01"));
        assertThat(options.get(FILING_HISTORY_DESCRIPTION), is(notNullValue()));
        assertThat(options.get(FILING_HISTORY_DESCRIPTION), is("capital-allotment-shares"));

        final JsonNode values = new ObjectMapper().readTree(options.get(FILING_HISTORY_DESCRIPTION_VALUES));
        assertThat(getStringValue(values, "date"), is("2021-01-01"));
        final JsonNode capital = values.get("capital");
        assertThat(capital, is(notNullValue()));
        assertThat(getNestedStringValue(capital, "figure"), is("1"));
        assertThat(getNestedStringValue(capital, "currency"), is("GBP"));
    }

    @Test
    @DisplayName("Process an order containing certified copies with different delivery timescales")
    void testConsumesCertCopiesOrderWithDifferentDeliveryTimescalesReceivedFromEmailSendTopic() throws ExecutionException, InterruptedException, IOException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString("/fixtures/multi-certified-copy-timescales.json",
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
    }

    @Test
    void testConsumesMissingImageDeliveryFromOrderReceivedAndPublishesChsItemOrdered() throws ExecutionException, InterruptedException, IOException {
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
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);
        ChdItemOrdered actual = KafkaTestUtils.getSingleRecord(chsItemOrderedConsumer, kafkaTopics.getChdItemOrdered()).value();

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
        assertEquals("ORD-123123-123123", actual.getReference());
        assertNotNull(actual.getItem());
    }

    @Test
    @DisplayName("Should successfully process an order containing multiple missing image delivery items")
    void testConsumesMultipleMissingImageDeliveriesFromOrderReceivedAndPublishesChsItemOrderedTwice() throws ExecutionException, InterruptedException, IOException {
        // given
        int midId = 123123;
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.OK.value())
                        .withHeader(CONTENT_TYPE, "application/json")
                        .withBody(JsonBody.json(IOUtils.resourceToString(
                                "/fixtures/multiple-missing-image-delivery.json",
                                StandardCharsets.UTF_8))));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);
        ConsumerRecords<String, ChdItemOrdered> actual = KafkaTestUtils.getRecords(chsItemOrderedConsumer, Duration.ofMillis(30000L), 2);

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
        assertEquals(2, actual.count());
        for (ConsumerRecord<String, ChdItemOrdered> record : actual) {
            assertEquals("ORD-123123-123123", record.value().getReference());
            assertNotNull(record.value().getItem());
            assertEquals("MID-123123-" + midId++, record.value().getItem().getId());
        }
    }

    @Test
    void testLogAnErrorWhenOrdersApiReturnsOrderNotFound() throws ExecutionException, InterruptedException {
        //given
        client.when(request()
                        .withPath(getOrderReference())
                        .withMethod(HttpMethod.GET.toString()))
                .respond(response()
                        .withStatusCode(HttpStatus.NOT_FOUND.value()));
        orderMessageDefaultConsumerAspect.setAfterProcessOrderReceivedEventLatch(new CountDownLatch(1));

        // when
        orderReceivedProducer.send(new ProducerRecord<>(
                kafkaTopics.getOrderReceived(),
                kafkaTopics.getOrderReceived(),
                getOrderReceived())).get();
        orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().await(30, TimeUnit.SECONDS);

        // then
        assertEquals(0, orderMessageDefaultConsumerAspect.getAfterProcessOrderReceivedEventLatch().getCount());
        verify(orderProcessResponseHandler).serviceError(any());
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(logger).error(argumentCaptor.capture(), anyMap());
        assertEquals("order-received message processing failed with a "
                + "non-recoverable exception", argumentCaptor.getValue());
    }

    private OrderReceived getOrderReceived() {
        OrderReceived orderReceived = new OrderReceived();
        orderReceived.setOrderUri(getOrderReference());
        return orderReceived;
    }

    private String getOrderReference() {
        return "/orders/ORD-111111-" + orderId;
    }

    private static Stream<Arguments> certificateTestParameters() {
        return Stream.of(
                Arguments.of("/fixtures/certified-certificate.json", "Order containing one certificate"),
                Arguments.of("/fixtures/multi-certified-certificate.json", "Order containing multiple certificates")
        );
    }

    private static Stream<Arguments> certifiedCopyTestParameters() {
        return Stream.of(
                Arguments.of("/fixtures/certified-copy.json", "Order containing one certified copy"),
                Arguments.of("/fixtures/multi-certified-copy.json", "Order containing multiple certified copies")
        );
    }

    private static String getNestedStringValue(final JsonNode node, final String key) {
        return node.findValuesAsString(key) != null &&
                node.findValuesAsString(key).size() == 1 ?
                node.findValuesAsString(key).getFirst() : "";
    }

    private static String getStringValue(final JsonNode node, final String key) {
        return node.get(key) != null ? node.get(key).stringValue() : "";
    }

    private static void assertItemGroupOrderedMessageIsAsExpected(final ItemGroupOrdered message,
                                                                  final String orderId,
                                                                  final String itemId) {
        assertThat(message.getOrderId(), is(notNullValue()));
        assertThat(message.getOrderId(), is(orderId));
        assertThat(message.getItems(), is(notNullValue()));
        assertThat(message.getItems().size(), is(1));
        assertThat(message.getItems().getFirst().getId(), is(itemId));
        assertThat(message.getDeliveryDetails(), is(nullValue()));
    }
}