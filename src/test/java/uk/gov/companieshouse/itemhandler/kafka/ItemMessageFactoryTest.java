package uk.gov.companieshouse.itemhandler.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.itemhandler.exception.KafkaMessagingException;
import uk.gov.companieshouse.itemhandler.itemsummary.OrderItemPair;
import uk.gov.companieshouse.itemhandler.model.ActionedBy;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.ItemLinks;
import uk.gov.companieshouse.itemhandler.model.MissingImageDeliveryItemOptions;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.kafka.message.Message;
import uk.gov.companieshouse.kafka.serialization.AvroSerializer;
import uk.gov.companieshouse.kafka.serialization.SerializerFactory;
import uk.gov.companieshouse.orders.items.ChdItemOrdered;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.itemhandler.util.TestConstants.MISSING_IMAGE_DELIVERY_ITEM_ID;
import static uk.gov.companieshouse.itemhandler.util.TestConstants.ORDER_REFERENCE;

/**
 * Unit tests the {@link ItemMessageFactory} class.
 */
@ExtendWith(MockitoExtension.class)
class ItemMessageFactoryTest {

    private static final byte[] MESSAGE_CONTENT = new byte[]{};

    @InjectMocks
    private ItemMessageFactory factoryUnderTest;

    @Mock
    private SerializerFactory serializerFactory;

    @Mock
    private Item item;

    @Mock
    private OrderData order;

    @Mock
    private MissingImageDeliveryItemOptions options;

    @Mock
    private ObjectMapper mapper;

    @Mock
    private ItemLinks itemLinks;

    @Mock
    private ActionedBy actionedBy;

    @Mock
    private AvroSerializer<ChdItemOrdered> serializer;

    @Mock
    private OrderItemPair orderItemPair;

    @Test
    @DisplayName("createMessage is able to create a message from a ChdItemOrdered object")
    void createMessageCreatesMessageFromOrder() throws Exception {

        // Given
        when(order.getOrderedAt()).thenReturn(LocalDateTime.now());
        when(order.getOrderedBy()).thenReturn(actionedBy);
        when(item.getItemOptions()).thenReturn(options);
        when(options.getFilingHistoryDescriptionValues()).thenReturn(new HashMap<>());
        when(item.getLinks()).thenReturn(itemLinks);
        when(orderItemPair.getOrder()).thenReturn(order);
        when(orderItemPair.getItem()).thenReturn(item);

        when(serializerFactory.getGenericRecordSerializer(ChdItemOrdered.class)).thenReturn(serializer);
        when(serializer.toBinary(any(ChdItemOrdered.class))).thenReturn(MESSAGE_CONTENT);

        final LocalDateTime intervalStart = LocalDateTime.now();

        // When
        final Message message = factoryUnderTest.createMessage(orderItemPair);

        final LocalDateTime intervalEnd = LocalDateTime.now();

        // Then
        verify(serializerFactory).getGenericRecordSerializer(ChdItemOrdered.class);
        verify(serializer).toBinary(any(ChdItemOrdered.class));

        assertThat(message, is(notNullValue()));
        assertThat(message.getValue(), is(MESSAGE_CONTENT));
        assertThat(message.getTopic(), is("chd-item-ordered"));
        assertThat(isWithinExecutionInterval(message.getTimestamp(), intervalStart, intervalEnd), is(true));

    }

    @Test
    @DisplayName("createMessage throws non-retryable KafkaMessagingException when input field is null")
    void createMessageThrowsNonRetryableExceptionIfFieldIsNull() {

        // Given
        when(order.getOrderedAt()).thenReturn(LocalDateTime.now());
        when(order.getOrderedBy()).thenReturn(actionedBy);
        when(order.getReference()).thenReturn(ORDER_REFERENCE);
        when(item.getItemOptions()).thenReturn(options);
        when(item.getId()).thenReturn(MISSING_IMAGE_DELIVERY_ITEM_ID);
        when(options.getFilingHistoryDescriptionValues()).thenReturn(new HashMap<>());
        when(orderItemPair.getOrder()).thenReturn(order);
        when(orderItemPair.getItem()).thenReturn(item);

        // When and then
        Exception exception = assertThrows(KafkaMessagingException.class, () -> {
            factoryUnderTest.createMessage(orderItemPair);
        });
        assertEquals(
                "Unable to create ChdItemOrdered message for order ORD-432118-793830 item ID MID-242116-007650!",
                exception.getMessage());
    }

    /**
     * Determines whether the timestamp is within the interval between the start and end times.
     * @param timestamp the timestamp
     * @param intervalStart the interval start - roughly the start of the test
     * @param intervalEnd the interval end - roughly the end of the test
     * @return  whether the timestamp is within the interval (<code>true</code>) or not (<code>false</code>)
     */
    private boolean isWithinExecutionInterval(final long timestamp,
                                              final LocalDateTime intervalStart,
                                              final LocalDateTime intervalEnd) {
        final LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                TimeZone.getDefault().toZoneId());
        final boolean isOnOrAfterStart = time.isAfter(intervalStart) || time.isEqual(intervalStart);
        final boolean isBeforeOrOnEnd = time.isBefore(intervalEnd) || time.isEqual(intervalEnd);
        return isOnOrAfterStart && isBeforeOrOnEnd;
    }

}
