package uk.gov.companieshouse.itemhandler.kafka;

import static java.time.LocalDateTime.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.itemhandler.exception.KafkaMessagingException;
import uk.gov.companieshouse.itemhandler.itemsummary.ItemGroup;
import uk.gov.companieshouse.itemhandler.model.ActionedBy;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;

import java.util.Collections;

/**
 * Unit tests {@link ItemGroupOrderedFactory}.
 */
@ExtendWith(MockitoExtension.class)
class ItemGroupOrderedFactoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger("ItemGroupOrderedFactoryTest");

    @Mock
    private ObjectMapper mapper;

    @Mock
    private ItemGroup digitalItemGroup;

    @Mock
    private OrderData order;

    @Mock
    private Item digitalItem;

    @Mock
    private ActionedBy customer;

    @Test
    @DisplayName("createMessage() propagates error as expected KafkaMessagingException")
    void testCreateMessagePropagatesException() {

        // Given
        final ItemGroupOrderedFactory factory = new ItemGroupOrderedFactory(LOGGER, mapper);
        when(digitalItemGroup.getOrder()).thenReturn(order);
        when(digitalItemGroup.getItems()).thenReturn(Collections.singletonList(digitalItem));
        when(order.getReference()).thenReturn("ORD-123123-123123");
        when(digitalItem.getId()).thenReturn("CRT-123123-123123");
        when(order.getOrderedAt()).thenReturn(now());
        when(order.getOrderedBy()).thenReturn(customer);

        // When and then
        final KafkaMessagingException exception = assertThrows(KafkaMessagingException.class,
                () -> factory.createMessage(digitalItemGroup));
        assertThat(exception.getMessage(), is("Unable to create ItemGroupOrdered message for order " +
                "ORD-123123-123123 item ID CRT-123123-123123! " +
                "Error: Field payment_reference type:STRING pos:4 does not accept null values"));

    }

}