package uk.gov.companieshouse.itemhandler.service;

import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import uk.gov.companieshouse.itemhandler.exception.ServiceException;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.ItemType;
import uk.gov.companieshouse.itemhandler.model.OrderData;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Unit tests the {@link OrderRouterService} class.
 */
@RunWith(PowerMockRunner.class)
@ExtendWith(MockitoExtension.class)
// TODO GCI-1300 ? @PrepareForTest(HttpResponseException.class)
class OrderRouterServiceTest {

    private static final String ORDER_REFERENCE = "ORD-432118-793830";
    private static final String CERTIFICATE_ITEM_ID = "CRT-052815-956034";
    private static final String CERTIFICATE_KIND = ItemType.CERTIFICATE.getKind();
    private static final String UNKNOWN_KIND = "item#unknown";

    @InjectMocks
    private OrderRouterService serviceUnderTest;

    @Mock
    private OrderData order;

    @Mock
    private List<Item> items;

    @Mock
    private Item item;

    /**
     * This is a JUnit 4 test to take advantage of PowerMock.
     * @throws Exception should something unexpected happen
     */
    @org.junit.Test
    // @DisplayName("Delegates to item type to send messages")
    public void routeOrderDelegatesToItemTypeSendMessages() {

        // Given
        when(order.getItems()).thenReturn(items);
        when(items.get(0)).thenReturn(item);
        when(item.getKind()).thenReturn(CERTIFICATE_KIND);

        // When
        serviceUnderTest.getItemType(order);

        // Then
        // TODO GCI-1300 Make assertions

    }


    @Test
    @DisplayName("Propagates exception to client")
    void routeOrderPropagatesGetItemTypeException() {

        // Given
        when(order.getReference()).thenReturn(ORDER_REFERENCE);

        // When and then
        assertThatExceptionOfType(ServiceException.class).isThrownBy(() ->
                serviceUnderTest.routeOrder(order))
                .withMessage("Order ORD-432118-793830 contains no items.")
                .withNoCause();

    }


    @Test
    @DisplayName("Infers item type correctly from first item kind")
    void getItemTypeInfersItemTypeFromFirstItemKindCorrectly() {

        // Given
        when(order.getItems()).thenReturn(items);
        when(items.get(0)).thenReturn(item);
        when(item.getKind()).thenReturn(CERTIFICATE_KIND);

        // When
        final ItemType type = serviceUnderTest.getItemType(order);

        // Then
        assertThat(type, is (ItemType.CERTIFICATE));

    }

    @Test
    @DisplayName("Throws non-retryable exception if order contains no items")
    void getItemTypeThrowsNonRetryableExceptionIfOrderContainsNoItems() {

        // Given
        when(order.getReference()).thenReturn(ORDER_REFERENCE);

        // When and then
        thenNonRetryableExceptionIsThrownWithMessage("Order ORD-432118-793830 contains no items.");

    }

    @Test
    @DisplayName("Throws non-retryable exception if item kind is unknown")
    void getItemTypeThrowsNonRetryableExceptionIfKindIsUnknown() {

        // Given
        when(order.getReference()).thenReturn(ORDER_REFERENCE);
        when(order.getItems()).thenReturn(items);
        when(items.get(0)).thenReturn(item);
        when(item.getId()).thenReturn(CERTIFICATE_ITEM_ID);
        when(item.getKind()).thenReturn(UNKNOWN_KIND);

        // When and then
        thenNonRetryableExceptionIsThrownWithMessage(
                "Kind item#unknown on item CRT-052815-956034 on order ORD-432118-793830 is unknown.");

    }

    /**
     * Asserts that an exception of type {@link ServiceException} is thrown with the expected message.
     * @param exceptionMessage the expected exception message
     */
    private void thenNonRetryableExceptionIsThrownWithMessage(final String exceptionMessage) {
        assertThatExceptionOfType(ServiceException.class).isThrownBy(() ->
                serviceUnderTest.getItemType(order))
                .withMessage(exceptionMessage)
                .withNoCause();
    }

}
