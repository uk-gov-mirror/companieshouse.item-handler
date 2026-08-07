package uk.gov.companieshouse.itemhandler.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import uk.gov.companieshouse.kafka.serialization.AvroSerializer;
import uk.gov.companieshouse.kafka.serialization.SerializerFactory;
import uk.gov.companieshouse.orders.items.ChdItemOrdered;
import uk.gov.companieshouse.orders.items.DeliveryDetails;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.companieshouse.itemhandler.util.TestUtils.createAvroOrder;
import static uk.gov.companieshouse.itemhandler.util.TestUtils.createDeliveryDetails;

/**
 * Unit tests {@link uk.gov.companieshouse.kafka.serialization.AvroSerializer} for
 * {@link uk.gov.companieshouse.orders.items.ChdItemOrdered} to verify whether fields are handled as optional
 * within the ChdItemOrdered Avro schema.
 */
class ChdItemOrderedAvroSerializerTest {

    final AvroSerializer<ChdItemOrdered> serializerUnderTest =
            new SerializerFactory().getGenericRecordSerializer(ChdItemOrdered.class);

    @Test
    @DisplayName("toBinary throws a NullPointerException if mandatory field reference value is absent")
    void toBinaryNullPaymentReferencNullPointerException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.setReference(null);

        // When and then
        Exception exception = assertThrows(NullPointerException.class, () -> {
            serializerUnderTest.toBinary(order);
        });
        Assertions.assertEquals(
                "Cannot invoke \"java.lang.CharSequence.toString()\" because \"charSequence\" is null",
                exception.getCause().getMessage());
    }

    @Test
    @DisplayName("toBinary handles absence of optional payment_reference gracefully")
    void toBinaryNullPaymentReferenceThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.setPaymentReference(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional company_name gracefully")
    void toBinaryNullCompanyNameThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.getItem().setCompanyName(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional company_number gracefully")
    void toBinaryNullCompanyNumberThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.getItem().setCompanyNumber(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional customer_reference gracefully")
    void toBinaryNullCustomerReferenceThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.getItem().setCustomerReference(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional description_identifier gracefully")
    void toBinaryNullDescriptionIdentifierThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.getItem().setDescriptionIdentifier(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional description_values gracefully")
    void toBinaryNullDescriptionValuesThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.getItem().setDescriptionValues(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional discount_applied gracefully")
    void toBinaryNullDiscountAppliedThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.getItem().getItemCosts().getFirst().setDiscountApplied(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional delivery_details gracefully")
    void toBinaryNullDeliveryDetailsThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        order.setDeliveryDetails(null);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional address_line_2 gracefully")
    void toBinaryNullAddressLine2ThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        final DeliveryDetails details = createDeliveryDetails();
        details.setAddressLine2(null);
        order.setDeliveryDetails(details);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional po_box gracefully")
    void toBinaryNullPoBoxThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        final DeliveryDetails details = createDeliveryDetails();
        details.setPoBox(null);
        order.setDeliveryDetails(details);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional postal_code gracefully")
    void toBinaryNullPostalCodeThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        final DeliveryDetails details = createDeliveryDetails();
        details.setPostalCode(null);
        order.setDeliveryDetails(details);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

    @Test
    @DisplayName("toBinary handles absence of optional region gracefully")
    void toBinaryNullRegionThrowsNoException() {
        // Given
        final ChdItemOrdered order = createAvroOrder();
        final DeliveryDetails details = createDeliveryDetails();
        details.setRegion(null);
        order.setDeliveryDetails(details);

        // When and then
        assertAll(() -> serializerUnderTest.toBinary(order));
    }

}
