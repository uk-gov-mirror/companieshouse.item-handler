package uk.gov.companieshouse.itemhandler.itemsummary;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.itemhandler.exception.NonRetryableException;
import uk.gov.companieshouse.itemhandler.model.CertificateItemOptions;
import uk.gov.companieshouse.itemhandler.model.CertificateType;
import uk.gov.companieshouse.itemhandler.model.DeliveryDetails;
import uk.gov.companieshouse.itemhandler.model.DeliveryTimescale;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.OrderData;

@ExtendWith(MockitoExtension.class)
class CertificateConfirmationMapperTest {

    @InjectMocks
    private CertificateConfirmationMapper mapper;

    @Mock
    private EmailConfig config;

    @Mock
    private CertificateEmailConfig certificateEmailConfig;

    @Mock
    private DeliverableItemGroup deliverableItemGroup;

    @Mock
    private OrderData order;

    @Mock
    private DeliveryDetails deliveryDetails;

    @Mock
    private Item item;

    @Mock
    private CertificateItemOptions itemOptions;

    @Test
    @DisplayName("Map certificates with standard delivery requested to CertificateEmailData object")
    void testMapStandardDeliveryCertificatesToCertificateEmailData() {
        // given
        when(deliverableItemGroup.getOrder()).thenReturn(order);
        when(config.getCertificate()).thenReturn(certificateEmailConfig);
        when(config.getOrdersAdminHost()).thenReturn("host");
        when(certificateEmailConfig.getRecipient()).thenReturn("example@companieshouse.gov.uk");
        when(certificateEmailConfig.getStandardSubjectLine()).thenReturn("subject");
        when(order.getDeliveryDetails()).thenReturn(deliveryDetails);
        when(order.getReference()).thenReturn("ORD-123123-123123");
        when(order.getOrderedAt()).thenReturn(LocalDateTime.of(2022, 8,25, 15, 18));
        when(deliverableItemGroup.getItems()).thenReturn(Collections.singletonList(item));
        when(item.getId()).thenReturn("CRT-123123-123123");
        when(item.getItemOptions()).thenReturn(itemOptions);
        when(itemOptions.getCertificateType()).thenReturn(CertificateType.INCORPORATION_WITH_ALL_NAME_CHANGES);
        when(item.getCompanyNumber()).thenReturn("12345678");
        when(item.getQuantity()).thenReturn(1);
        when(item.getTotalItemCost()).thenReturn("15");
        when(order.getPaymentReference()).thenReturn("payment reference");
        when(deliverableItemGroup.getTimescale()).thenReturn(DeliveryTimescale.STANDARD);
        when(order.getTotalOrderCost()).thenReturn("10");

        // when
        EmailMetadata<CertificateEmailData> emailMetadata = mapper.map(deliverableItemGroup);

        // then
        assertThat(emailMetadata.getEmailData(), is(equalTo(CertificateEmailData.builder()
                                                        .withTo("example@companieshouse.gov.uk")
                                                        .withSubject("subject")
                                                        .withOrderReference("ORD-123123-123123")
                                                        .withDeliveryDetails(deliveryDetails)
                                                        .withPaymentDetails(new PaymentDetails("payment reference", "25 August 2022 - 15:18:00"))
                                                        .addCertificate(new CertificateSummary("CRT-123123-123123",
                                                                "Incorporation with all company name changes",
                                                                "12345678",
                                                                1,
                                                                "£15",
                                                                "host/orders-admin/order-summaries/ORD-123123-123123/items/CRT-123123-123123"))
                                                        .build())));
        assertThat(emailMetadata.getAppId(), is("item-handler.certificate-summary-order-confirmation"));
        assertThat(emailMetadata.getMessageType(), is("certificate_summary_order_confirmation"));
    }

    @Test
    @DisplayName("Map certificates with express delivery requested to CertificateEmailData object")
    void testMapExpressDeliveryCertificatesToCertificateEmailData() {
        // given
        when(deliverableItemGroup.getOrder()).thenReturn(order);
        when(config.getCertificate()).thenReturn(certificateEmailConfig);
        when(config.getOrdersAdminHost()).thenReturn("host");
        when(certificateEmailConfig.getRecipient()).thenReturn("example@companieshouse.gov.uk");
        when(certificateEmailConfig.getExpressSubjectLine()).thenReturn("subject");
        when(order.getDeliveryDetails()).thenReturn(deliveryDetails);
        when(order.getReference()).thenReturn("ORD-123123-123123");
        when(order.getOrderedAt()).thenReturn(LocalDateTime.of(2022, 8,25, 15, 18));
        when(deliverableItemGroup.getItems()).thenReturn(Collections.singletonList(item));
        when(item.getId()).thenReturn("CRT-123123-123123");
        when(item.getItemOptions()).thenReturn(itemOptions);
        when(itemOptions.getCertificateType()).thenReturn(CertificateType.DISSOLUTION);
        when(item.getCompanyNumber()).thenReturn("12345678");
        when(item.getQuantity()).thenReturn(1);
        when(item.getTotalItemCost()).thenReturn("15");
        when(order.getPaymentReference()).thenReturn("payment reference");
        when(deliverableItemGroup.getTimescale()).thenReturn(DeliveryTimescale.SAME_DAY);
        when(order.getTotalOrderCost()).thenReturn("10");

        // when
        EmailMetadata<CertificateEmailData> emailMetadata = mapper.map(deliverableItemGroup);

        // then
        assertThat(emailMetadata.getEmailData(), is(equalTo(CertificateEmailData.builder()
                .withTo("example@companieshouse.gov.uk")
                .withSubject("subject")
                .withOrderReference("ORD-123123-123123")
                .withDeliveryDetails(deliveryDetails)
                .withPaymentDetails(new PaymentDetails("payment reference", "25 August 2022 - 15:18:00"))
                .addCertificate(new CertificateSummary("CRT-123123-123123",
                        "Dissolution with all company name changes",
                        "12345678",
                        1,
                        "£15",
                        "host/orders-admin/order-summaries/ORD-123123-123123/items/CRT-123123-123123"))
                .build())));
        assertThat(emailMetadata.getAppId(), is("item-handler.certificate-summary-order-confirmation"));
        assertThat(emailMetadata.getMessageType(), is("certificate_summary_order_confirmation"));
    }

    @Test
    @DisplayName("Map multiple certificate items to CertificateEmailData object")
    void testMapMultipleCertificatesToCertificateEmailData() {
        // given
        when(deliverableItemGroup.getOrder()).thenReturn(order);
        when(config.getCertificate()).thenReturn(certificateEmailConfig);
        when(config.getOrdersAdminHost()).thenReturn("host");
        when(certificateEmailConfig.getRecipient()).thenReturn("example@companieshouse.gov.uk");
        when(certificateEmailConfig.getStandardSubjectLine()).thenReturn("subject");
        when(order.getDeliveryDetails()).thenReturn(deliveryDetails);
        when(order.getReference()).thenReturn("ORD-123123-123123");
        when(order.getOrderedAt()).thenReturn(LocalDateTime.of(2022, 8,25, 15, 18));
        when(deliverableItemGroup.getItems()).thenReturn(Arrays.asList(item, item));
        when(item.getId()).thenReturn("CRT-123123-123123", "CRT-123123-123123", "CRT-123123-123124", "CRT-123123-123124");
        when(item.getItemOptions()).thenReturn(itemOptions);
        when(itemOptions.getCertificateType()).thenReturn(CertificateType.INCORPORATION_WITH_ALL_NAME_CHANGES, CertificateType.DISSOLUTION);
        when(item.getCompanyNumber()).thenReturn("12345678", "87654321");
        when(item.getQuantity()).thenReturn(1);
        when(item.getTotalItemCost()).thenReturn("15", "50");
        when(order.getPaymentReference()).thenReturn("payment reference");
        when(deliverableItemGroup.getTimescale()).thenReturn(DeliveryTimescale.STANDARD);
        when(order.getTotalOrderCost()).thenReturn("10");

        // when
        EmailMetadata<CertificateEmailData> emailMetadata = mapper.map(deliverableItemGroup);

        // then
        assertThat(emailMetadata.getEmailData(), is(equalTo(CertificateEmailData.builder()
                .withTo("example@companieshouse.gov.uk")
                .withSubject("subject")
                .withOrderReference("ORD-123123-123123")
                .withDeliveryDetails(deliveryDetails)
                .withPaymentDetails(new PaymentDetails("payment reference", "25 August 2022 - 15:18:00"))
                .addCertificate(new CertificateSummary("CRT-123123-123123",
                        "Incorporation with all company name changes",
                        "12345678",
                        1,
                        "£15",
                        "host/orders-admin/order-summaries/ORD-123123-123123/items/CRT-123123-123123"))
                .addCertificate(new CertificateSummary("CRT-123123-123124",
                        "Dissolution with all company name changes",
                        "87654321",
                        1,
                        "£50",
                        "host/orders-admin/order-summaries/ORD-123123-123123/items/CRT-123123-123124"))
                .build())));
        assertThat(emailMetadata.getAppId(), is("item-handler.certificate-summary-order-confirmation"));
        assertThat(emailMetadata.getMessageType(), is("certificate_summary_order_confirmation"));
    }

    @Test
    @DisplayName("Throw NonRetryableException if certificate type unhandled")
    void testThrowNonRetryableExceptionIfCertificateTypeUnhandled() {
        // given
        when(deliverableItemGroup.getOrder()).thenReturn(order);
        when(config.getCertificate()).thenReturn(certificateEmailConfig);
        when(certificateEmailConfig.getRecipient()).thenReturn("example@companieshouse.gov.uk");
        when(certificateEmailConfig.getExpressSubjectLine()).thenReturn("subject");
        when(order.getDeliveryDetails()).thenReturn(deliveryDetails);
        when(order.getReference()).thenReturn("ORD-123123-123123");
        when(order.getOrderedAt()).thenReturn(LocalDateTime.of(2022, 8,25, 15, 18));
        when(deliverableItemGroup.getItems()).thenReturn(Collections.singletonList(item));
        when(item.getId()).thenReturn("CRT-123123-123123");
        when(item.getItemOptions()).thenReturn(itemOptions);
        when(itemOptions.getCertificateType()).thenReturn(CertificateType.INCORPORATION);
        when(order.getPaymentReference()).thenReturn("payment reference");
        when(deliverableItemGroup.getTimescale()).thenReturn(DeliveryTimescale.SAME_DAY);
        when(order.getTotalOrderCost()).thenReturn("10");

        // when
        Executable executable = () -> mapper.map(deliverableItemGroup);

        // then
        NonRetryableException exception = assertThrows(NonRetryableException.class, executable);
        assertThat(exception.getMessage(), is(equalTo("Unhandled certificate type: [INCORPORATION]")));
    }

    @Test
    @DisplayName("Ensures the correct email subject is set when total order is 0 (admin free order)")
    void testMapFreeCertificateToCertificateEmailData() {
        // given
        when(deliverableItemGroup.getOrder()).thenReturn(order);
        when(config.getCertificate()).thenReturn(certificateEmailConfig);
        when(config.getOrdersAdminHost()).thenReturn("host");
        when(certificateEmailConfig.getRecipient()).thenReturn("example@companieshouse.gov.uk");
        when(certificateEmailConfig.getStandardSubjectLine()).thenReturn("subject");
        when(order.getDeliveryDetails()).thenReturn(deliveryDetails);
        when(order.getReference()).thenReturn("ORD-123123-123123");
        when(order.getOrderedAt()).thenReturn(LocalDateTime.of(2022, 8,25, 15, 18));
        when(deliverableItemGroup.getItems()).thenReturn(Collections.singletonList(item));
        when(item.getId()).thenReturn("CRT-123123-123123");
        when(item.getItemOptions()).thenReturn(itemOptions);
        when(itemOptions.getCertificateType()).thenReturn(CertificateType.INCORPORATION_WITH_ALL_NAME_CHANGES);
        when(item.getCompanyNumber()).thenReturn("12345678");
        when(item.getQuantity()).thenReturn(1);
        when(item.getTotalItemCost()).thenReturn("0");
        when(order.getPaymentReference()).thenReturn("payment reference");
        when(deliverableItemGroup.getTimescale()).thenReturn(DeliveryTimescale.STANDARD);
        when(order.getTotalOrderCost()).thenReturn("0");

        // when
        EmailMetadata<CertificateEmailData> emailMetadata = mapper.map(deliverableItemGroup);

        // then
        assertThat(emailMetadata.getEmailData(), is(equalTo(CertificateEmailData.builder()
                .withTo("example@companieshouse.gov.uk")
                .withSubject("subject - [FREE CERTIFICATE ADMIN ORDER]")
                .withOrderReference("ORD-123123-123123")
                .withDeliveryDetails(deliveryDetails)
                .withPaymentDetails(new PaymentDetails("payment reference", "25 August 2022 - 15:18:00"))
                .addCertificate(new CertificateSummary("CRT-123123-123123",
                        "Incorporation with all company name changes",
                        "12345678",
                        1,
                        "£0",
                        "host/orders-admin/order-summaries/ORD-123123-123123/items/CRT-123123-123123"))
                .build())));
        assertThat(emailMetadata.getAppId(), is("item-handler.certificate-summary-order-confirmation"));
        assertThat(emailMetadata.getMessageType(), is("certificate_summary_order_confirmation"));
    }
}
