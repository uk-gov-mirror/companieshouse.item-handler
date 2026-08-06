package uk.gov.companieshouse.itemhandler.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.email.EmailSend;
import uk.gov.companieshouse.itemhandler.client.EmailClient;
import uk.gov.companieshouse.itemhandler.exception.NonRetryableException;
import uk.gov.companieshouse.itemhandler.itemsummary.*;
import uk.gov.companieshouse.itemhandler.model.OrderData;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static uk.gov.companieshouse.itemhandler.model.DeliveryTimescale.STANDARD;

/** Unit tests the {@link EmailService} class. */
@ExtendWith(MockitoExtension.class)
class EmailServiceTest {

    private static final String TEST_EXCEPTION_MESSAGE = "Test message!";

    @InjectMocks
    private EmailService emailServiceUnderTest;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private EmailClient emailClient;

    @Mock
    private OrderData order;

    @Captor
    private ArgumentCaptor<EmailSend> emailCaptor;

    @Mock
    private ConfirmationMapperFactory confirmationMapperFactory;

    @Mock
    private OrderConfirmationMapper<CertificateEmailData> certificateConfirmationMapper;

    @Mock
    private OrderConfirmationMapper<CertifiedCopyEmailData> certifiedCopyConfirmationMapper;

    @Mock
    private CertificateEmailData data;

    @Mock
    private EmailMetadata<CertificateEmailData> metadata;

    @Mock
    private CertifiedCopyEmailData certCopyData;

    @Mock
    private EmailMetadata<CertifiedCopyEmailData> certCopyMetadata;

    /** Extends {@link JacksonException} so it can be instantiated in these tests. */
    private static class TestJsonProcessingException extends JacksonException {

        protected TestJsonProcessingException(String msg) {
            super(msg);
        }
    }

    @Test
    @DisplayName("Email service handles certified certificate emails correctly")
    void serviceCallsMapMethodOnCertificateConfirmationMapper() throws JacksonException {
        // given
        when(confirmationMapperFactory.getCertificateMapper()).thenReturn(certificateConfirmationMapper);
        when(certificateConfirmationMapper.map(any())).thenReturn(metadata);
        when(metadata.getEmailData()).thenReturn(data);
        when(metadata.getAppId()).thenReturn("appId");
        when(metadata.getMessageType()).thenReturn("messageType");
        when(objectMapper.writeValueAsString(any())).thenReturn("data");
        when(order.getReference()).thenReturn("ORD-123123-123123");

        // when
        emailServiceUnderTest.sendOrderConfirmation(new DeliverableItemGroup(order, "item#certificate", STANDARD));

        // then
        verify(emailClient, times(1)).sendEmail(emailCaptor.capture());

        assertThat(emailCaptor.getValue().getAppId(), is(equalTo("appId")));
        assertThat(emailCaptor.getValue().getMessageType(), is(equalTo("messageType")));
        assertThat(emailCaptor.getValue().getData(), is(equalTo("data")));
        assertThat(emailCaptor.getValue().getMessageId(), is(notNullValue()));
        assertThat(emailCaptor.getValue().getCreatedAt(), is(notNullValue()));
    }

    @Test
    @DisplayName("Email service handles certified copy emails correctly")
    void serviceCallsMapMethodOnCertifiedCopyConfirmationMapper() throws JacksonException {
        // given
        when(confirmationMapperFactory.getCertifiedCopyMapper()).thenReturn(certifiedCopyConfirmationMapper);
        when(certifiedCopyConfirmationMapper.map(any())).thenReturn(certCopyMetadata);
        when(certCopyMetadata.getEmailData()).thenReturn(certCopyData);
        when(certCopyMetadata.getAppId()).thenReturn("appId");
        when(certCopyMetadata.getMessageType()).thenReturn("messageType");
        when(objectMapper.writeValueAsString(any())).thenReturn("data");
        when(order.getReference()).thenReturn("ORD-123123-123123");

        // when
        emailServiceUnderTest.sendOrderConfirmation(new DeliverableItemGroup(order, "item#certified-copy", STANDARD));

        // then
        verify(emailClient, times(1)).sendEmail(emailCaptor.capture());

        assertThat(emailCaptor.getValue().getAppId(), is(equalTo("appId")));
        assertThat(emailCaptor.getValue().getMessageType(), is(equalTo("messageType")));
        assertThat(emailCaptor.getValue().getData(), is(equalTo("data")));
        assertThat(emailCaptor.getValue().getMessageId(), is(notNullValue()));
        assertThat(emailCaptor.getValue().getCreatedAt(), is(notNullValue()));
    }

    @Test
    @DisplayName("Email service throws NonRetryableException when object mappers fails to serialise")
    void serviceThrowsNonRetryableExceptionWhenSerialisationFails() throws Exception  {
        // given
        when(confirmationMapperFactory.getCertificateMapper()).thenReturn(certificateConfirmationMapper);
        when(certificateConfirmationMapper.map(any())).thenReturn(metadata);
        when(metadata.getEmailData()).thenReturn(data);
        when(objectMapper.writeValueAsString(data)).thenThrow(new TestJsonProcessingException(TEST_EXCEPTION_MESSAGE));
        when(order.getReference()).thenReturn("ORD-123456-123456");

        // when
        Executable executable = () -> emailServiceUnderTest.sendOrderConfirmation(new DeliverableItemGroup(order, "item#certificate", STANDARD));

        // then
        NonRetryableException actual = assertThrows(NonRetryableException.class, executable);
        assertEquals("Error converting order (ORD-123456-123456) confirmation to JSON", actual.getMessage());
    }

    @Test
    @DisplayName("Email service throws NonRetryableException for non valid item kinds")
    void serviceThrowsNonRetryableExceptionWhenItemKindIsNotValid() {
        // given

        // when
        Executable executable = () -> emailServiceUnderTest.sendOrderConfirmation(new DeliverableItemGroup(order, "item#unknowntype", STANDARD));

        // then
        NonRetryableException actual = assertThrows(NonRetryableException.class, executable);
        assertEquals("Unknown item kind: [item#unknowntype]", actual.getMessage());
    }
}