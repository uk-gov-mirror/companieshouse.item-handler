package uk.gov.companieshouse.itemhandler.service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.email.EmailSend;
import uk.gov.companieshouse.itemhandler.email.CertificateOrderConfirmation;
import uk.gov.companieshouse.itemhandler.kafka.EmailSendMessageProducer;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.itemhandler.mapper.OrderDataToCertificateOrderConfirmationMapper;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.kafka.exceptions.SerializationException;

/**
 * Communicates with <code>chs-email-sender</code> via the ` <code>send-email</code> Kafka topic to
 * trigger the sending of emails.
 */
@Service
public class EmailService {

    private static final String NOTIFICATION_API_APP_ID =
            "item-handler.certificate-order-confirmation";
    private static final String NOTIFICATION_API_MESSAGE_TYPE =
            "certificate_order_confirmation_email";
    /** This email address is supplied only to satisfy Avro contract. */
    private static final String TOKEN_EMAIL_ADDRESS = "chs-orders@ch.gov.uk";

    private final OrderDataToCertificateOrderConfirmationMapper orderToConfirmationMapper;
    private final ObjectMapper objectMapper;
    private final EmailSendMessageProducer producer;

    @Value("${certificate.order.confirmation.recipient}")
    private String recipient;

    public EmailService(
            final OrderDataToCertificateOrderConfirmationMapper orderToConfirmationMapper,
            final ObjectMapper objectMapper, final EmailSendMessageProducer producer) {
        this.orderToConfirmationMapper = orderToConfirmationMapper;
        this.objectMapper = objectMapper;
        this.producer = producer;
    }

    /**
     * Sends out a certificate order confirmation email.
     * 
     * @param order the order information used to compose the order confirmation email.
     * @throws JsonProcessingException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SerializationException
     */
    public void sendCertificateOrderConfirmation(final OrderData order)
            throws JsonProcessingException, InterruptedException, ExecutionException,
            SerializationException {
        final CertificateOrderConfirmation confirmation =
                orderToConfirmationMapper.orderToConfirmation(order);
        confirmation.setTo(recipient);
        LoggingUtils.logWithOrderReference("Sending confirmation email for order",
                confirmation.getOrderReferenceNumber());

        final EmailSend email = new EmailSend();
        email.setAppId(NOTIFICATION_API_APP_ID);
        email.setEmailAddress(TOKEN_EMAIL_ADDRESS);
        email.setMessageId(UUID.randomUUID().toString());
        email.setMessageType(NOTIFICATION_API_MESSAGE_TYPE);
        email.setData(objectMapper.writeValueAsString(confirmation));
        email.setCreatedAt(LocalDateTime.now().toString());

        producer.sendMessage(email, confirmation.getOrderReferenceNumber());
    }

}
