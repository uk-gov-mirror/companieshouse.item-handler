package uk.gov.companieshouse.itemhandler.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.email.EmailSend;
import uk.gov.companieshouse.itemhandler.client.EmailClient;
import uk.gov.companieshouse.itemhandler.exception.NonRetryableException;
import uk.gov.companieshouse.itemhandler.itemsummary.ConfirmationMapperFactory;
import uk.gov.companieshouse.itemhandler.itemsummary.DeliverableItemGroup;
import uk.gov.companieshouse.itemhandler.itemsummary.EmailMetadata;
import uk.gov.companieshouse.itemhandler.itemsummary.OrderConfirmationMapper;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.logging.Logger;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Communicates with <code>chs-email-sender</code> via the <code>send-email</code> Kafka topic to
 * trigger the sending of emails.
 */
@Service
public class EmailService {

    private static final Logger LOGGER = LoggingUtils.getLogger();
    private static final String ITEM_KIND_CERTIFIED_COPY = "item#certified-copy";
    private static final String ITEM_KIND_CERTIFICATE = "item#certificate";

    /**
     * This email address is supplied only to satisfy Avro contract.
     */
    public static final String TOKEN_EMAIL_ADDRESS = "chs-orders@ch.gov.uk";

    private final ObjectMapper objectMapper;
    private final ConfirmationMapperFactory confirmationMapperFactory;
    private final EmailClient emailClient;

    public EmailService(@Qualifier("snakeCaseMapper")final ObjectMapper objectMapper,
            final ConfirmationMapperFactory confirmationMapperFactory,
            final EmailClient emailClient) {
        this.objectMapper = objectMapper;
        this.confirmationMapperFactory = confirmationMapperFactory;
        this.emailClient = emailClient;
    }

    /**
     * Sends out a certificate or certified copy order confirmation email.
     *
     * @param itemGroup a {@link DeliverableItemGroup group of deliverable items}.
     */
    public void sendOrderConfirmation(final DeliverableItemGroup itemGroup) {
        try {
            EmailSend emailSend;
            if (ITEM_KIND_CERTIFIED_COPY.equals(itemGroup.getKind())) {
                emailSend = mapEmailSend(itemGroup, confirmationMapperFactory.getCertifiedCopyMapper());
            } else if (ITEM_KIND_CERTIFICATE.equals(itemGroup.getKind())) {
                emailSend = mapEmailSend(itemGroup, confirmationMapperFactory.getCertificateMapper());
            } else {
                throw new NonRetryableException(String.format("Unknown item kind: [%s]", itemGroup.getKind()));
            }

            String orderReference = itemGroup.getOrder().getReference();
            LoggingUtils.logWithOrderReference("Sending confirmation email for order", orderReference);

            emailClient.sendEmail(emailSend);

        } catch (JacksonException exception) {
            String msg = String.format("Error converting order (%s) confirmation to JSON", itemGroup.getOrder().getReference());
            LOGGER.error(msg, exception);
            throw new NonRetryableException(msg);
        }
    }

    private EmailSend mapEmailSend(DeliverableItemGroup itemGroup, OrderConfirmationMapper<?> mapper) {
        EmailMetadata<?> emailMetadata = mapper.map(itemGroup);

        EmailSend emailSend = new EmailSend();
        emailSend.setAppId(emailMetadata.getAppId());
        emailSend.setMessageType(emailMetadata.getMessageType());
        emailSend.setData(objectMapper.writeValueAsString(emailMetadata.getEmailData()));
        emailSend.setEmailAddress(TOKEN_EMAIL_ADDRESS);
        emailSend.setMessageId(UUID.randomUUID().toString());
        emailSend.setCreatedAt(LocalDateTime.now().toString());

        return emailSend;
    }
}