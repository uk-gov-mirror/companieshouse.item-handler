package uk.gov.companieshouse.itemhandler.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.email.EmailSend;
import uk.gov.companieshouse.itemhandler.email.OrderConfirmation;
import uk.gov.companieshouse.itemhandler.exception.ServiceException;
import uk.gov.companieshouse.itemhandler.kafka.EmailSendMessageProducer;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.itemhandler.mapper.OrderDataToCertificateOrderConfirmationMapper;
import uk.gov.companieshouse.itemhandler.mapper.OrderDataToCertifiedCopyOrderConfirmationMapper;
import uk.gov.companieshouse.itemhandler.mapper.OrderDataToMissingImageDeliveryOrderConfirmationMapper;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.kafka.exceptions.SerializationException;
import uk.gov.companieshouse.logging.Logger;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Communicates with <code>chs-email-sender</code> via the <code>send-email</code> Kafka topic to
 * trigger the sending of emails.
 */
@Service
public class EmailService {

    private static final Logger LOGGER = LoggingUtils.getLogger();

    private static final String CERTIFICATE_ORDER_NOTIFICATION_API_APP_ID =
            "item-handler.certificate-order-confirmation";
    private static final String CERTIFICATE_ORDER_NOTIFICATION_API_MESSAGE_TYPE =
            "certificate_order_confirmation_email";
    private static final String CERTIFIED_COPY_ORDER_NOTIFICATION_API_APP_ID =
            "item-handler.certified-copy-order-confirmation";
    private static final String CERTIFIED_COPY_ORDER_NOTIFICATION_API_MESSAGE_TYPE =
            "certified_copy_order_confirmation_email";
    private static final String MISSING_IMAGE_DELIVERY_ORDER_NOTIFICATION_API_APP_ID =
            "item-handler.missing-image-delivery-order-confirmation";
    private static final String MISSING_IMAGE_DELIVERY_ORDER_NOTIFICATION_API_MESSAGE_TYPE =
            "missing_image_delivery_order_confirmation_email";
    private static final String ITEM_TYPE_CERTIFICATE = "certificate";
    private static final String ITEM_TYPE_CERTIFIED_COPY = "certified-copy";
    private static final String ITEM_TYPE_MISSING_IMAGE_DELIVERY = "missing-image-delivery";

    /**
     * This email address is supplied only to satisfy Avro contract.
     */
    private static final String TOKEN_EMAIL_ADDRESS = "chs-orders@ch.gov.uk";

    private final OrderDataToCertificateOrderConfirmationMapper orderToCertificateOrderConfirmationMapper;
    private final OrderDataToCertifiedCopyOrderConfirmationMapper orderToCertifiedCopyOrderConfirmationMapper;
    private final OrderDataToMissingImageDeliveryOrderConfirmationMapper orderDataToMissingImageDeliveryOrderConfirmationMapper;
    private final ObjectMapper objectMapper;
    private final EmailSendMessageProducer producer;

    @Value("${certificate.order.confirmation.recipient}")
    private String certificateOrderRecipient;
    @Value("${certified-copy.order.confirmation.recipient}")
    private String certifiedCopyOrderRecipient;
    @Value("${missing-image-delivery.order.confirmation.recipient}")
    private String missingImageDeliveryOrderRecipient;

    public EmailService(
            final OrderDataToCertificateOrderConfirmationMapper orderToConfirmationMapper,
            final OrderDataToCertifiedCopyOrderConfirmationMapper orderToCertifiedCopyOrderConfirmationMapper,
            final OrderDataToMissingImageDeliveryOrderConfirmationMapper orderDataToMissingImageDeliveryOrderConfirmationMapper,
            final ObjectMapper objectMapper, final EmailSendMessageProducer producer) {
        this.orderToCertificateOrderConfirmationMapper = orderToConfirmationMapper;
        this.orderToCertifiedCopyOrderConfirmationMapper = orderToCertifiedCopyOrderConfirmationMapper;
        this.orderDataToMissingImageDeliveryOrderConfirmationMapper
                 = orderDataToMissingImageDeliveryOrderConfirmationMapper;
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
    public void sendOrderConfirmation(final OrderData order)
            throws JsonProcessingException, InterruptedException, ExecutionException, SerializationException {
        String descriptionId = order.getItems().get(0).getDescriptionIdentifier();
        OrderConfirmation confirmation = getOrderConfirmation(order);
        final EmailSend email = new EmailSend();

        switch (descriptionId) {
            case ITEM_TYPE_CERTIFICATE:
                confirmation.setTo(certificateOrderRecipient);
                email.setAppId(CERTIFICATE_ORDER_NOTIFICATION_API_APP_ID);
                email.setMessageType(CERTIFICATE_ORDER_NOTIFICATION_API_MESSAGE_TYPE);
                break;
            case ITEM_TYPE_CERTIFIED_COPY:
                confirmation.setTo(certifiedCopyOrderRecipient);
                email.setAppId(CERTIFIED_COPY_ORDER_NOTIFICATION_API_APP_ID);
                email.setMessageType(CERTIFIED_COPY_ORDER_NOTIFICATION_API_MESSAGE_TYPE);
                break;
            case ITEM_TYPE_MISSING_IMAGE_DELIVERY:
                confirmation.setTo(missingImageDeliveryOrderRecipient);
                email.setAppId(MISSING_IMAGE_DELIVERY_ORDER_NOTIFICATION_API_APP_ID);
                email.setMessageType(MISSING_IMAGE_DELIVERY_ORDER_NOTIFICATION_API_MESSAGE_TYPE);
                break;
            default:
                final Map<String, Object> logMap = LoggingUtils.createLogMapWithOrderReference(order.getReference());
                final String error = "Unable to determine order confirmation type from description ID " +
                        descriptionId + "!";
                LOGGER.error(error, logMap);
                throw new ServiceException(error);
        }

        email.setEmailAddress(TOKEN_EMAIL_ADDRESS);
        email.setMessageId(UUID.randomUUID().toString());
        email.setData(objectMapper.writeValueAsString(confirmation));
        email.setCreatedAt(LocalDateTime.now().toString());

        String orderReference = confirmation.getOrderReferenceNumber();
        LoggingUtils.logWithOrderReference("Sending confirmation email for order", orderReference);
        producer.sendMessage(email, orderReference);
    }

    private OrderConfirmation getOrderConfirmation(final OrderData order) {
        String descriptionId = order.getItems().get(0).getDescriptionIdentifier();
        switch (descriptionId) {
            case ITEM_TYPE_CERTIFICATE:
                return orderToCertificateOrderConfirmationMapper.orderToConfirmation(order);
            case ITEM_TYPE_CERTIFIED_COPY:
                return orderToCertifiedCopyOrderConfirmationMapper.orderToConfirmation(order);
            case ITEM_TYPE_MISSING_IMAGE_DELIVERY:
                return orderDataToMissingImageDeliveryOrderConfirmationMapper.orderToConfirmation(order);
            default:
                final Map<String, Object> logMap = LoggingUtils.createLogMapWithOrderReference(order.getReference());
                final String error = "Unable to determine order confirmation type from description ID " +
                        descriptionId + "!";
                LOGGER.error(error, logMap);
                throw new ServiceException(error);
        }

    }
}
