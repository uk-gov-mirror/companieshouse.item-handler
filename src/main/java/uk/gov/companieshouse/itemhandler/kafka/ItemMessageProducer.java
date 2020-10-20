package uk.gov.companieshouse.itemhandler.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.itemhandler.exception.RetryableErrorException;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.kafka.message.Message;
import uk.gov.companieshouse.logging.Logger;

import java.util.Map;

import static uk.gov.companieshouse.itemhandler.logging.LoggingUtils.ITEM_ID;
import static uk.gov.companieshouse.itemhandler.logging.LoggingUtils.ORDER_REFERENCE_NUMBER;
import static uk.gov.companieshouse.itemhandler.logging.LoggingUtils.createLogMapWithAcknowledgedKafkaMessage;
import static uk.gov.companieshouse.itemhandler.logging.LoggingUtils.logIfNotNull;

@Service
public class ItemMessageProducer {
    private static final Logger LOGGER = LoggingUtils.getLogger();
    private final ItemMessageFactory itemMessageFactory;
    private final ItemKafkaProducer itemKafkaProducer;

    public ItemMessageProducer(final ItemMessageFactory itemMessageFactory,
                               final ItemKafkaProducer itemKafkaProducer) {
        this.itemMessageFactory = itemMessageFactory;
        this.itemKafkaProducer = itemKafkaProducer;
    }

    /**
     * Sends (produces) a message to the Kafka <code>chd-item-ordered</code> topic representing the missing image
     * delivery item provided.
     * @param order the {@link OrderData} instance retrieved from the Orders API
     * @param orderReference the reference of the order to which the item belongs
     * @param itemId the ID of the item that the message to be sent represents
     */
    public void sendMessage(final OrderData order,
                            final String orderReference,
                            final String itemId) {

        final Map<String, Object> logMap = LoggingUtils.createLogMap();
        logIfNotNull(logMap, ORDER_REFERENCE_NUMBER, orderReference);
        logIfNotNull(logMap, ITEM_ID, itemId);
        LOGGER.info("Sending message to kafka producer", logMap);

        final Message message = itemMessageFactory.createMessage(order);
        try {
            itemKafkaProducer.sendMessage(orderReference, itemId, message,
                    recordMetadata ->
                            logOffsetFollowingSendIngOfMessage(orderReference, itemId, recordMetadata));
        } catch (Exception e) {
            final String errorMessage = String.format(
                    "Kafka item message could not be sent for order reference %s item ID %s", orderReference, itemId);
            logMap.put(LoggingUtils.EXCEPTION, e);
            LOGGER.error(errorMessage, logMap);
            // An error occurring during message production may be transient. Throw a retryable exception.
            throw new RetryableErrorException(errorMessage, e);
        }
    }

    /**
     * Logs the order reference, item ID, topic, partition and offset for the item message produced to a Kafka topic.
     * @param orderReference the order reference
     * @param itemId the item ID
     * @param recordMetadata the metadata for a record that has been acknowledged by the server for the message produced
     */
    void logOffsetFollowingSendIngOfMessage(final String orderReference,
                                            final String itemId,
                                            final RecordMetadata recordMetadata) {
        final Map<String, Object> logMapCallback =  createLogMapWithAcknowledgedKafkaMessage(recordMetadata);
        logIfNotNull(logMapCallback, ORDER_REFERENCE_NUMBER, orderReference);
        logIfNotNull(logMapCallback, ITEM_ID, itemId);
        LOGGER.info("Message sent to Kafka topic", logMapCallback);
    }
}
