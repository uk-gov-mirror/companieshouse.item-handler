package uk.gov.companieshouse.itemhandler.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.kafka.message.Message;
import uk.gov.companieshouse.kafka.producer.ProducerConfig;
import uk.gov.companieshouse.logging.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static uk.gov.companieshouse.itemhandler.logging.LoggingUtils.logIfNotNull;

@Service
public class ItemKafkaProducer extends KafkaProducer {

    private static final Logger LOGGER = LoggingUtils.getLogger();

    /**
     * TODO GCI-1428 Javadoc this
     * Sends message to Kafka topic
     * @param orderUri
     * @param itemId
     * @param message
     * @param asyncResponseLogger
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Async
    public void sendMessage(final String orderUri,
                            final String itemId,
                            final Message message,
                            final Consumer<RecordMetadata> asyncResponseLogger)
            throws ExecutionException, InterruptedException {

        final Map<String, Object> logMap = LoggingUtils.createLogMap();
        logIfNotNull(logMap, LoggingUtils.ORDER_URI, orderUri);
        logIfNotNull(logMap, LoggingUtils.ITEM_ID, itemId);
        LOGGER.info("Sending message to kafka topic", logMap);

        final Future<RecordMetadata> recordMetadataFuture = getChKafkaProducer().sendAndReturnFuture(message);
        asyncResponseLogger.accept(recordMetadataFuture.get());
    }

    @Override
    protected void modifyProducerConfig(final ProducerConfig producerConfig) {
        producerConfig.setMaxBlockMilliseconds(10000);
    }
}
