package uk.gov.companieshouse.itemhandler.kafka;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.itemhandler.exception.KafkaMessagingException;
import uk.gov.companieshouse.itemhandler.itemsummary.OrderItemPair;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.itemhandler.model.ActionedBy;
import uk.gov.companieshouse.itemhandler.model.MissingImageDeliveryItemOptions;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.kafka.message.Message;
import uk.gov.companieshouse.kafka.serialization.AvroSerializer;
import uk.gov.companieshouse.kafka.serialization.SerializerFactory;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.orders.items.ChdItemOrdered;
import uk.gov.companieshouse.orders.items.Item;
import uk.gov.companieshouse.orders.items.ItemCosts;
import uk.gov.companieshouse.orders.items.Links;
import uk.gov.companieshouse.orders.items.OrderedBy;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Service
public class ItemMessageFactory {

    private static final Logger LOGGER = LoggingUtils.getLogger();

    private static final String CHD_ITEM_ORDERED_TOPIC = "chd-item-ordered";

    private static final String ZERO_POSTAGE_COST = "0";
    private static final boolean NO_POSTAL_DELIVERY = false;

    private static final String FILING_HISTORY_ID = "filingHistoryId";
    private static final String FILING_HISTORY_DATE = "filingHistoryDate";
    private static final String FILING_HISTORY_DESCRIPTION = "filingHistoryDescription";
    private static final String FILING_HISTORY_DESCRIPTION_VALUES = "filingHistoryDescriptionValues";
    private static final String FILING_HISTORY_TYPE = "filingHistoryType";
    private static final String FILING_HISTORY_CATEGORY = "filingHistoryCategory";
    private static final String FILING_HISTORY_BARCODE = "filingHistoryBarcode";

    private final SerializerFactory serializerFactory;
    private final ObjectMapper objectMapper;

    public ItemMessageFactory(final SerializerFactory serializer, @Qualifier("snakeCaseMapper") final ObjectMapper mapper) {
        serializerFactory = serializer;
        objectMapper = mapper;
    }

    /**
     * Creates an item message for onward production to an outbound Kafka topic.
     *
     * @param orderItemPair the {@link OrderItemPair} of order and MID order item returned from orders API
     * @return the avro message representing the item (plus some order related information)
     */
    public Message createMessage(final OrderItemPair orderItemPair) {
        LOGGER.trace("Creating item message");
        final Message message;
        try {
            final ChdItemOrdered outgoing = buildChdItemOrdered(orderItemPair);
            final AvroSerializer<ChdItemOrdered> serializer =
                serializerFactory.getGenericRecordSerializer(ChdItemOrdered.class);
            message = new Message();
            message.setValue(serializer.toBinary(outgoing));
            message.setTopic(CHD_ITEM_ORDERED_TOPIC);
            message.setTimestamp(new Date().getTime());
        } catch (Exception ex) {
            final String errorMessage =
                format("Unable to create ChdItemOrdered message for order %s item ID %s!",
                        orderItemPair.getOrder().getReference(),
                        orderItemPair.getItem().getId());
            LOGGER.error(errorMessage, ex);
            throw new KafkaMessagingException(errorMessage, ex);
        }
        return message;
    }

    /**
     * Creates a {@link ChdItemOrdered} Kafka message content instance from the {@link OrderItemPair} instance
     * provided.
     *
     * @param orderItemPair the original order and MID item pairing from which the message content is built
     * @return the resulting Kafka message content object
     */
    ChdItemOrdered buildChdItemOrdered(final OrderItemPair orderItemPair) {
        OrderData order = orderItemPair.getOrder();
        final uk.gov.companieshouse.itemhandler.model.Item orderItem = orderItemPair.getItem();
        final ChdItemOrdered outgoing = new ChdItemOrdered();
        outgoing.setOrderedAt(order.getOrderedAt().toString());
        outgoing.setOrderedBy(createOrderedBy(order.getOrderedBy()));
        outgoing.setPaymentReference(order.getPaymentReference());
        outgoing.setReference(order.getReference());
        outgoing.setTotalOrderCost(order.getTotalOrderCost());

        final Item item = new Item();
        item.setId(orderItem.getId());
        item.setCompanyName(orderItem.getCompanyName());
        item.setCompanyNumber(orderItem.getCompanyNumber());
        item.setCustomerReference(orderItem.getCustomerReference());
        item.setDescription(orderItem.getDescription());
        item.setDescriptionIdentifier(orderItem.getDescriptionIdentifier());
        item.setDescriptionValues(orderItem.getDescriptionValues());
        item.setItemCosts(createFirstItemCosts(orderItem));
        item.setItemOptions(createFirstItemOptionsForMid(orderItem));
        item.setItemUri(orderItem.getItemUri());
        item.setKind(orderItem.getKind());

        item.setPostageCost(ZERO_POSTAGE_COST);
        item.setIsPostalDelivery(NO_POSTAL_DELIVERY);

        item.setLinks(new Links(orderItem.getLinks().getSelf()));
        item.setQuantity(orderItem.getQuantity());
        item.setTotalItemCost(orderItem.getTotalItemCost());

        outgoing.setItem(item);
        return outgoing;
    }

    /**
     * Creates a List of {@link ItemCosts} from the first item's List of
     * {@link uk.gov.companieshouse.itemhandler.model.ItemCosts}.
     *
     * @param item {@link uk.gov.companieshouse.itemhandler.model.Item} the current MID item being processed in the order
     * @return list item costs
     */
    private List<ItemCosts> createFirstItemCosts(final uk.gov.companieshouse.itemhandler.model.Item item) {
        return item.getItemCosts().stream().map(costs ->
            new ItemCosts(costs.getCalculatedCost(),
                costs.getDiscountApplied(),
                costs.getItemCost(),
                costs.getProductType().getJsonName()))
            .collect(toList());

    }

    /**
     * Creates a suitable map of values representing MID item options ready for use as part of a Kafka message.
     *
     * @param item {@link uk.gov.companieshouse.itemhandler.model.Item} the current MID item being processed in the order
     * @return map of values representing MID item options
     */
    private Map<String, String> createFirstItemOptionsForMid(final uk.gov.companieshouse.itemhandler.model.Item item) {
        // For now we know we are dealing with MID only.
        final MissingImageDeliveryItemOptions options = (MissingImageDeliveryItemOptions) item.getItemOptions();
        final Map<String, String> optionsForMid = new HashMap<>();
        optionsForMid.put(FILING_HISTORY_ID, options.getFilingHistoryId());
        optionsForMid.put(FILING_HISTORY_DATE, options.getFilingHistoryDate());
        optionsForMid.put(FILING_HISTORY_DESCRIPTION, options.getFilingHistoryDescription());
        // Note this implicit contract - consumer needs to deserialise to Map<String, Object>.
        optionsForMid.put(FILING_HISTORY_DESCRIPTION_VALUES,
            objectMapper.writeValueAsString(options.getFilingHistoryDescriptionValues()));
        optionsForMid.put(FILING_HISTORY_TYPE, options.getFilingHistoryType());
        optionsForMid.put(FILING_HISTORY_CATEGORY, options.getFilingHistoryCategory());
        optionsForMid.put(FILING_HISTORY_BARCODE, options.getFilingHistoryBarcode());
        return optionsForMid;
    }

    /**
     * Creates a Kafka message content {@link OrderedBy} instance from the {@link ActionedBy} instance provided.
     *
     * @param actionedBy the ordered by info from the order
     * @return the info to populate the Kafka message with
     */
    private OrderedBy createOrderedBy(final ActionedBy actionedBy) {
        return new OrderedBy(actionedBy.getEmail(), actionedBy.getId());
    }

}
