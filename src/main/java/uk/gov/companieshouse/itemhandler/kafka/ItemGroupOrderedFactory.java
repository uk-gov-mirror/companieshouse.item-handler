package uk.gov.companieshouse.itemhandler.kafka;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static uk.gov.companieshouse.itemhandler.logging.LoggingUtils.getLogMap;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import uk.gov.companieshouse.itemgroupordered.ItemGroupOrdered;
import uk.gov.companieshouse.itemgroupordered.OrderedBy;
import uk.gov.companieshouse.itemhandler.exception.KafkaMessagingException;
import uk.gov.companieshouse.itemhandler.itemsummary.ItemGroup;
import uk.gov.companieshouse.itemhandler.model.ActionedBy;
import uk.gov.companieshouse.itemhandler.model.CertifiedCopyItemOptions;
import uk.gov.companieshouse.itemhandler.model.FilingHistoryDocument;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.ItemCosts;
import uk.gov.companieshouse.itemhandler.model.ItemOptions;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ItemGroupOrderedFactory {

    static final String FILING_HISTORY_ID = "filingHistoryId";
    static final String FILING_HISTORY_DESCRIPTION = "filingHistoryDescription";
    static final String FILING_HISTORY_DESCRIPTION_VALUES = "filingHistoryDescriptionValues";
    static final String FILING_HISTORY_TYPE = "filingHistoryType";

    private final Logger logger;
    private final ObjectMapper objectMapper;

    public ItemGroupOrderedFactory(Logger logger, @Qualifier("snakeCaseMapper") ObjectMapper objectMapper) {
        this.logger = logger;
        this.objectMapper = objectMapper;
    }

    public ItemGroupOrdered createMessage(final ItemGroup digitalItemGroup) {
        final OrderData order = digitalItemGroup.getOrder();
        final Item item = digitalItemGroup.getItems().get(0);
        logger.info("Creating ItemGroupOrdered message for order " + order.getReference() + ".",
                getLogMap(order.getReference(), item.getId()));
        try {
            return ItemGroupOrdered.newBuilder()
                    .setOrderId(order.getReference())
                    .setOrderedAt(order.getOrderedAt().toString())
                    .setOrderedBy(createOrderedBy(order))
                    .setPaymentReference(order.getPaymentReference())
                    .setReference(order.getReference())
                    .setTotalOrderCost(order.getTotalOrderCost())
                    .setItems(singletonList(createItem(item)))
                    .setLinks(createOrderLinks(order))
                    .build();
        } catch (Exception ex) {
            final String errorMessage =
                    format("Unable to create ItemGroupOrdered message for order %s item ID %s! Error: %s",
                            order.getReference(),
                            item.getId(),
                            ex.getMessage());
            logger.error(errorMessage, ex, getLogMap(order.getReference(), item.getId()));
            throw new KafkaMessagingException(errorMessage, ex);
        }
    }

    private OrderedBy createOrderedBy(final OrderData order) {
        final ActionedBy actionedBy = order.getOrderedBy();
        return new OrderedBy(actionedBy.getEmail(), actionedBy.getId());
    }

    private uk.gov.companieshouse.itemgroupordered.Item createItem(final Item item) {
        return new uk.gov.companieshouse.itemgroupordered.Item(
                item.getCompanyName(),
                item.getCompanyNumber(),
                item.getCustomerReference(),
                item.getDescription(),
                item.getDescriptionIdentifier(),
                item.getDescriptionValues(),
                item.getEtag(),
                item.getId(),
                createItemCosts(item.getItemCosts()),
                createFilingHistoryItemOptions(item.getItemOptions()),
                item.getItemUri(),
                item.getKind(),
                createItemLinks(item),
                item.getPostageCost(),
                item.isPostalDelivery(),
                item.getQuantity(),
                item.getTotalItemCost()
        );
    }

    private List<uk.gov.companieshouse.itemgroupordered.ItemCosts> createItemCosts(final List<ItemCosts> itemCosts) {
        return itemCosts.stream()
                .map(costs -> new uk.gov.companieshouse.itemgroupordered.ItemCosts(
                        costs.getCalculatedCost(),
                        costs.getDiscountApplied(),
                        costs.getItemCost(),
                        costs.getProductType().toString()))
                .collect(Collectors.toList());
    }

    /**
     * Creates filing history options from the item options provided, for certified copies only.
     * @param options the item options from which filing history options may be extracted
     * @return map of filing history item options, or <code>null</code> if the options provided are not those for
     * a certified copy
     */
    private Map<String, String> createFilingHistoryItemOptions(final ItemOptions options) {
       if (options instanceof CertifiedCopyItemOptions) {
            return createCertifiedCopyFirstFilingHistoryDocOptions((CertifiedCopyItemOptions) options);
       }
       return null;
    }

    /**
     * Creates a suitable map of values representing copy filing history item options ready for use as
     * part of a Kafka message.
     *
     * @param options {@link uk.gov.companieshouse.itemhandler.model.CertifiedCopyItemOptions} the
     *                current copy item being processed in the order
     * @return map of values representing copy item filing history options
     */
    private Map<String, String> createCertifiedCopyFirstFilingHistoryDocOptions(final CertifiedCopyItemOptions options) {
        final Map<String, String> filingHistoryOptions = new HashMap<>();
        final FilingHistoryDocument firstDocument = options.getFilingHistoryDocuments().getFirst();
        filingHistoryOptions.put(FILING_HISTORY_TYPE, firstDocument.getFilingHistoryType());
        filingHistoryOptions.put(FILING_HISTORY_ID, firstDocument.getFilingHistoryId());
        filingHistoryOptions.put(FILING_HISTORY_DESCRIPTION, firstDocument.getFilingHistoryDescription());
        // Note this implicit contract - consumer needs to deserialise to Map<String, Object>.
        filingHistoryOptions.put(FILING_HISTORY_DESCRIPTION_VALUES,
                objectMapper.writeValueAsString(firstDocument.getFilingHistoryDescriptionValues()));
        return filingHistoryOptions;
    }

    private uk.gov.companieshouse.itemgroupordered.ItemLinks createItemLinks(final Item item) {
        return new uk.gov.companieshouse.itemgroupordered.ItemLinks(item.getLinks().getSelf());
    }

    private uk.gov.companieshouse.itemgroupordered.OrderLinks createOrderLinks(final OrderData order) {
        return order.getLinks() != null ?
                new uk.gov.companieshouse.itemgroupordered.OrderLinks(order.getLinks().getSelf()) : null;
    }

}
