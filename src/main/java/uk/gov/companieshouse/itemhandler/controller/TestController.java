package uk.gov.companieshouse.itemhandler.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;
import uk.gov.companieshouse.itemhandler.logging.LoggingUtils;
import uk.gov.companieshouse.itemhandler.service.OrderProcessorService;
import uk.gov.companieshouse.logging.Logger;

/**
 * Temporary controller introduced to facilitate testing.
 * TODO GCI-1460 Remove this temporary test code.
 */
@RestController
public class TestController {

    private static final Logger LOGGER = LoggingUtils.getLogger();

    private static final String ORDERS_URI_PREFIX = "/orders/";

    private final OrderProcessorService orderProcessor;

    public TestController(final OrderProcessorService orderProcessor) {
        this.orderProcessor = orderProcessor;
    }

    @PutMapping("${uk.gov.companieshouse.item-handler.test}/{kind}/{order}")
    public ResponseEntity<Void> testProcessing (final @PathVariable String kind,
                                                final @PathVariable String order) {
        LOGGER.info("testProcessing (" + kind + ", " + order + ")");
        orderProcessor.processOrderReceived(ORDERS_URI_PREFIX + order);
        return ResponseEntity.status(HttpStatus.OK).build();
    }

}