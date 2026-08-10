package uk.gov.companieshouse.itemhandler.mapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;
import uk.gov.companieshouse.api.model.order.ActionedByApi;
import uk.gov.companieshouse.api.model.order.DeliveryDetailsApi;
import uk.gov.companieshouse.api.model.order.OrdersApi;
import uk.gov.companieshouse.api.model.order.item.BaseItemApi;
import uk.gov.companieshouse.api.model.order.item.CertificateApi;
import uk.gov.companieshouse.api.model.order.item.CertificateItemOptionsApi;
import uk.gov.companieshouse.api.model.order.item.CertificateTypeApi;
import uk.gov.companieshouse.api.model.order.item.CertifiedCopyApi;
import uk.gov.companieshouse.api.model.order.item.CertifiedCopyItemOptionsApi;
import uk.gov.companieshouse.api.model.order.item.CollectionLocationApi;
import uk.gov.companieshouse.api.model.order.item.DeliveryMethodApi;
import uk.gov.companieshouse.api.model.order.item.DeliveryTimescaleApi;
import uk.gov.companieshouse.api.model.order.item.DirectorOrSecretaryDetailsApi;
import uk.gov.companieshouse.api.model.order.item.FilingHistoryDocumentApi;
import uk.gov.companieshouse.api.model.order.item.IncludeAddressRecordsTypeApi;
import uk.gov.companieshouse.api.model.order.item.IncludeDobTypeApi;
import uk.gov.companieshouse.api.model.order.item.ItemCostsApi;
import uk.gov.companieshouse.api.model.order.item.LinksApi;
import uk.gov.companieshouse.api.model.order.item.MissingImageDeliveryApi;
import uk.gov.companieshouse.api.model.order.item.MissingImageDeliveryItemOptionsApi;
import uk.gov.companieshouse.api.model.order.item.RegisteredOfficeAddressDetailsApi;
import uk.gov.companieshouse.itemhandler.model.ActionedBy;
import uk.gov.companieshouse.itemhandler.model.CertificateItemOptions;
import uk.gov.companieshouse.itemhandler.model.CertifiedCopyItemOptions;
import uk.gov.companieshouse.itemhandler.model.CompanyStatus;
import uk.gov.companieshouse.itemhandler.model.DeliveryDetails;
import uk.gov.companieshouse.itemhandler.model.DirectorOrSecretaryDetails;
import uk.gov.companieshouse.itemhandler.model.FilingHistoryDocument;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.ItemCosts;
import uk.gov.companieshouse.itemhandler.model.MissingImageDeliveryItemOptions;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.itemhandler.model.OrderLinks;
import uk.gov.companieshouse.itemhandler.model.RegisteredOfficeAddressDetails;
import uk.gov.companieshouse.itemhandler.service.FilingHistoryDescriptionProviderService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static uk.gov.companieshouse.api.model.order.item.IncludeAddressRecordsTypeApi.CURRENT;
import static uk.gov.companieshouse.api.model.order.item.IncludeDobTypeApi.PARTIAL;
import static uk.gov.companieshouse.api.model.order.item.ProductTypeApi.CERTIFICATE;
import static uk.gov.companieshouse.api.model.order.item.ProductTypeApi.CERTIFIED_COPY_INCORPORATION_SAME_DAY;
import static uk.gov.companieshouse.api.model.order.item.ProductTypeApi.MISSING_IMAGE_DELIVERY_ACCOUNTS;

@ExtendWith(SpringExtension.class)
@SpringJUnitConfig(OrdersApiToOrderDataMapperTest.Config.class)
class OrdersApiToOrderDataMapperTest {

    @Configuration
    @ComponentScan(basePackageClasses = {OrdersApiToOrderDataMapperTest.class})
    static class Config {
        @Bean
        public ObjectMapper objectMapper() {
            return JsonMapper.builder()
                    .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                    .build();
        }

        @Bean
        public FilingHistoryDescriptionProviderService filingHistoryDescriptionProviderService() {
            return new FilingHistoryDescriptionProviderService();
        }
    }

    @Autowired
    private OrdersApiToOrderDataMapper mapper;

    private static final String ID = "CHS00000000000000001";
    private static final String ORDER_ETAG          = "etag-xyz";
    private static final String ORDER_KIND          = "order";
    private static final String ORDER_REF           = "reference-xyz";
    private static final String PAYMENT_REF         = "payment-ref-xyz";
    private static final String ORDER_TOTAL_COST    = "10";
    private static final String LINK_SELF           = "/link.self";
    private static final String ACTIONED_BY_EMAIL   = "demo@ch.gov.uk";
    private static final String ACTIONED_BY_ID      = "id-xyz";

    private static final String CONTACT_NUMBER      = "00006444";
    private static final String COMPANY_NUMBER      = "00006444";
    private static final String COMPANY_NAME        = "Phillips & Daughters";
    private static final int QUANTITY               = 10;
    private static final String DESCRIPTION         = "Certificate";
    private static final String DESCRIPTION_IDENTIFIER_CERTIFICATE = "certificate";
    private static final String DESCRIPTION_IDENTIFIER_CERTIFIEDCOPY = "certified-copy";
    private static final String DESCRIPTION_IDENTIFIER_MISSING_IMAGE_DERLIVERY = "missing-image-derlivery";
    private static final Map<String, String> DESCRIPTION_VALUES = singletonMap("key1", "value1");
    private static final String POSTAGE_COST        = "0";
    private static final String TOTAL_ITEM_COST     = "100";
    private static final String KIND_CERTIFICATE    = "item#certificate";
    private static final String KIND_CERTIFIEDCOPY  = "item#certified-copy";
    private static final String KIND_MISSING_IMAGE_DELIVERY = "item#missing-image-delivery";
    private static final boolean POSTAL_DELIVERY    = true;
    private static final String CUSTOMER_REFERENCE  = "Certificate ordered by NJ.";
    private static final String TOKEN_ETAG          = "9d39ea69b64c80ca42ed72328b48c303c4445e28";
    private static final FilingHistoryDocumentApi FILING_HISTORY;
    private static final ItemCostsApi CERTIFICATE_ITEM_COSTS;
    private static final ItemCostsApi CERTIFIED_COPY_ITEM_COSTS;
    private static final ItemCostsApi MISSING_IMAGE_DELIVERY_ITEM_COSTS;
    private static final LinksApi LINKS_API;
    private static final String LINKS_SELF = "links/self";
    private static final CertificateItemOptionsApi CERTIFICATE_ITEM_OPTIONS;
    private static final CertifiedCopyItemOptionsApi CERTIFIED_COPY_ITEM_OPTIONS;
    private static final MissingImageDeliveryItemOptionsApi MISSING_IMAGE_DELIVERY_ITEM_OPTIONS;
    private static final FilingHistoryDocument DOCUMENT = new FilingHistoryDocument(
            "1993-04-01",
            "memorandum-articles",
            null,
            "MDAxMTEyNzExOGFkaXF6a2N4",
            "MEM/ARTS",
            "£15"
    );
    private static final String FORENAME = "John";
    private static final String SURNAME = "Smith";

    private static final boolean INCLUDE_ADDRESS = true;
    private static final boolean INCLUDE_APPOINTMENT_DATE = false;
    private static final boolean INCLUDE_BASIC_INFORMATION = true;
    private static final boolean INCLUDE_COUNTRY_OF_RESIDENCE = false;
    private static final IncludeDobTypeApi INCLUDE_DOB_TYPE = PARTIAL;
    private static final boolean INCLUDE_NATIONALITY= false;
    private static final boolean INCLUDE_OCCUPATION = true;
    private static final boolean INCLUDE_COMPANY_OBJECTS_INFORMATION = true;
    private static final boolean INCLUDE_EMAIL_COPY = true;
    private static final boolean INCLUDE_GOOD_STANDING_INFORMATION = false;

    private static final IncludeAddressRecordsTypeApi INCLUDE_ADDRESS_RECORDS_TYPE = CURRENT;
    private static final boolean INCLUDE_DATES = true;

    private final static DirectorOrSecretaryDetailsApi DIRECTOR_OR_SECRETARY_DETAILS;
    private final static RegisteredOfficeAddressDetailsApi REGISTERED_OFFICE_ADDRESS_DETAILS;
    static {
        DIRECTOR_OR_SECRETARY_DETAILS = new DirectorOrSecretaryDetailsApi();
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeAddress(INCLUDE_ADDRESS);
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeAppointmentDate(INCLUDE_APPOINTMENT_DATE);
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeBasicInformation(INCLUDE_BASIC_INFORMATION);
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeCountryOfResidence(INCLUDE_COUNTRY_OF_RESIDENCE);
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeDobType(INCLUDE_DOB_TYPE);
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeNationality(INCLUDE_NATIONALITY);
        DIRECTOR_OR_SECRETARY_DETAILS.setIncludeOccupation(INCLUDE_OCCUPATION);

        REGISTERED_OFFICE_ADDRESS_DETAILS = new RegisteredOfficeAddressDetailsApi();
        REGISTERED_OFFICE_ADDRESS_DETAILS.setIncludeAddressRecordsType(INCLUDE_ADDRESS_RECORDS_TYPE);
        REGISTERED_OFFICE_ADDRESS_DETAILS.setIncludeDates(INCLUDE_DATES);

        CERTIFICATE_ITEM_OPTIONS = new CertificateItemOptionsApi();
        CERTIFICATE_ITEM_OPTIONS.setCertificateType(CertificateTypeApi.INCORPORATION);
        CERTIFICATE_ITEM_OPTIONS.setCollectionLocation(CollectionLocationApi.BELFAST);
        CERTIFICATE_ITEM_OPTIONS.setContactNumber(CONTACT_NUMBER);
        CERTIFICATE_ITEM_OPTIONS.setDeliveryMethod(DeliveryMethodApi.POSTAL);
        CERTIFICATE_ITEM_OPTIONS.setDeliveryTimescale(DeliveryTimescaleApi.STANDARD);
        CERTIFICATE_ITEM_OPTIONS.setDirectorDetails(DIRECTOR_OR_SECRETARY_DETAILS);
        CERTIFICATE_ITEM_OPTIONS.setForename(FORENAME);
        CERTIFICATE_ITEM_OPTIONS.setIncludeCompanyObjectsInformation(INCLUDE_COMPANY_OBJECTS_INFORMATION);
        CERTIFICATE_ITEM_OPTIONS.setIncludeEmailCopy(INCLUDE_EMAIL_COPY);
        CERTIFICATE_ITEM_OPTIONS.setIncludeGoodStandingInformation(INCLUDE_GOOD_STANDING_INFORMATION);
        CERTIFICATE_ITEM_OPTIONS.setRegisteredOfficeAddressDetails(REGISTERED_OFFICE_ADDRESS_DETAILS);
        CERTIFICATE_ITEM_OPTIONS.setSecretaryDetails(DIRECTOR_OR_SECRETARY_DETAILS);
        CERTIFICATE_ITEM_OPTIONS.setSurname(SURNAME);


        CERTIFICATE_ITEM_COSTS = new ItemCostsApi();
        CERTIFICATE_ITEM_COSTS.setDiscountApplied("1");
        CERTIFICATE_ITEM_COSTS.setItemCost("2");
        CERTIFICATE_ITEM_COSTS.setCalculatedCost("3");
        CERTIFICATE_ITEM_COSTS.setProductType(CERTIFICATE);

        CERTIFIED_COPY_ITEM_COSTS = new ItemCostsApi();
        CERTIFIED_COPY_ITEM_COSTS.setDiscountApplied("1");
        CERTIFIED_COPY_ITEM_COSTS.setItemCost("2");
        CERTIFIED_COPY_ITEM_COSTS.setCalculatedCost("3");
        CERTIFIED_COPY_ITEM_COSTS.setProductType(CERTIFIED_COPY_INCORPORATION_SAME_DAY);

        MISSING_IMAGE_DELIVERY_ITEM_COSTS = new ItemCostsApi();
        MISSING_IMAGE_DELIVERY_ITEM_COSTS.setDiscountApplied("1");
        MISSING_IMAGE_DELIVERY_ITEM_COSTS.setItemCost("2");
        MISSING_IMAGE_DELIVERY_ITEM_COSTS.setCalculatedCost("3");
        MISSING_IMAGE_DELIVERY_ITEM_COSTS.setProductType(MISSING_IMAGE_DELIVERY_ACCOUNTS);

        FILING_HISTORY = new FilingHistoryDocumentApi(DOCUMENT.getFilingHistoryDate(),
                DOCUMENT.getFilingHistoryDescription(),
                DOCUMENT.getFilingHistoryDescriptionValues(),
                DOCUMENT.getFilingHistoryId(),
                DOCUMENT.getFilingHistoryType(),
                DOCUMENT.getFilingHistoryCost()
        );

        CERTIFIED_COPY_ITEM_OPTIONS = new CertifiedCopyItemOptionsApi();
        CERTIFIED_COPY_ITEM_OPTIONS.setCollectionLocation(CollectionLocationApi.BELFAST);
        CERTIFIED_COPY_ITEM_OPTIONS.setContactNumber(CONTACT_NUMBER);
        CERTIFIED_COPY_ITEM_OPTIONS.setDeliveryMethod(DeliveryMethodApi.POSTAL);
        CERTIFIED_COPY_ITEM_OPTIONS.setDeliveryTimescale(DeliveryTimescaleApi.STANDARD);
        CERTIFIED_COPY_ITEM_OPTIONS.setFilingHistoryDocuments(singletonList(FILING_HISTORY));
        CERTIFIED_COPY_ITEM_OPTIONS.setForename(FORENAME);
        CERTIFIED_COPY_ITEM_OPTIONS.setSurname(SURNAME);

        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS = new MissingImageDeliveryItemOptionsApi();
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryDate(DOCUMENT.getFilingHistoryDate());
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryDescription(DOCUMENT.getFilingHistoryDescription());
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryDescriptionValues(DOCUMENT.getFilingHistoryDescriptionValues());
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryId(DOCUMENT.getFilingHistoryId());
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryType(DOCUMENT.getFilingHistoryType());
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryCategory("filingHistoryCategory");
        MISSING_IMAGE_DELIVERY_ITEM_OPTIONS.setFilingHistoryBarcode("01234567");

        LINKS_API = new LinksApi();
        LINKS_API.setSelf(LINKS_SELF);
    }

    @Autowired
    private OrdersApiToOrderDataMapper apiToOrderDataMapper;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void testCertificateApiToCertificate() {
        CertificateApi certificateApi = new CertificateApi();
        certificateApi.setId(ID);
        certificateApi.setCompanyName(COMPANY_NAME);
        certificateApi.setCompanyNumber(COMPANY_NUMBER);
        certificateApi.setCustomerReference(CUSTOMER_REFERENCE);
        certificateApi.setQuantity(QUANTITY);
        certificateApi.setDescription(DESCRIPTION);
        certificateApi.setDescriptionIdentifier(DESCRIPTION_IDENTIFIER_CERTIFICATE);
        certificateApi.setDescriptionValues(DESCRIPTION_VALUES);
        certificateApi.setItemCosts(singletonList(CERTIFICATE_ITEM_COSTS));
        certificateApi.setKind(KIND_CERTIFICATE);
        certificateApi.setPostalDelivery(POSTAL_DELIVERY);
        certificateApi.setItemOptions(CERTIFICATE_ITEM_OPTIONS);
        certificateApi.setLinks(LINKS_API);
        certificateApi.setPostageCost(POSTAGE_COST);
        certificateApi.setTotalItemCost(TOTAL_ITEM_COST);

        final Item certificate = apiToOrderDataMapper.apiToItem(certificateApi);

        Assertions.assertEquals(certificateApi.getId(), certificate.getId());
        assertThat(certificate.getId(), is(certificateApi.getId()));
        assertThat(certificate.getCompanyName(), is(certificateApi.getCompanyName()));
        assertThat(certificate.getCompanyNumber(), is(certificateApi.getCompanyNumber()));
        assertThat(certificate.getCustomerReference(), is(certificateApi.getCustomerReference()));
        assertThat(certificate.getQuantity(), is(certificateApi.getQuantity()));
        assertThat(certificate.getDescription(), is(certificateApi.getDescription()));
        assertThat(certificate.getDescriptionIdentifier(), is(certificateApi.getDescriptionIdentifier()));
        assertThat(certificate.getDescriptionValues(), is(certificateApi.getDescriptionValues()));
        assertThat(certificate.getKind(), is(certificateApi.getKind()));
        assertThat(certificate.isPostalDelivery(), is(certificateApi.isPostalDelivery()));
        assertThat(certificate.getEtag(), is(certificateApi.getEtag()));
        assertThat(certificate.getItemUri(), is(certificateApi.getLinks().getSelf()));
        assertThat(certificate.getLinks().getSelf(), is(certificateApi.getLinks().getSelf()));

        assertItemCosts(certificateApi.getItemCosts().getFirst(), certificate.getItemCosts().getFirst());
        assertItemOptionsSame((CertificateItemOptionsApi) certificateApi.getItemOptions(),
                (CertificateItemOptions) certificate.getItemOptions());
        assertThat(certificate.getPostageCost(), is(certificateApi.getPostageCost()));
        assertThat(certificate.getTotalItemCost(), is(certificateApi.getTotalItemCost()));
    }

    @Test
    void testCertifiedCopyApiToCertifiedCopy() {
        CertifiedCopyApi certifiedCopyApi = new CertifiedCopyApi();
        certifiedCopyApi.setId(ID);
        certifiedCopyApi.setCompanyName(COMPANY_NAME);
        certifiedCopyApi.setCompanyNumber(COMPANY_NUMBER);
        certifiedCopyApi.setCustomerReference(CUSTOMER_REFERENCE);
        certifiedCopyApi.setQuantity(QUANTITY);
        certifiedCopyApi.setDescription(DESCRIPTION);
        certifiedCopyApi.setDescriptionIdentifier(DESCRIPTION_IDENTIFIER_CERTIFIEDCOPY);
        certifiedCopyApi.setDescriptionValues(DESCRIPTION_VALUES);
        certifiedCopyApi.setItemCosts(singletonList(CERTIFIED_COPY_ITEM_COSTS));
        certifiedCopyApi.setKind(KIND_CERTIFIEDCOPY);
        certifiedCopyApi.setPostalDelivery(POSTAL_DELIVERY);
        certifiedCopyApi.setItemOptions(CERTIFIED_COPY_ITEM_OPTIONS);
        certifiedCopyApi.setLinks(LINKS_API);
        certifiedCopyApi.setPostageCost(POSTAGE_COST);
        certifiedCopyApi.setTotalItemCost(TOTAL_ITEM_COST);

        final Item certifiedCopy = apiToOrderDataMapper.apiToItem(certifiedCopyApi);

        Assertions.assertEquals(certifiedCopyApi.getId(), certifiedCopy.getId());
        assertThat(certifiedCopy.getId(), is(certifiedCopyApi.getId()));
        assertThat(certifiedCopy.getCompanyName(), is(certifiedCopyApi.getCompanyName()));
        assertThat(certifiedCopy.getCompanyNumber(), is(certifiedCopyApi.getCompanyNumber()));
        assertThat(certifiedCopy.getCustomerReference(), is(certifiedCopyApi.getCustomerReference()));
        assertThat(certifiedCopy.getQuantity(), is(certifiedCopyApi.getQuantity()));
        assertThat(certifiedCopy.getDescription(), is(certifiedCopyApi.getDescription()));
        assertThat(certifiedCopy.getDescriptionIdentifier(), is(certifiedCopyApi.getDescriptionIdentifier()));
        assertThat(certifiedCopy.getDescriptionValues(), is(certifiedCopyApi.getDescriptionValues()));
        assertThat(certifiedCopy.getKind(), is(certifiedCopyApi.getKind()));
        assertThat(certifiedCopy.isPostalDelivery(), is(certifiedCopyApi.isPostalDelivery()));
        assertThat(certifiedCopy.getEtag(), is(certifiedCopyApi.getEtag()));
        assertThat(certifiedCopy.getItemUri(), is(certifiedCopyApi.getLinks().getSelf()));
        assertThat(certifiedCopy.getLinks().getSelf(), is(certifiedCopyApi.getLinks().getSelf()));

        assertItemCosts(certifiedCopyApi.getItemCosts().getFirst(), certifiedCopy.getItemCosts().getFirst());
        assertItemOptionsSame((CertifiedCopyItemOptionsApi) certifiedCopyApi.getItemOptions(),
                (CertifiedCopyItemOptions) certifiedCopy.getItemOptions());
        assertThat(certifiedCopy.getPostageCost(), is(certifiedCopyApi.getPostageCost()));
        assertThat(certifiedCopy.getTotalItemCost(), is(certifiedCopyApi.getTotalItemCost()));
    }

    @Test
    void testMissingImageDeliveryApiToMissingImageDelivery() {
        MissingImageDeliveryApi missingImageDeliveryApi = new MissingImageDeliveryApi();
        missingImageDeliveryApi.setId(ID);
        missingImageDeliveryApi.setCompanyName(COMPANY_NAME);
        missingImageDeliveryApi.setCompanyNumber(COMPANY_NUMBER);
        missingImageDeliveryApi.setCustomerReference(CUSTOMER_REFERENCE);
        missingImageDeliveryApi.setQuantity(QUANTITY);
        missingImageDeliveryApi.setDescription(DESCRIPTION);
        missingImageDeliveryApi.setDescriptionIdentifier(DESCRIPTION_IDENTIFIER_MISSING_IMAGE_DERLIVERY);
        missingImageDeliveryApi.setDescriptionValues(DESCRIPTION_VALUES);
        missingImageDeliveryApi.setItemCosts(singletonList(MISSING_IMAGE_DELIVERY_ITEM_COSTS));
        missingImageDeliveryApi.setKind(KIND_MISSING_IMAGE_DELIVERY);
        missingImageDeliveryApi.setItemOptions(MISSING_IMAGE_DELIVERY_ITEM_OPTIONS);
        missingImageDeliveryApi.setLinks(LINKS_API);
        missingImageDeliveryApi.setPostageCost(POSTAGE_COST);
        missingImageDeliveryApi.setTotalItemCost(TOTAL_ITEM_COST);

        final Item missingImageDeliveryItem = apiToOrderDataMapper.apiToItem(missingImageDeliveryApi);


        Assertions.assertEquals(missingImageDeliveryApi.getId(), missingImageDeliveryItem.getId());
        assertThat(missingImageDeliveryItem.getId(), is(missingImageDeliveryApi.getId()));
        assertThat(missingImageDeliveryItem.getCompanyName(), is(missingImageDeliveryApi.getCompanyName()));
        assertThat(missingImageDeliveryItem.getCompanyNumber(), is(missingImageDeliveryApi.getCompanyNumber()));
        assertThat(missingImageDeliveryItem.getCustomerReference(), is(missingImageDeliveryApi.getCustomerReference()));
        assertThat(missingImageDeliveryItem.getQuantity(), is(missingImageDeliveryApi.getQuantity()));
        assertThat(missingImageDeliveryItem.getDescription(), is(missingImageDeliveryApi.getDescription()));
        assertThat(missingImageDeliveryItem.getDescriptionIdentifier(), is(missingImageDeliveryApi.getDescriptionIdentifier()));
        assertThat(missingImageDeliveryItem.getDescriptionValues(), is(missingImageDeliveryApi.getDescriptionValues()));
        assertThat(missingImageDeliveryItem.getKind(), is(missingImageDeliveryApi.getKind()));
        assertThat(missingImageDeliveryItem.getEtag(), is(missingImageDeliveryApi.getEtag()));
        assertThat(missingImageDeliveryItem.getItemUri(), is(missingImageDeliveryApi.getLinks().getSelf()));
        assertThat(missingImageDeliveryItem.getLinks().getSelf(), is(missingImageDeliveryApi.getLinks().getSelf()));

        assertItemCosts(missingImageDeliveryApi.getItemCosts().getFirst(), missingImageDeliveryItem.getItemCosts().getFirst());
        assertItemOptionsSame((MissingImageDeliveryItemOptionsApi) missingImageDeliveryApi.getItemOptions(),
            (MissingImageDeliveryItemOptions) missingImageDeliveryItem.getItemOptions());
        assertThat(missingImageDeliveryItem.getPostageCost(), is(missingImageDeliveryApi.getPostageCost()));
        assertThat(missingImageDeliveryItem.getTotalItemCost(), is(missingImageDeliveryApi.getTotalItemCost()));
    }

    private void assertItemCosts(final ItemCostsApi itemCostsApi, final ItemCosts itemCosts) {
        assertThat(itemCosts.getDiscountApplied(), is(itemCostsApi.getDiscountApplied()));
        assertThat(itemCosts.getItemCost(), is(itemCostsApi.getItemCost()));
        assertThat(itemCosts.getCalculatedCost(), is(itemCostsApi.getCalculatedCost()));
        assertThat(itemCosts.getProductType().getJsonName(), is(itemCostsApi.getProductType().getJsonName()));
    }

    private void assertItemOptionsSame(final CertificateItemOptionsApi source,
                                       final CertificateItemOptions target) {
        assertThat(target.getCertificateType().getJsonName(), is(source.getCertificateType().getJsonName()));
        assertThat(target.getCollectionLocation().getJsonName(), is(source.getCollectionLocation().getJsonName()));
        assertThat(target.getContactNumber(), is(source.getContactNumber()));
        assertThat(target.getDeliveryMethod().getJsonName(), is(source.getDeliveryMethod().getJsonName()));
        assertThat(target.getDeliveryTimescale().getJsonName(), is(source.getDeliveryTimescale().getJsonName()));
        assertDetailsSame(source.getDirectorDetails(), target.getDirectorDetails());
        assertThat(target.getForename(), is(source.getForename()));
        assertThat(target.getIncludeCompanyObjectsInformation(), is(source.getIncludeCompanyObjectsInformation()));
        assertThat(target.getIncludeEmailCopy(), is(source.getIncludeEmailCopy()));
        assertThat(target.getIncludeGoodStandingInformation(), is(source.getIncludeGoodStandingInformation()));
        assertAddressDetailsSame(source.getRegisteredOfficeAddressDetails(), target.getRegisteredOfficeAddressDetails());
        assertDetailsSame(source.getSecretaryDetails(), target.getSecretaryDetails());
        assertThat(target.getSurname(), is(source.getSurname()));
    }

    private void assertItemOptionsSame(final CertifiedCopyItemOptionsApi source, final CertifiedCopyItemOptions target) {
        assertThat(target.getCollectionLocation().getJsonName(), is(source.getCollectionLocation().getJsonName()));
        assertThat(target.getContactNumber(), is(source.getContactNumber()));
        assertThat(target.getDeliveryMethod().getJsonName(), is(source.getDeliveryMethod().getJsonName()));
        assertThat(target.getDeliveryTimescale().getJsonName(), is(source.getDeliveryTimescale().getJsonName()));
        assertThat(objectMapper.readTree(objectMapper.writeValueAsString(target.getFilingHistoryDocuments())),
                is(objectMapper.readTree(objectMapper.writeValueAsString(source.getFilingHistoryDocuments()))));
        assertThat(target.getForename(), is(source.getForename()));
        assertThat(target.getSurname(), is(source.getSurname()));
    }

    private void assertItemOptionsSame(final MissingImageDeliveryItemOptionsApi source,
                                       final MissingImageDeliveryItemOptions target) {
        assertThat(target.getFilingHistoryDate(), is(source.getFilingHistoryDate()));
        assertThat(target.getFilingHistoryDescription(), is(source.getFilingHistoryDescription()));
        assertThat(target.getFilingHistoryDescriptionValues(), is(source.getFilingHistoryDescriptionValues()));
        assertThat(target.getFilingHistoryId(), is(source.getFilingHistoryId()));
        assertThat(target.getFilingHistoryType(), is(source.getFilingHistoryType()));
        assertThat(target.getFilingHistoryCategory(), is(source.getFilingHistoryCategory()));
        assertThat(target.getFilingHistoryBarcode(), is(source.getFilingHistoryBarcode()));
    }

    private void assertDetailsSame(final DirectorOrSecretaryDetailsApi source,
                                   final DirectorOrSecretaryDetails target) {
        assertThat(target.getIncludeAddress(), is(source.getIncludeAddress()));
        assertThat(target.getIncludeAppointmentDate(), is(source.getIncludeAppointmentDate()));
        assertThat(target.getIncludeBasicInformation(), is(source.getIncludeBasicInformation()));
        assertThat(target.getIncludeCountryOfResidence(), is(source.getIncludeCountryOfResidence()));
        assertThat(target.getIncludeDobType().getJsonName(), is(source.getIncludeDobType().getJsonName()));
        assertThat(target.getIncludeNationality(), is(source.getIncludeNationality()));
        assertThat(target.getIncludeOccupation(), is(source.getIncludeOccupation()));
    }

    private void assertAddressDetailsSame(final RegisteredOfficeAddressDetailsApi source,
                                          final RegisteredOfficeAddressDetails target) {
        assertThat(target.getIncludeAddressRecordsType().getJsonName(), is(source.getIncludeAddressRecordsType().getJsonName()));
        assertThat(target.getIncludeDates(), is(source.getIncludeDates()));
    }

    @Test
    void ordersApiToOrderDataMapperTest(){
        OrdersApi ordersApi = new OrdersApi();
        ordersApi.setEtag(ORDER_ETAG);
        ordersApi.setKind(ORDER_KIND);
        ordersApi.setReference(ORDER_REF);
        ordersApi.setPaymentReference(PAYMENT_REF);
        ordersApi.setTotalOrderCost(ORDER_TOTAL_COST);
        LinksApi linksApi = new LinksApi();
        linksApi.setSelf(LINK_SELF);
        ordersApi.setLinks(linksApi);
        LocalDateTime orderedAt = LocalDateTime.now();
        ordersApi.setOrderedAt(orderedAt);
        ActionedByApi actionedByApi = new ActionedByApi();
        actionedByApi.setId(ACTIONED_BY_ID);
        actionedByApi.setEmail(ACTIONED_BY_EMAIL);
        ordersApi.setOrderedBy(actionedByApi);
        List<BaseItemApi> items = singletonList(setupTestItemApi());
        ordersApi.setItems(items);

        final OrderData actualOrderData = mapper.ordersApiToOrderData(ordersApi);
        assertThat(actualOrderData.getEtag(), is(ordersApi.getEtag()));
        assertThat(actualOrderData.getKind(), is(ordersApi.getKind()));
        assertThat(actualOrderData.getPaymentReference(), is(ordersApi.getPaymentReference()));
        assertThat(actualOrderData.getReference(), is(ordersApi.getReference()));
        assertThat(actualOrderData.getTotalOrderCost(), is(ordersApi.getTotalOrderCost()));
        assertThat(actualOrderData.getOrderedAt(), is(ordersApi.getOrderedAt()));

        Item item = actualOrderData.getItems().getFirst();
        BaseItemApi itemApi = ordersApi.getItems().getFirst();
        assertItemCosts(item.getItemCosts().getFirst(), itemApi.getItemCosts().getFirst());
        assertOrderedBy(actualOrderData.getOrderedBy(), ordersApi.getOrderedBy());
        assertLinks(actualOrderData.getLinks(), ordersApi.getLinks());
        assertDeliveryDetails(actualOrderData.getDeliveryDetails(), ordersApi.getDeliveryDetails());
    }

    private void assertItemCosts(final ItemCosts costs, final ItemCostsApi costsApi) {
        assertThat(costs.getCalculatedCost(), is(costsApi.getCalculatedCost()));
    }

    private void assertOrderedBy(final ActionedBy orderedBy, final ActionedByApi orderedByApi){
        assertThat(orderedBy.getEmail(), is(orderedByApi.getEmail()));
        assertThat(orderedBy.getId(), is(orderedByApi.getId()));
    }

    private void assertLinks(final OrderLinks links, final LinksApi linksApi) {
        assertThat(links.getSelf(), is(linksApi.getSelf()));
    }

    private void assertDeliveryDetails(final DeliveryDetails deliveryDetails,
                                       final DeliveryDetailsApi deliveryDetailsApi) {
        assertThat(deliveryDetails.getForename(), is(deliveryDetailsApi.getForename()));
        assertThat(deliveryDetails.getSurname(), is(deliveryDetailsApi.getSurname()));
        assertThat(deliveryDetails.getAddressLine1(), is(deliveryDetailsApi.getAddressLine1()));
        assertThat(deliveryDetails.getAddressLine2(), is(deliveryDetailsApi.getAddressLine2()));
        assertThat(deliveryDetails.getPremises(), is(deliveryDetailsApi.getPremises()));
        assertThat(deliveryDetails.getLocality(), is(deliveryDetailsApi.getLocality()));
        assertThat(deliveryDetails.getRegion(), is(deliveryDetailsApi.getRegion()));
        assertThat(deliveryDetails.getPoBox(), is(deliveryDetailsApi.getPoBox()));
        assertThat(deliveryDetails.getPostalCode(), is(deliveryDetailsApi.getPostalCode()));
        assertThat(deliveryDetails.getCountry(), is(deliveryDetailsApi.getCountry()));
    }

    private BaseItemApi setupTestItemApi(){
        BaseItemApi item = new BaseItemApi();
        item.setCompanyName(COMPANY_NAME);
        item.setCompanyNumber(COMPANY_NUMBER);
        item.setCustomerReference(CUSTOMER_REFERENCE);
        item.setQuantity(QUANTITY);
        item.setDescription(DESCRIPTION);
        item.setDescriptionIdentifier(DESCRIPTION_IDENTIFIER_CERTIFICATE);
        item.setDescriptionValues(DESCRIPTION_VALUES);
        ItemCostsApi itemCosts = new ItemCostsApi();
        itemCosts.setProductType(CERTIFICATE);
        itemCosts.setCalculatedCost("5");
        itemCosts.setItemCost("5");
        itemCosts.setDiscountApplied("0");
        item.setItemCosts(singletonList(itemCosts));
        item.setPostageCost(POSTAGE_COST);
        item.setTotalItemCost(TOTAL_ITEM_COST);
        item.setKind(KIND_CERTIFICATE);
        item.setPostalDelivery(POSTAL_DELIVERY);
        item.setItemOptions(CERTIFICATE_ITEM_OPTIONS);
        item.setEtag(TOKEN_ETAG);

        return item;
    }

    @Test
    @DisplayName("Correctly maps company status active to enumerated type ACTIVE")
    void testActiveCompanyStatusMapping() {
        assertThat(CompanyStatus.ACTIVE, is(mapper.mapCompanyStatus("active")));
    }

    @Test
    @DisplayName("Correctly maps company status liquidation to enumerated type LIQUIDATION")
    void testLiquidatedCompanyStatusMapping() {
        assertThat(CompanyStatus.LIQUIDATION, is(mapper.mapCompanyStatus("liquidation")));
    }

    @Test
    @DisplayName("Correctly maps unmapped company status to enumerated type OTHER")
    void testOtherCompanyStatusMapping() {
        assertThat(CompanyStatus.OTHER, is(mapper.mapCompanyStatus("xyz")));
    }
}
