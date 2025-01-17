package uk.gov.companieshouse.itemhandler.mapper;


import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.springframework.beans.factory.annotation.Value;
import uk.gov.companieshouse.itemhandler.email.CertificateOrderConfirmation;
import uk.gov.companieshouse.itemhandler.model.CertificateItemOptions;
import uk.gov.companieshouse.itemhandler.model.CertificateType;
import uk.gov.companieshouse.itemhandler.model.DirectorOrSecretaryDetails;
import uk.gov.companieshouse.itemhandler.model.IncludeDobType;
import uk.gov.companieshouse.itemhandler.model.Item;
import uk.gov.companieshouse.itemhandler.model.OrderData;
import uk.gov.companieshouse.itemhandler.model.RegisteredOfficeAddressDetails;


import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.TRUE;
import static uk.gov.companieshouse.itemhandler.model.IncludeAddressRecordsType.CURRENT;

@Mapper(componentModel = "spring")
public abstract class OrderDataToCertificateOrderConfirmationMapper implements MapperUtil {

    @Value("${dispatch-days}")
    private String dispatchDays;

    // Name/address mappings
    @Mapping(source = "deliveryDetails.forename", target="forename")
    @Mapping(source = "deliveryDetails.surname", target="surname")
    @Mapping(source = "deliveryDetails.addressLine1", target="addressLine1")
    @Mapping(source = "deliveryDetails.addressLine2", target="addressLine2")
    @Mapping(source = "deliveryDetails.locality", target="houseName")
    @Mapping(source = "deliveryDetails.premises", target="houseNumberStreetName")
    @Mapping(source = "deliveryDetails.region", target="city")
    @Mapping(source = "deliveryDetails.postalCode", target="postCode")
    @Mapping(source = "deliveryDetails.country", target="country")

    // Order details field mappings
    @Mapping(source = "reference", target="orderReferenceNumber")
    @Mapping(source = "orderedBy.email", target="emailAddress")
    @Mapping(source = "totalOrderCost", target="feeAmount")

    public abstract CertificateOrderConfirmation orderToConfirmation(OrderData order);

    /**
     * Implements the more complex mapping behaviour required for some fields.
     * @param order the order to be mapped
     * @param confirmation the confirmation mapped to
     */
    @AfterMapping
    public void specialMappings(final OrderData order, final @MappingTarget CertificateOrderConfirmation confirmation)
    {
        final Item item = order.getItems().get(0);

        // Order details field mappings
        final CertificateItemOptions certificateItemOptions = (CertificateItemOptions) item.getItemOptions();
        final String timescale = certificateItemOptions.getDeliveryTimescale().toString();
        confirmation.setDeliveryMethod(toSentenceCase(timescale) + " delivery (aim to dispatch within " + dispatchDays + " working days)");
        confirmation.setTimeOfPayment(getTimeOfPayment(order.getOrderedAt()));

        // Certificate details field mappings
        confirmation.setCompanyName(item.getCompanyName());
        confirmation.setCompanyNumber(item.getCompanyNumber());
        confirmation.setCertificateType(getCertificateType(certificateItemOptions.getCertificateType()));
        confirmation.setCertificateIncludes(getCertificateIncludes(item));
        final CertificateItemOptions options = (CertificateItemOptions) item.getItemOptions();
        confirmation.setCertificateRegisteredOfficeOptions(getCertificateRegisteredOfficeOptions(item));
        confirmation.setCertificateDirectorOptions(getCertificateDirectorOptions(item));
        confirmation.setCertificateSecretaryOptions(getCertificateSecretaryOptions(item));
        confirmation.setCertificateGoodStandingInformation(getCertificateOptionsText(options.getIncludeGoodStandingInformation()));

        final DirectorOrSecretaryDetails secretaryDetails = options.getSecretaryDetails();
        if (secretaryDetails == null) {
            confirmation.setCertificateSecretaries("No");
        } else {
            confirmation.setCertificateSecretaries(getCertificateOptionsText(secretaryDetails.getIncludeBasicInformation()));
        }

        final DirectorOrSecretaryDetails directorDetails = options.getDirectorDetails();
        if (directorDetails == null) {
            confirmation.setCertificateDirectors("No");
        } else {
            confirmation.setCertificateDirectors(getCertificateOptionsText(directorDetails.getIncludeBasicInformation()));
        }

        confirmation.setCertificateCompanyObjects(getCertificateOptionsText(options.getIncludeCompanyObjectsInformation()));
    }

    /**
     * Examines the certificate item provided to build up the certificate includes list of descriptions.
     * @param certificate the certificate item
     * @return the certificate includes
     */
    //Remove when all tickets are done
    public String[] getCertificateIncludes(final Item certificate) {
        final List<String> includes = new ArrayList<>();
        final CertificateItemOptions options = (CertificateItemOptions) certificate.getItemOptions();
        if (TRUE.equals(options.getIncludeGoodStandingInformation())) {
            includes.add("Statement of good standing");
        }
        final RegisteredOfficeAddressDetails office = options.getRegisteredOfficeAddressDetails();
        if (office != null && office.getIncludeAddressRecordsType() == CURRENT ) {
            includes.add("Registered office address");
        }
        final DirectorOrSecretaryDetails directors = options.getDirectorDetails();
        if (directors != null && TRUE.equals(directors.getIncludeBasicInformation())) {
            includes.add("Directors");
        }
        final DirectorOrSecretaryDetails secretaries = options.getSecretaryDetails();
        if (secretaries != null && TRUE.equals(secretaries.getIncludeBasicInformation())) {
            includes.add("Secretaries");
        }
        if (TRUE.equals(options.getIncludeCompanyObjectsInformation())) {
            includes.add("Company objects");
        }
        return includes.toArray(new String[0]);
    }

    public String getCertificateRegisteredOfficeOptions(final Item certificate) {
        final CertificateItemOptions options = (CertificateItemOptions) certificate.getItemOptions();
        final RegisteredOfficeAddressDetails office = options.getRegisteredOfficeAddressDetails();
        if (office == null || office.getIncludeAddressRecordsType() == null) {
            return "No";
        }
        else{
            switch (office.getIncludeAddressRecordsType()) {
                case CURRENT:
                    return "Current address";
                case CURRENT_AND_PREVIOUS:
                    return "Current address and the one previous";
                case CURRENT_PREVIOUS_AND_PRIOR:
                    return "Current address and the two previous";
                case ALL:
                    return "All current and previous addresses";
                default:
                    return "No";
            }
        }
    }

    public String[] getCertificateDirectorOptions(final Item certificate) {
        final CertificateItemOptions options = (CertificateItemOptions) certificate.getItemOptions();
        final DirectorOrSecretaryDetails directors = options.getDirectorDetails();
        final List<String> includes = new ArrayList<>();
        if (directors != null && directors.getIncludeBasicInformation() != null) {
            if (directors.getIncludeAddress() != null && directors.getIncludeAddress()) {
                includes.add("Correspondence address");
            }
            if (directors.getIncludeOccupation() != null && directors.getIncludeOccupation()) {
                includes.add("Occupation");
            }
            if (directors.getIncludeDobType() != null && directors.getIncludeDobType() == IncludeDobType.PARTIAL) {
                includes.add("Date of birth (month and year)");
            }
            if (directors.getIncludeAppointmentDate() != null && directors.getIncludeAppointmentDate()) {
                includes.add("Appointment date");
            }
            if (directors.getIncludeNationality() != null && directors.getIncludeNationality()) {
                includes.add("Nationality");
            }
            if (directors.getIncludeCountryOfResidence() != null && directors.getIncludeCountryOfResidence()) {
                includes.add("Country of residence");
            }
        }
        return includes.toArray(new String[0]);
    }

    public String[] getCertificateSecretaryOptions(final Item certificate) {
        final CertificateItemOptions options = (CertificateItemOptions) certificate.getItemOptions();
        final DirectorOrSecretaryDetails secretaries = options.getSecretaryDetails();
        final List<String> includes = new ArrayList<>();
        if (secretaries != null && secretaries.getIncludeBasicInformation() != null) {
            if (secretaries.getIncludeAddress() != null && secretaries.getIncludeAddress()) {
                includes.add("Correspondence address");
            }
            if (secretaries.getIncludeAppointmentDate() != null && secretaries.getIncludeAppointmentDate()) {
                includes.add("Appointment date");
            }
        }
        return includes.toArray(new String[0]);
    }

    public String getCertificateOptionsText (Boolean options) {
        if (options == null) {
            return "No";
        }
        return (options) ? "Yes": "No";
    }

    /**
     * Gets the appropriate label for the {@link CertificateType} value provided.
     * @param type the type to be labelled
     * @return the label string representing the type as it is to be rendered in a certificate confirmation email
     */
    public String getCertificateType(final CertificateType type) {
        return type == CertificateType.INCORPORATION_WITH_ALL_NAME_CHANGES ?
                "Incorporation with all company name changes" : toSentenceCase(type.toString());
    }
}
