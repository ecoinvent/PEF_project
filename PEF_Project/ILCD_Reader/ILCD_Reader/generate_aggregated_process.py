from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file
import lxml.etree as ET
from datetime import datetime
import sys
import os
from collections import defaultdict


class EditProcessAggregate:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    xml = "{http://www.w3.org/XML/1998/namespace}"
    sheet = "aggr_file_update_meta&admin"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.source_file_dict = defaultdict()

    def generate_files(self):
        self._read_excelTemplate()
        self._read_source_xml_contents()
        self._process_xmls()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name=None)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)
        self._read_xml_string_from_excel()
        self._convert_df_to_dictionary()

    def _read_xml_string_from_excel(self):
        """[summary]
        """
        ele1 = next(iter(self.df['aggr_file_update_reviewer'].columns))
        ele2 = next(iter(self.df['aggr_file_update_compliance'].columns))
        ele3 = next(iter(self.df['aggr_file_workflow'].columns))

        list_of_ele1 = ele1.split("</common:referenceToNameOfReviewerAndInstitution>")
        my_new_list = ["".join((ele, "</common:referenceToNameOfReviewerAndInstitution>")) for ele in list_of_ele1[:-1]]
        elements_list = [*my_new_list, ele2, ele3]
        self.converted_list = self._convert_string_to_xml_element(elements_list)

    @staticmethod
    def _convert_string_to_xml_element(elements_list):
        """[summary]

        Args:
            elements_list ([type]): [description]

        Returns:
            [type]: [description]
        """
        converted_list = []
        for ele in elements_list:
            parser = ET.XMLParser(remove_blank_text=True, recover=True)
            tree = ET.ElementTree(ET.fromstring(ele, parser=parser))
            tree_ele = tree.getroot()
            converted_list.append(tree_ele)
        return converted_list

    def _convert_df_to_dictionary(self):
        self.data_dictionary = self.df['aggr_file_update_meta&admin'].to_dict("list")

    def _read_source_xml_contents(self):
        source_file = next(iter(self.data_dictionary["referenceToTechnologyFlowDiagrammOrPicture_refObjectId"]))
        self._open_source_file(source_file)
        self.source_file_dict["source_version"] = self.root.administrativeInformation.publicationAndOwnership[f"{EditProcessAggregate.common}dataSetVersion"].text
        self.source_file_dict["source_uri"] = self.root.administrativeInformation.dataEntryBy[f"{EditProcessAggregate.common}referenceToDataSetFormat"].get("uri")
        self.source_file_dict["source_shortname"] = self.root.sourceInformation.dataSetInformation[f"{EditProcessAggregate.common}shortName"].text

    def _process_xmls(self):
        for filename, *args in zip(self.data_dictionary["filename"],
                                   self.data_dictionary["generalComment"],
                                   self.data_dictionary["dataSetValidUntil"],
                                #    self.data_dictionary["referenceToCompleteReviewReport_version"],
                                   self.data_dictionary["reviewDetails"],
                                   self.data_dictionary["referenceToTechnologyFlowDiagrammOrPicture_refObjectId"],
                                   self.data_dictionary["modellingConstants"],
                                   self.data_dictionary["dataSelectionAndCombinationPrinciples"],
                                   self.data_dictionary["project"],
                                   self.data_dictionary["intendedApplications"],
                                   self.data_dictionary["dataSetVersion"],
                                   self.data_dictionary["workflowAndPublicationStatus"],
                                   self.data_dictionary["accessRestrictions"]
                                   ):

            self._open_xml_file(filename)
            self._validate_xml_elements()
            self._update_xml_elements_contents(args)
            self._remove_child_exchanges_elements()
            self._remove_LCIA_elements()
            self._save_modified_xml(filename)

    def _open_source_file(self, filename):
        suffix = '.xml'
        source_dir = r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\00_ILCD_pilot_20190611\sources"
        self.file_path = os.path.join(source_dir, filename + suffix)
        if os.path.isfile(self.file_path):
            self._get_xml_root()

    def _open_xml_file(self, filename):
        suffix = '.xml'
        self.file_path = os.path.join(self.source_xml_dir, filename + suffix)
        if os.path.isfile(self.file_path):
            self._get_xml_root()

    def _get_xml_root(self):
        """[summary]

        Args:
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        try:
            with open(self.file_path, "r", encoding="utf-8") as xml_f:
                self.myfile = objectify.parse(xml_f)
        except IOError as error:
            print(f"Error: Couldnt process {self.file_path}, {error}")
        else:
            self.root = self.myfile.getroot()

    def _validate_xml_elements(self):
        try:
            self.dataset_info = self.root.processInformation.dataSetInformation
            self.location_of_operation = self.root.processInformation.geography.locationOfOperationSupplyOrProduction
            self.technology = self.root.processInformation.technology
            self.admin_info = self.root.administrativeInformation
            self.publication = self.root.administrativeInformation.publicationAndOwnership
            self.data_entry = self.root.administrativeInformation.dataEntryBy
            self.commissioner = self.root.administrativeInformation[f"{EditProcessAggregate.common}commissionerAndGoal"]
            self.modeling_validation = self.root.modellingAndValidation
            self.exchanges = self.root.exchanges
            self.lcia_results = self.root.LCIAResults
            self.validation = self.root.modellingAndValidation.validation
            self.validation_review = self.validation.review
            self.time = self.root.processInformation.time
            self.modelling_constant = self.root.modellingAndValidation.LCIMethodAndAllocation.modellingConstants
            self.dataselection = (self.root.modellingAndValidation
                                  .dataSourcesTreatmentAndRepresentativeness
                                  .dataSelectionAndCombinationPrinciples
                                  )
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _update_xml_elements_contents(self, args):
        """[summary]

        Args:
            root ([type]): [description]
        """
        (generalComment, dataSetValidUntil, reviewDetails, referencetech_refid,
         modelling_constants, dataSelectionAndCombinationPrinciples, project, intendedApplications, dataSetVersion,
         workflowAndPublicationStatus, accessRestrictions) = args

        if self.location_of_operation.attrib.get("location") == "RER":
            self.location_of_operation.attrib["location"] = "EU+EFTA+UK"

        self.dataset_info[f"{EditProcessAggregate.common}generalComment"]._setText(generalComment)
        self.time[f"{EditProcessAggregate.common}dataSetValidUntil"]._setText(str(dataSetValidUntil))
        # self.validation_review[f"{EditProcessAggregate.common}referenceToCompleteReviewReport"].set("version", reviewreport_version)

        # review = self.validation.find("review[@type='Independent review panel']/content")
        review_details = self.validation.getchildren()[1]
        review_details[f"{EditProcessAggregate.common}reviewDetails"]._setText(reviewDetails)
        # self.validation_review[f"{EditProcessAggregate.common}reviewDetails"]._setText(reviewDetails)
        self.modelling_constant._setText(modelling_constants)
        self.dataselection._setText(dataSelectionAndCombinationPrinciples)
        self.commissioner[f"{EditProcessAggregate.common}project"]._setText(project)
        self.commissioner[f"{EditProcessAggregate.common}intendedApplications"]._setText(intendedApplications)
        self.data_entry[f"{EditProcessAggregate.common}timeStamp"]._setText(datetime.now().astimezone().isoformat())
        (self.publication[f"{EditProcessAggregate.common}dateOfLastRevision"]._setText(
                                                                                      datetime.now()
                                                                                      .astimezone()
                                                                                      .isoformat())
         )
        self.publication[f"{EditProcessAggregate.common}dataSetVersion"]._setText(dataSetVersion)
        self.publication[f"{EditProcessAggregate.common}accessRestrictions"]._setText(accessRestrictions)

        self._create_flow_diagramm_element(referencetech_refid)
        self._insert_elements_in_xml()
        # updating reference to complete review report will be ignore temporary
        # (self.validation_review[f"{EditProcessAggregate.common}referenceToCompleteReviewReport"]
        #  .set("refObjectId", str(self.dict_row['referenceToCompleteReviewReport_refObjectId']))
        #  )
        # (self.validation_review[f"{EditProcessAggregate.common}referenceToCompleteReviewReport"]
        #  .set("uri", self.dict_row['referenceToTechnologyFlowDiagrammOrPicture_uri'])
        #  )

    def _create_flow_diagramm_element(self, referencetech_refid):
        #  creating referenceToTechnologyFlowDiagramm Element
        referenceToTechnologyFlowDiagrammOrPicture = objectify.SubElement(
            self.technology,
            "referenceToTechnologyFlowDiagrammOrPicture",
            attrib={"type": "source data set",
                    "refObjectId": referencetech_refid,
                    "version": self.source_file_dict["source_version"],
                    "uri": self.source_file_dict["source_uri"]
                    }
            )
        referenceToTechnology_short_description = objectify.SubElement(referenceToTechnologyFlowDiagrammOrPicture,
                                                                       EditProcessAggregate.common + "shortDescription",
                                                                       attrib={
                                                                        EditProcessAggregate.xml + "lang": "en"}
                                                                       )
        referenceToTechnology_short_description._setText(self.source_file_dict["source_shortname"])

    def _insert_elements_in_xml(self):
        """[summary]

        Args:
            root ([type]): [description]
        """
        (self.root.administrativeInformation
            [f"{EditProcessAggregate.common}commissionerAndGoal"]
            [f"{EditProcessAggregate.common}referenceToCommissioner"]
            .attrib['version']) = '29.00.000'

        (self.root.administrativeInformation
            [f"{EditProcessAggregate.common}commissionerAndGoal"]
            [f"{EditProcessAggregate.common}referenceToCommissioner"]
            [f"{EditProcessAggregate.common}shortDescription"]
            ._setText("European Commission, DG ENV B1")
         )

        self._remove_complianceDeclarations_elements()

        length = len(self.validation_review.getchildren()) - 1
        for i in self.converted_list[:-2]:
            self.validation_review.insert(length, i)
            length = length + 1
        self.modeling_validation.insert(len(self.modeling_validation.getchildren()) + 1, self.converted_list[-2])
        self.publication.insert(3, self.converted_list[-1])

    def _remove_complianceDeclarations_elements(self):
        for element in self.modeling_validation.getchildren():
            if element.tag == EditProcessAggregate.process + "complianceDeclarations":
                self.modeling_validation.remove(element)

    def _remove_child_exchanges_elements(self):
        for exchange in self.exchanges.getchildren():
            if exchange.tag == EditProcessAggregate.process + "exchange":
                self.exchanges.remove(exchange)

    def _remove_LCIA_elements(self):
        for lcia_element in self.lcia_results.getchildren():
            self.lcia_results.remove(lcia_element)

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)


obj = EditProcessAggregate(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\propanol_eg_aggregatedProcess.xlsx",
    r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\00_ILCD_pilot_20190611\processes",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\agg_processes"
)

obj.generate_files()
