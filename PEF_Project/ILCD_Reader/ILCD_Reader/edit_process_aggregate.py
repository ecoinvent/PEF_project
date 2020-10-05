from lxml import objectify
import pandas as pd
from util.file_utils import create_file_list, save_xml_file
import lxml.etree as ET
from datetime import datetime
import sys
import os


class EditProcessAggregate:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None

    def edit_files(self):
        self._read_excelTemplate()
        self._open_xml_file()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name=None)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)
        self._read_xml_string_from_excel()
        self._convert_dataframe_firstrow_todict()

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

    def _convert_dataframe_firstrow_todict(self):
        """[summary]
        """
        self.dict_row = next(iter(self.df['aggr_file_update_meta&admin'].to_dict('records')))

    def _open_xml_file(self):
        """[summary]

        Args:
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        self._create_list_of_files()
        for file in self.files:
            try:
                with open(file, "r", encoding="utf-8") as xml_f:
                    self.myfile = objectify.parse(xml_f)
            except IOError as error:
                print(f"Error: Couldnt process {file}, {error}")
            else:
                root = self.myfile.getroot()
                self._validate_xml_elements(root)
                self._edit_xml_elements_contents(root)
                self._insert_elements_in_xml(root)
                self._remove_child_exchanges_elements()
        self._save_modified_xml(file)

    def _create_list_of_files(self):
        """[summary]
        """
        self.files = create_file_list(self.source_xml_dir)

    def _validate_xml_elements(self, root):
        try:
            self.dataset_info = root.processInformation.dataSetInformation
            self.technology = root.processInformation.technology
            self.review = root.modellingAndValidation.validation.review
            self.admin_info = root.administrativeInformation
            self.publication = root.administrativeInformation.publicationAndOwnership
            self.data_entry = root.administrativeInformation.dataEntryBy
            self.modeling_validation = root.modellingAndValidation
            self.exchanges = root.exchanges
            self.validation_review = root.modellingAndValidation.validation.review
            self.time = root.processInformation.time
            self.modelling_constant = root.modellingAndValidation.LCIMethodAndAllocation.modellingConstants
            self.dataselection = (root.modellingAndValidation
                                  .dataSourcesTreatmentAndRepresentativeness
                                  .dataSelectionAndCombinationPrinciples
                                  )
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _edit_xml_elements_contents(self, root):
        """[summary]

        Args:
            root ([type]): [description]
        """
        self.time[f"{EditProcessAggregate.common}dataSetValidUntil"]._setText(str(self.dict_row['dataSetValidUntil']))
        self.data_entry[f"{EditProcessAggregate.common}timeStamp"]._setText(datetime.now().astimezone().isoformat())
        (self.publication[f"{EditProcessAggregate.common}dateOfLastRevision"]._setText(
                                                                                      datetime.now()
                                                                                      .astimezone()
                                                                                      .isoformat())
         )
        self.publication[f"{EditProcessAggregate.common}dataSetVersion"]._setText(self.dict_row['dataSetVersion'])
        self.publication[f"{EditProcessAggregate.common}accessRestrictions"]._setText(self.dict_row['accessRestrictions'])
        self.dataset_info[f"{EditProcessAggregate.common}generalComment"]._setText(self.dict_row['generalComment'])
        self.validation_review[f"{EditProcessAggregate.common}reviewDetails"]._setText(self.dict_row['reviewDetails'])
        (self.validation_review[f"{EditProcessAggregate.common}referenceToCompleteReviewReport"]
         .set("refObjectId", str(self.dict_row['referenceToCompleteReviewReport_refObjectId']))
         )
        (self.validation_review[f"{EditProcessAggregate.common}referenceToCompleteReviewReport"]
         .set("uri", self.dict_row['referenceToTechnologyFlowDiagrammOrPicture_uri'])
         )
        (self.validation_review[f"{EditProcessAggregate.common}referenceToCompleteReviewReport"]
         .set("version", self.dict_row['referenceToCompleteReviewReport_version'])
         )
        referenceToTechnologyFlowDiagrammOrPicture = objectify.SubElement(
            self.technology,
            "referenceToTechnologyFlowDiagrammOrPicture",
            attrib={"type": "source data set",
                    "refObjectId": self.dict_row['referenceToTechnologyFlowDiagrammOrPicture_refObjectId'],
                    "version": self.dict_row['referenceToTechnologyFlowDiagrammOrPicture_version'],
                    "uri": self.dict_row['referenceToTechnologyFlowDiagrammOrPicture_uri']
                    }
            )
        referenceToTechnology_short_description = objectify.SubElement(referenceToTechnologyFlowDiagrammOrPicture,
                                                                       EditProcessAggregate.common + "shortDescription",
                                                                       attrib={
                                                                        EditProcessAggregate.common + "lang": "en"}
                                                                       )
        referenceToTechnology_short_description._setText('this is test')

        self.modelling_constant._setText(self.dict_row['modellingConstants'])
        self.dataselection._setText(self.dict_row['dataSelectionAndCombinationPrinciples'])

        (self.admin_info[f"{EditProcessAggregate.common}commissionerAndGoal"]
                        [f"{EditProcessAggregate.common}project"]._setText(self.dict_row['project'])
         )

        (self.admin_info[f"{EditProcessAggregate.common}commissionerAndGoal"]
         [f"{EditProcessAggregate.common}intendedApplications"]._setText(self.dict_row['intendedApplications'])
         )

    def _insert_elements_in_xml(self, root):
        """[summary]

        Args:
            root ([type]): [description]
        """
        (root.administrativeInformation
            [f"{EditProcessAggregate.common}commissionerAndGoal"]
            [f"{EditProcessAggregate.common}referenceToCommissioner"]
            .attrib['version']) = '00.00.000'

        (root.administrativeInformation
            [f"{EditProcessAggregate.common}commissionerAndGoal"]
            [f"{EditProcessAggregate.common}referenceToCommissioner"]
            [f"{EditProcessAggregate.common}shortDescription"]
            ._setText("European Commission, DG ENV B1")
         )

        self._remove_complianceDeclarations_elements()

        length = len(self.review.getchildren()) - 1
        for i in self.converted_list[:-2]:
            self.review.insert(length, i)
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

    def _save_modified_xml(self, file_path):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        save_xml_file(self.myfile, self.destination_xml_dir, file_name)


obj = EditProcessAggregate(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\propanol_eg_aggregatedProcess_review.xlsx",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\processes\test",
    r"D:\ecoinvent_scripts"
)
obj.edit_files()
