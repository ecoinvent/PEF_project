from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file, create_file_list
# import lxml.etree as ET
import sys
import os


class AddingFlowProperties:
    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    xml = "{http://www.w3.org/XML/1998/namespace}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.files = []

    def generate_files(self):
        self._read_excelTemplate()
        self._create_files_list()
        # reference_list = self._create_list_of_reference_product()
        self._process_xmls()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name="mainProcess")
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _create_list_of_reference_product(self):
        reference_product_list = self.df["referenceProduct"].tolist()
        return reference_product_list

    def _create_files_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_xml_dir)

    def _process_xmls(self):
        for filename in self.files:
            self._open_xml_file(filename)
            baseName = self._validate_xml_elements()
            if str(baseName).endswith("box") or str(baseName).endswith("exchanges"):
                continue
            row = self._find_reference_product(baseName)
            self._create_first_flow_property(row)
            self._create_second_flow_property(row)
            file = os.path.splitext(os.path.basename(filename))[0]
            self._save_modified_xml(file)

    def _open_xml_file(self, filename):
        self.file_path = os.path.join(self.source_xml_dir, filename)
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
            baseName = self.root.flowInformation.dataSetInformation.name.baseName
            self.flow_properties = self.root.flowProperties
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)
        return baseName

    def _find_reference_product(self, baseName):
        filter = self.df["referenceProduct"] == baseName
        row = self.df.loc[filter, :]
        return row

    def _create_first_flow_property(self, row):
        flow_property = objectify.SubElement(
            self.flow_properties,
            "flowProperty",
            attrib={
                    "dataSetInternalID": "1"
                    }
            )
        referenceToFlowPropertyDataSet = objectify.SubElement(
            flow_property,
            "referenceToFlowPropertyDataSet",
            attrib={
                    "refObjectId": "7ec8d70e-4ffc-4024-86b7-6141cc0a2bf5",
                    "version": "03.00.003",
                    "type": "flow property data set",
                    "uri": "../flowproperties/7ec8d70e-4ffc-4024-86b7-6141cc0a2bf5.xml"
                    }
            )
        short_description = objectify.SubElement(
            referenceToFlowPropertyDataSet,
            AddingFlowProperties.common + "shortDescription",
            attrib={
                    AddingFlowProperties.xml + "lang": "en",
                    }
            )
        mean_value = objectify.SubElement(
            flow_property,
            "meanValue",
            )
        short_description._setText("Water content (mass)")
        mean_value._setText(str(row["water content"].values[0]))

    def _create_second_flow_property(self, row):
        #  creating referenceToTechnologyFlowDiagramm Element
        flow_property = objectify.SubElement(
            self.flow_properties,
            "flowProperty",
            attrib={
                    "dataSetInternalID": "2"
                    }
            )
        referenceToFlowPropertyDataSet = objectify.SubElement(
            flow_property,
            "referenceToFlowPropertyDataSet",
            attrib={
                    "refObjectId": "62e503ce-544a-4599-b2ad-bcea15a7bf20",
                    "version": "03.00.003",
                    "type": "flow property data set",
                    "uri": "../flowproperties/62e503ce-544a-4599-b2ad-bcea15a7bf20.xml"
                    }
            )
        short_description = objectify.SubElement(
            referenceToFlowPropertyDataSet,
            AddingFlowProperties.common + "shortDescription",
            attrib={
                    AddingFlowProperties.xml + "lang": "en",
                    }
            )
        mean_value = objectify.SubElement(
            flow_property,
            "meanValue",
            )
        short_description._setText("Carbon content (biogenic)")
        mean_value._setText(str(row["biogenic carbon content"].values[0]))

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)


obj = AddingFlowProperties(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\datasetList_deliverables_update.xlsx",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\ILCD_EFtransition_ecoinvent_20200904\flows",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\flow_files1"
)
obj.generate_files()
