import pandas as pd
import os
from lxml import objectify
import sys
from util.file_utils import save_xml_file
from datetime import datetime
from collections import defaultdict
import uuid


class GenerateSourceFiles:
    common = "{http://lca.jrc.it/ILCD/Common}"

    def __init__(self, source_template, xml_sample_file, destination_xml_dir):
        self.source_template = source_template
        self.xml_sample_file = xml_sample_file
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.data_dictionary = defaultdict(list)
        self.suffix = ".xml"

    def generated_sources(self):
        sheet = "newSourceDetails"
        # self._read_excelTemplate()
        self._generate_UUID(sheet)
        self._create_dict_of_list(sheet)
        self._process_xmls()
        # self._save_excel()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name=None)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _generate_UUID(self):
        sheet = "newSourceDetails"
        self.df[sheet]['UUID'] = self.df[sheet]['UUID'].apply(lambda _: str(uuid.uuid4()))

    def _create_dict_of_list(self, sheet):
        self.data_dictionary = self.df[sheet].to_dict("list")

    def _process_xmls(self):
        self._open_xml_file()
        self._validate_xml_elements()
        for file_uuid, *args in zip(self.data_dictionary["UUID"],
                                    self.data_dictionary["shortName"],
                                    self.data_dictionary["sourceCitation"],
                                    self.data_dictionary["dataSetVersion"],
                                    ):
            self._update_xml_elements_contents(file_uuid, args)
            self._save_modified_xml(file_uuid)

    def _open_xml_file(self):
        if os.path.isfile(self.xml_sample_file):
            self._get_xml_root()

    def _get_xml_root(self):
        """[summary]

        Args:
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        try:
            with open(self.xml_sample_file, "r", encoding="utf-8") as xml_f:
                self.myfile = objectify.parse(xml_f)
        except IOError as error:
            print(f"Error: Couldnt process {self.file_path}, {error}")
        else:
            self.root = self.myfile.getroot()

    def _validate_xml_elements(self):
        try:
            self.uuid = self.root.sourceInformation.dataSetInformation[f"{GenerateSourceFiles.common}UUID"]
            self.shortname = self.root.sourceInformation.dataSetInformation[f"{GenerateSourceFiles.common}shortName"]
            self.source_citation = self.root.sourceInformation.dataSetInformation.sourceCitation
            self.timestamp = (self
                              .root
                              .administrativeInformation
                              .dataEntryBy[f"{GenerateSourceFiles.common}timeStamp"]
                              )
            self.dataset_version = (self
                                    .root
                                    .administrativeInformation
                                    .publicationAndOwnership
                                    [f"{GenerateSourceFiles.common}dataSetVersion"]
                                    )
            parent = self.root.administrativeInformation.publicationAndOwnership[f"{GenerateSourceFiles.common}permanentDataSetURI"].getparent()
            parent.remove(self.root.administrativeInformation.publicationAndOwnership[f"{GenerateSourceFiles.common}permanentDataSetURI"])
        except AttributeError as error:
            print(f"Error Occured in {self.file_path}", error)
        except ValueError as error:
            print(f"Error Occured in {self.file_path}", error)

    def _update_xml_elements_contents(self, file_uuid, args):
        shortname, source_citation, dataset_version = args
        self.uuid._setText(file_uuid)
        self.shortname._setText(shortname)
        self.source_citation._setText(source_citation)
        self.timestamp._setText(datetime.now().astimezone().isoformat())
        self.dataset_version._setText("35.00.000")

    def _save_modified_xml(self, file_uuid):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, file_uuid)

    def _save_excel(self):
        xls_path = r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\excels\sourceFiles_UUID.xlsx"
        sheets_list = ["sourceFileExample", "newSourceDetails"]
        with pd.ExcelWriter(xls_path, engine='xlsxwriter') as writer:
            for sheet in sheets_list:
                self.df[sheet].to_excel(writer, sheet)


obj = GenerateSourceFiles(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\sourceFiles.xlsx",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\ILCD_EFtransition_ecoinvent_20200904\sources\0d388ade-52ab-4ca6-8a9b-f06df45d880c.xml",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\source_files1"
)

obj.generated_sources()
