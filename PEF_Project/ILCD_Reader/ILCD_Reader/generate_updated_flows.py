import pandas as pd
import os
from lxml import objectify
import sys
from util.file_utils import save_xml_file
from datetime import datetime
from collections import defaultdict
import uuid


class GenerateUpdatedFlows:

    common = "{http://lca.jrc.it/ILCD/Common}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.data_dictionary = defaultdict(list)
        self.suffix = ".xml"
        self.count = 0

    def generated_flows(self):
        self._read_excelTemplate()
        self._generate_UUID()
        self._create_dict_of_list()
        self._process_xmls()
        self._save_excel()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _generate_UUID(self):
        self.df['New UUID'] = self.df['New UUID'].apply(lambda _: str(uuid.uuid4()))

    def _create_dict_of_list(self):
        self.data_dictionary = self.df.to_dict("list")

    def _process_xmls(self):
        for file_id, *args in zip(self.data_dictionary["UUID of flow file to copy and modify"],
                                  self.data_dictionary["New UUID"],
                                  self.data_dictionary["referenceProduct"],
                                  self.data_dictionary["dataSetVersion"]
                                  ):

            self._open_xml_file(file_id)
            self._validate_xml_elements()
            self._update_xml_elements_contents(args)
            self._save_modified_xml(next(iter(args)))

    def _open_xml_file(self, file_id):
        self.file_path = os.path.join(self.source_xml_dir, file_id + self.suffix)
        if os.path.isfile(self.file_path):
            self._get_xml_root()
            self.count = self.count + 1

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
            self.baseName = self.root.flowInformation.dataSetInformation.name.baseName
            self.uuid = self.root.flowInformation.dataSetInformation[f"{GenerateUpdatedFlows.common}UUID"]
            self.timestamp = (self
                              .root
                              .administrativeInformation
                              .dataEntryBy[f"{GenerateUpdatedFlows.common}timeStamp"]
                              )
            self.dataset_version = (self
                                    .root
                                    .administrativeInformation
                                    .publicationAndOwnership
                                    [f"{GenerateUpdatedFlows.common}dataSetVersion"]
                                    )
        except AttributeError as error:
            print(f"Error Occured in {self.file_path}", error)
        except ValueError as error:
            print(f"Error Occured in {self.file_path}", error)

    def _update_xml_elements_contents(self, args):
        uuid, baseName, version = args
        self.uuid._setText(uuid)
        self.baseName._setText(baseName)
        self.timestamp._setText(datetime.now().astimezone().isoformat())
        self.dataset_version._setText(version)

    def _save_modified_xml(self, file_name):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, file_name)

    def _save_excel(self):
        xls_path = r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\excels\newFlowFiles_TSE&T.xlsx"
        self.df.to_excel(xls_path, index=False)


obj = GenerateUpdatedFlows(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\newFlowFiles_TSE&T.xlsx",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\EF_30_all_datasets_TSv2\flows",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\modified_flows"
)

obj.generated_flows()
