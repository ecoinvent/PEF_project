from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file
import sys
import os
from collections import defaultdict


class GenerateLevelOneComplementProcess:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    xml = "{http://www.w3.org/XML/1998/namespace}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.meta_dictionary = defaultdict(list)
        self.complement_process_dict = defaultdict(list)

    def generate_files(self):
        self._read_excelTemplate()
        self._process_xmls()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name=None)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)
        self._convert_dfs_to_dictionary()

    def _convert_dfs_to_dictionary(self):
        self.meta_dictionary = self.df["level1_file_update_meta&admin"].to_dict("list")
        self.complement_process_dict = self.df["level1_file_update_compleProces"].to_dict("list")

    def _process_xmls(self):
        for filename, *args in zip(self.meta_dictionary["UUID"],
                                   self.complement_process_dict["refObjectId"],
                                   self.complement_process_dict["version"],
                                   self.complement_process_dict["uri"]
                                   ):

            self._open_xml_file(filename)
            self._validate_xml_elements()
            self._create_complement_process_elements()
            self._save_modified_xml(filename)

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
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _create_complement_process_elements(self):
        index = 2
        identifier_element = self.dataset_info.identifierOfSubDataSet
        complementing_process = objectify.SubElement(self.dataset_info, "complementingProcesses")
        for (ref, version, uri, short_descripe) in zip(
                    self.complement_process_dict["refObjectId"][:index],
                    self.complement_process_dict["version"][:index],
                    self.complement_process_dict["uri"][:index],
                    self.complement_process_dict["shortDescription"][:index],
                                                            ):
            referenceToFlowDataSet = objectify.SubElement(complementing_process,
                                                          "referenceToComplementingProcess",
                                                          attrib={"type": "process data set",
                                                                  "refObjectId": ref,
                                                                  "version": str(version),
                                                                  "uri": str(uri)})
            short_description = objectify.SubElement(referenceToFlowDataSet,
                                                     GenerateLevelOneComplementProcess.common + "shortDescription",
                                                     attrib={GenerateLevelOneComplementProcess.xml + "lang": "en"})
            short_description._setText(short_descripe)
        identifier_element.addnext(complementing_process)

        for key, value in self.complement_process_dict.items():
            self.complement_process_dict[key] = value[index:]

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)


obj = GenerateLevelOneComplementProcess(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\propanol_eg_level1Process.xlsx",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\Level1_process_20200902",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\Level1_Processes_CompProcess_20200902"
)

obj.generate_files()
