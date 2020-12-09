import pandas as pd
from util.file_utils import create_file_list, write_df_to_excel
from lxml import objectify
from tqdm import tqdm
import sys
import lxml.etree as ET
from util.file_utils import save_xml_file


class UpdatingProcessWithText:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    ext = "{http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2017}"

    def __init__(self, source_dir, destination_xml_dir):
        self.source_dir = source_dir
        self.destination_xml_dir = destination_xml_dir
        self.files = []
        self.numbers_list = []
        self.text_data = None
        self._read_text_file()
        self._parse_files()

    def _create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_dir)

    def _read_text_file(self):
        with open(r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\text\other.txt", 'r') as reader:
            self.text_data = reader.read()
        self._split_xml_string_into_text()

    def _split_xml_string_into_text(self):
        """[summary]
        """
        # list_of_ele1 = self.text_data.split("</referenceToSupportedImpactAssessmentMethods>")
        # self.converted_list = ["".join((ele, "</referenceToSupportedImpactAssessmentMethods>")) for ele in list_of_ele1[:-1]]
        list_of_ele1 = self.text_data.split("/>")
        self.converted_list = ["".join((ele, "/>")) for ele in list_of_ele1[:-1]]

    def _parse_files(self):
        """[summary]
        """
        self._create_name_list()
        myfile = None
        root = None
        for file in tqdm(self.files):
            try:
                with open(file, "r", encoding="utf-8") as xml_f:
                    myfile = objectify.parse(xml_f)
            except IOError as error:
                print(f"Couldnt process {file}, {error}")
            else:
                root = myfile.getroot()

            file_name = file.split("\\")[-1]
            self._validate_xml_elements(root)
            self._add_elements()
            self._save_modified_xml(myfile, file_name)

    def _validate_xml_elements(self, root):
        try:
            self.completeness = root.modellingAndValidation.completeness
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _add_elements(self):
        parser = ET.XMLParser(remove_blank_text=True, recover=True)
        for i in self.converted_list:
            tree = ET.ElementTree(ET.fromstring(i, parser=parser))
            tree_ele = tree.getroot()
            self.completeness.insert(len(self.completeness.getchildren()), tree_ele)

    def _save_modified_xml(self, parsed_file, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(parsed_file, self.destination_xml_dir, filename)


obj = UpdatingProcessWithText(
        # r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\ILCD_EFtransition_ecoinvent_20200904_new\processes",
        r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\process_insertion",
        r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\process_2ndinsertion"
    )
