from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file
import sys
from util.file_utils import create_file_list
from tqdm import tqdm


class ModifyLevelOneProcess:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    xml = "{http://www.w3.org/XML/1998/namespace}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None

    def generate_files(self):
        self._read_excelTemplate()
        self._parse_XML_files()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name="level1_parameters")
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _create_file_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_xml_dir)

    def _parse_XML_files(self):
        """[summary]
        """
        self._create_file_list()
        # myfile = None
        # root = None
        for file in tqdm(self.files):
            self._get_xml_root(file)
            self._validate_xml_elements()

            if (not any([self.baseName_text.endswith("elementary exchanges"),
                         self.baseName_text.endswith("black box")]) and
                    getattr(self.dataset_info, "complementingProcesses", "") != ""):
                self._edit_level1_process()
            else:
                continue

            file_name = file.split("\\")[-1]
            self._save_modified_xml(file_name)

    def _get_xml_root(self, file):
        """[summary]

        Args:
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        try:
            with open(file, "r", encoding="utf-8") as xml_f:
                self.myfile = objectify.parse(xml_f)
        except IOError as error:
            print(f"Error: Couldnt process {self.file_path}, {error}")
        else:
            self.root = self.myfile.getroot()

    def _validate_xml_elements(self):
        try:
            self.dataset_info = self.root.processInformation.dataSetInformation
            self.mathematicalRelations = self.root.processInformation.mathematicalRelations
            self.baseName_text = self.dataset_info.name.baseName.text
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _edit_level1_process(self):
        filtered_df = self._filter_df_baseName()
        self._convert_dfs_to_dictionary(filtered_df)
        self._create_mathmeticalRelations_elements()

    def _filter_df_baseName(self):
        filter = self.df["baseName"] == self.baseName_text
        filtered_df = self.df[filter]
        return filtered_df

    def _convert_dfs_to_dictionary(self, filtered_df):
        self.filtered_dictionary = filtered_df.to_dict("list")

    def _create_mathmeticalRelations_elements(self):
        for (name, meanValue, standardDev, comment) in zip(
                    self.filtered_dictionary["variableParameter name"],
                    self.filtered_dictionary["mathematicalRelation_meanValue"],
                    self.filtered_dictionary["mathematicalRelation_elativeStandardDeviation95In"],
                    self.filtered_dictionary["mathematicalRelation_comment"]
                                                            ):
            variableParameter = objectify.SubElement(self.mathematicalRelations, "variableParameter", attrib={"name": name})
            objectify.SubElement(variableParameter, "meanValue")._setText(str(meanValue))
            objectify.SubElement(variableParameter, "relativeStandardDeviation95In")._setText(str(standardDev))
            objectify.SubElement(variableParameter, "comment", attrib={ModifyLevelOneProcess.xml + "lang": "en"})._setText(comment)

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)


obj = ModifyLevelOneProcess(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\propanol_eg_level1Process_steamBroken.xlsx",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\process_2ndinsertion",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\process_level1",
)

obj.generate_files()
