from lxml import objectify
from util.file_utils import save_xml_file
import sys
from collections import defaultdict
from util.file_utils import create_file_list
from tqdm import tqdm


class ModifyBlackBoxProcess:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    xml = "{http://www.w3.org/XML/1998/namespace}"

    def __init__(self, source_xml_dir, destination_xml_dir):
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.source_file_dict = defaultdict()
        self.count = -1

    def generate_files(self):
        self._process_XML_files()

    def _create_file_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_xml_dir)

    def _process_XML_files(self):
        """[summary]
        """
        self._create_file_list()
        for file in tqdm(self.files):
            self._get_xml_root(file)
            self._validate_xml_elements()

            if any([self.base_name.endswith("black box"), self.base_name.endswith("elementary exchanges")]):
                self._edit_blockbox_process()
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
            self.validation = self.root.modellingAndValidation.validation
            self.base_name = self.dataset_info.name.baseName.text
            self.compliance_declaration = self.root.modellingAndValidation.complianceDeclarations
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _edit_blockbox_process(self):
        self._remove_validation_childs_elements()
        self._create_compliance_elements()

    def _remove_validation_childs_elements(self):
        for review in self.validation.getchildren():
            if review.tag == ModifyBlackBoxProcess.process + "review":
                self.validation.remove(review)

    def _create_compliance_elements(self):
        children = self.root.modellingAndValidation.    .getchildren()
        ref = getattr(children[0], f"{ModifyBlackBoxProcess.common}referenceToComplianceSystem", "")
        ref.attrib["version"] = "00.00.001"

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)


obj = ModifyBlackBoxProcess(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\process_2ndinsertion",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\blackbox_elementry"
)

obj.generate_files()
