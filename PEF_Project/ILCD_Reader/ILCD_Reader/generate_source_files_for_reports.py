import os
from lxml import objectify
from tqdm import tqdm
from util.file_utils import save_xml_file, create_file_list
from datetime import datetime
import uuid


class GenerateSourceFilesForReports:
    common = "{http://lca.jrc.it/ILCD/Common}"

    def __init__(self, source_file, process_files_dir, output_dir):
        self.source_file = source_file
        self.process_files_dir = process_files_dir
        self.output_dir = output_dir
        self.myfile = None
        self.suffix = ".xml"

    def generated_source_files(self):
        self._open_source_xml_file()
        self._validate_source_xml_elements()
        self._parse_process_files()

    def _open_source_xml_file(self):
        if os.path.isfile(self.source_file):
            self._get_source_xml_root()

    def _get_source_xml_root(self):
        """[summary]

        Args:
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        try:
            with open(self.source_file, "r", encoding="utf-8") as xml_f:
                self.myfile = objectify.parse(xml_f)
        except IOError as error:
            print(f"Error: Couldnt process {self.file_path}, {error}")
        else:
            self.root = self.myfile.getroot()

    def _validate_source_xml_elements(self):
        try:
            self.uuid = self.root.sourceInformation.dataSetInformation[f"{GenerateSourceFilesForReports.common}UUID"]
            self.shortname = self.root.sourceInformation.dataSetInformation[f"{GenerateSourceFilesForReports.common}shortName"]
            self.referenceToDigitalFile = self.root.sourceInformation.dataSetInformation.referenceToDigitalFile
            self.timestamp = (self
                              .root
                              .administrativeInformation
                              .dataEntryBy[f"{GenerateSourceFilesForReports.common}timeStamp"]
                              )
            self.referenceToDataSetFormat = (self
                              .root
                              .administrativeInformation
                              .dataEntryBy[f"{GenerateSourceFilesForReports.common}referenceToDataSetFormat"]
                              )
            self.dataset_version = (self
                                    .root
                                    .administrativeInformation
                                    .publicationAndOwnership
                                    [f"{GenerateSourceFilesForReports.common}dataSetVersion"]
                                    )
            referenceToDataset = self.root.administrativeInformation.dataEntryBy[f"{GenerateSourceFilesForReports.common}referenceToDataSetFormat"]
            self.refernce_shortDescription = referenceToDataset[f"{GenerateSourceFilesForReports.common}shortDescription"]
        except AttributeError as error:
            print(f"Error Occured in {self.source_file}", error)
        except ValueError as error:
            print(f"Error Occured in {self.source_file}", error)

    def _parse_process_files(self):
        """[summary]
        """
        root = None
        self.__create_name_list()
        for file in tqdm(self.files):
            try:
                with open(file, "r", encoding="utf-8") as xml_file:
                    root = objectify.parse(xml_file).getroot()
                self._update_xml_elements_contents(root)
            except IOError as error:
                print(f"Couldnt process {self.file}, {error}")

    def __create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.process_files_dir)

    def _update_xml_elements_contents(self, root):
        meta_elements = self._read_process_meta_elements(root)
        source_uuid = self._generate_UUID()
        self.uuid._setText(str(source_uuid))
        self.shortname._setText(meta_elements)
        self.referenceToDigitalFile.attrib["uri"] = f"../external_docs/{meta_elements}.pdf"
        self.timestamp._setText(datetime.now().astimezone().isoformat())
        self.referenceToDataSetFormat.attrib["version"] = "03.00.003"
        self.refernce_shortDescription._setText("ILCD format 1.1")
        self.dataset_version._setText("04.00.000")
        self._save_modified_xml(str(source_uuid))

    def _read_process_meta_elements(self, root):
        """[summary]

        Args:
            root ([type]): [description]
            file_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        dataSet = root.processInformation.dataSetInformation
        location_of_operation = root.processInformation.geography.locationOfOperationSupplyOrProduction
        baseName = str(getattr(dataSet.name, "baseName", ""))
        geo = str(location_of_operation.attrib["location"])
        return f"review report for {baseName} - {geo}"

    def _generate_UUID(self):
        return uuid.uuid4()

    def _save_modified_xml(self, file_uuid):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.output_dir, file_uuid)


obj = GenerateSourceFilesForReports(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\095ae822-f972-53ca-9417-ff58f9951a72.xml",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\processes\process_level1",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\source_files"
)

obj.generated_source_files()
