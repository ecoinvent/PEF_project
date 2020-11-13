from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file
# import lxml.etree as ET
from collections import defaultdict
import sys
import os


class PropanolAggregatedProcess:
    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    xml = "{http://www.w3.org/XML/1998/namespace}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None
        self.df = None
        self.source_file_dict = defaultdict()

    def generate_files(self):
        self._read_excelTemplate()
        self._read_source_excel()
        files_list = self._create_list_of_files()
        reference_list = self._create_list_of_references()
        self._process_xmls(files_list, reference_list)

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name=None)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _read_source_excel(self):
        """[summary]
        """
        try:
            self.source_df = pd.read_excel(r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\sourceFiles.xlsx", sheet_name="newSourceDetails")
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _create_list_of_files(self):
        files_list = self.df["aggr_file_update_meta&admin"]["filename"].tolist()
        return files_list

    def _create_list_of_references(self):
        column = (self.df["aggr_file_update_meta&admin"]["referenceToPrecedingDataSetVersion"].str.replace("(UUID:\s|\sVersion:\s)", "", regex=True)
                                                                                              .str.replace(";", " & "))
        reference_list = column.tolist()
        return reference_list

    def _process_xmls(self, files_list, reference_list):
        for filename, ref in zip(files_list, reference_list):
            self._open_xml_file(filename)
            self._validate_xml_elements()
            self._create_reference(ref)
            self._create_refdata_source(filename)
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
            self.publication = self.root.administrativeInformation.publicationAndOwnership
            self.dataset_version = self.publication[f"{PropanolAggregatedProcess.common}dataSetVersion"]
            self.data_sources_treatment = self.root.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _create_reference(self, ref):
        #  creating referenceToTechnologyFlowDiagramm Element
        referenceToPrecedingDataSetVersion = objectify.SubElement(
            self.publication,
            PropanolAggregatedProcess.common + "referenceToPrecedingDataSetVersion"
            )
        referenceToPrecedingDataSetVersion._setText(ref)
        self.dataset_version.addnext(referenceToPrecedingDataSetVersion)

    def _create_refdata_source(self, file_name):
        row = self.df["dataSource"].loc[self.df["dataSource"]["filename"] == file_name, :]
        for i in range(1, 10):
            if not pd.isnull(row[f"source{i}"].values[0]):
                uuid = self._find_source_file_UUID(row[f"source{i}"].values[0])
                reference_datasource = objectify.SubElement(
                    self.data_sources_treatment,
                    "referenceToDataSource",
                    attrib={
                            "refObjectId": uuid,
                            "version": "35.00.000",
                            "type": "source data set",
                            "uri": f"../sources/{uuid}.xml"
                            }
                    )
                short_description = objectify.SubElement(
                    reference_datasource,
                    PropanolAggregatedProcess.common + "shortDescription",
                    attrib={
                        PropanolAggregatedProcess.xml + "lang": "en",
                            }
                    )
                short_description._setText(row[f"source{i}"].values[0])

    def _find_source_file_UUID(self, name):
        filter = self.source_df["shortName"] == name
        uuid = self.source_df.loc[filter, "UUID"]
        return uuid.values[0]

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)


obj = PropanolAggregatedProcess(
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\propanol_eg_aggregatedProcess.xlsx",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\ILCD_EFtransition_ecoinvent_20200904\processes",
    r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\process_files1"
)
obj.generate_files()
