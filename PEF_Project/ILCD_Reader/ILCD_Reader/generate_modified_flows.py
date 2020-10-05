import pandas as pd
import os
from lxml import objectify
import sys
from util.file_utils import save_xml_file
from datetime import datetime
from collections import defaultdict
import uuid


class GenerateModifiedFlows:

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
        for sheet in ["mainProcess", "elEx", "blackbox"]:
            if sheet == "elEx" or sheet == "blackbox":
                self._generate_UUID(sheet)
            self._create_dict_of_list(sheet)
            self._process_xmls(sheet)
        self._save_excel()

    def _read_excelTemplate(self):
        """[summary]
        """
        try:
            self.df = pd.read_excel(self.source_template, sheet_name=None)
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
            sys.exit(1)

    def _generate_UUID(self, sheet):
        self.df[sheet]['UUID'] = self.df[sheet]['UUID'].apply(lambda x: str(uuid.uuid4()))

    def _create_dict_of_list(self, sheet):
        self.data_dictionary = self.df[sheet].to_dict("list")

    def _process_xmls(self, sheet):
        if sheet == "mainProcess":
            for file_id, *args in zip(self.data_dictionary["ID_referenceProduct"],
                                      self.data_dictionary["dataSetVersion"],
                                      self.data_dictionary["referenceToFlowPropertyDataSet_refObjectId"],
                                      self.data_dictionary["referenceToFlowPropertyDataSet_version"],
                                      self.data_dictionary["referenceToFlowPropertyDataSet_uri"],
                                      ):

                self._open_xml_file(file_id)
                self._validate_xml_elements()
                self._update_xml_elements_contents(sheet, args)
                self._save_modified_xml(sheet)

        if sheet == "elEx" or sheet == "blackbox":
            for file_id, *args in zip(self.data_dictionary["ID_referenceProduct"],
                                      self.data_dictionary["UUID"],
                                      self.data_dictionary["baseName"],
                                      self.data_dictionary["class level='0'"],
                                      self.data_dictionary["class level='1'"],
                                      self.data_dictionary["typeOfDataSet"],
                                      self.data_dictionary["dataSetVersion"],
                                      self.data_dictionary["referenceToFlowPropertyDataSet_refObjectId"],
                                      self.data_dictionary["referenceToFlowPropertyDataSet_version"],
                                      self.data_dictionary["referenceToFlowPropertyDataSet_uri"]
                                      ):

                self._open_xml_file(file_id)
                self._validate_xml_elements()
                self._update_xml_elements_contents(sheet, args)
                self._save_modified_xml(sheet)

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
            self.uuid = self.root.flowInformation.dataSetInformation[f"{GenerateModifiedFlows.common}UUID"]
            self.class_level_0, self.class_level_1 = (self
                                                      .root.flowInformation
                                                      .dataSetInformation
                                                      .classificationInformation
                                                      [f"{GenerateModifiedFlows.common}classification"]
                                                      .getchildren()[:2]
                                                      )
            self.type_Of_DataSet = self.root.modellingAndValidation.LCIMethod.typeOfDataSet
            self.timestamp = (self
                              .root
                              .administrativeInformation
                              .dataEntryBy[f"{GenerateModifiedFlows.common}timeStamp"]
                              )
            self.dataset_version = (self
                                    .root
                                    .administrativeInformation
                                    .publicationAndOwnership
                                    [f"{GenerateModifiedFlows.common}dataSetVersion"]
                                    )
            self.reference_toflow = self.root.flowProperties.flowProperty.referenceToFlowPropertyDataSet
        except AttributeError as error:
            print(f"Error Occured in {self.file_path}", error)
        except ValueError as error:
            print(f"Error Occured in {self.file_path}", error)

    def _update_xml_elements_contents(self, sheet, args):
        if sheet == "mainProcess":
            version, refObjectId, refversion, refuri = args
            self.dataset_version._setText(version)
            self.timestamp._setText(datetime.now().astimezone().isoformat())
            self.reference_toflow.set("refObjectId", refObjectId)
            self.reference_toflow.set("version", refversion)
            self.reference_toflow.set("uri", refuri)

        if sheet == "elEx" or sheet == "blackbox":
            uuid, baseName, class0, class1, dataset_type, version, refObjectId, refversion, refuri = args
            self.uuid._setText(uuid)
            self.baseName._setText(baseName)
            self.class_level_0._setText(class0)
            self.class_level_1._setText(class1)
            self.type_Of_DataSet._setText(dataset_type)
            self.timestamp._setText(datetime.now().astimezone().isoformat())
            self.dataset_version._setText(version)
            self.reference_toflow.set("refObjectId", refObjectId)
            self.reference_toflow.set("version", refversion)
            self.reference_toflow.set("uri", refuri)

    def _save_modified_xml(self, sheet):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        if sheet == "mainProcess":
            file_name = os.path.splitext(os.path.basename(self.file_path))[0]
        if sheet == "elEx" or sheet == "blackbox":
            file_name = self.uuid

        target_dir = os.path.join(self.destination_xml_dir, sheet)
        save_xml_file(self.myfile, target_dir, file_name)

    def _save_excel(self):
        sheets_list = ["allDatasets_flowID", "mainProcess", "elEx", "blackbox"]
        xls_path = r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\excels\datasetList_deliverables.xlsx"
        with pd.ExcelWriter(xls_path, engine='xlsxwriter') as writer:
            for sheet in sheets_list:
                self.df[sheet].to_excel(writer, sheet)
            writer.save()


obj = GenerateModifiedFlows(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\datasetList_deliverables.xlsx",
    r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\00_ILCD_pilot_20190611\flows",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\flows"
)

obj.generated_flows()
