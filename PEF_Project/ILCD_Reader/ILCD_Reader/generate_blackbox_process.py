from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file
from datetime import datetime
import sys
import os
from collections import defaultdict
import uuid


class GenerateBlackBoxProcess:

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
        self.count = -1

    def generate_files(self):
        self._read_excelTemplate()
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
        # self._generate_UUID()
        self._convert_dfs_to_dictionary()

    def _generate_UUID(self):
        self.df["blackBox_file_update_meta&admin"]["UUID"] = (self.df["blackBox_file_update_meta&admin"]["UUID"]
                                                                  .apply(lambda x: str(uuid.uuid4())))

    def _convert_dfs_to_dictionary(self):
        self.meta_dictionary = self.df["blackBox_file_update_meta&admin"].to_dict("list")
        self.exchanges_dictionary = self.df["blackBox_file_update_exchanges"].to_dict("list")

    def _process_xmls(self):
        for filename, *args in zip(self.meta_dictionary["main process ID"],
                                   self.meta_dictionary["UUID"],
                                   self.meta_dictionary["baseName"],
                                   self.meta_dictionary["typeOfDataSet"],
                                   ):

            self._open_xml_file(filename)
            self._validate_xml_elements()
            self._update_xml_elements_contents(args)
            self._remove_child_exchanges_elements()
            self._create_exchanges_elements()
            self._save_modified_xml(args[0])

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
            self.base_name = self.dataset_info.name.baseName
            self.file_UUID = self.dataset_info[f"{GenerateBlackBoxProcess.common}UUID"]
            self.dataset_type = self.root.modellingAndValidation.LCIMethodAndAllocation.typeOfDataSet
            self.time_stamp = (self.root
                                   .administrativeInformation
                                   .dataEntryBy[f"{GenerateBlackBoxProcess.common}timeStamp"])
            self.revision_date = (self.root
                                      .administrativeInformation
                                      .publicationAndOwnership[f"{GenerateBlackBoxProcess.common}dateOfLastRevision"])
            self.exchanges = self.root.exchanges
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _update_xml_elements_contents(self, args):
        """[summary]

        Args:
            root ([type]): [description]
        """
        (file_UUID, base_name, typeOfDataSet) = args

        self.file_UUID._setText(file_UUID)
        self.base_name._setText(base_name)
        self.dataset_type._setText(typeOfDataSet)
        self.time_stamp._setText(datetime.now().astimezone().isoformat())
        self.revision_date._setText(datetime.now().astimezone().isoformat())

    def _remove_child_exchanges_elements(self):
        for exchange in self.exchanges.getchildren():
            if exchange.tag == GenerateBlackBoxProcess.process + "exchange":
                self.exchanges.remove(exchange)

    def _create_exchanges_elements(self):
        index = 1
        for (ref, reftype, uri, version, shortname, direction, mean, result,
                deviation, src_type, data_derivation) in zip(
                    self.exchanges_dictionary["referenceToFlowDataSet.refObjectId"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.type"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.uri"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.version"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.shortDescription"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.exchangeDirection"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.meanAmount"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.resultingAmount"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.relativeStandardDeviation95In"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.dataSourceType"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.dataDerivationTypeStatus"][:index],
                                                            ):
            exchange = objectify.SubElement(self.exchanges, "exchange", attrib={"dataSetInternalID": "0"})
            referenceToFlowDataSet = objectify.SubElement(exchange,
                                                          "referenceToFlowDataSet",
                                                          attrib={"refObjectId": ref,
                                                                  "type": reftype,
                                                                  "uri": str(uri),
                                                                  "version": str(version)})
            short_description = objectify.SubElement(referenceToFlowDataSet,
                                                     GenerateBlackBoxProcess.common + "shortDescription",
                                                     attrib={GenerateBlackBoxProcess.xml + "lang": "en"})
            short_description._setText(shortname)
            objectify.SubElement(exchange, "exchangeDirection")._setText(direction)
            objectify.SubElement(exchange, "meanAmount")._setText(str(float(mean)))
            objectify.SubElement(exchange, "resultingAmount")._setText(str(float(result)))
            objectify.SubElement(exchange, "relativeStandardDeviation95In")._setText(str(float(deviation)))
            objectify.SubElement(exchange, "dataSourceType")._setText(src_type)
            objectify.SubElement(exchange, "dataDerivationTypeStatus")._setText(data_derivation)
        for key, value in self.exchanges_dictionary.items():
            self.exchanges_dictionary[key] = value[index:]

    def _save_modified_xml(self, filename):
        """[summary]

        Args:
            file_path ([type]): [description]
        """
        save_xml_file(self.myfile, self.destination_xml_dir, filename)

    def _save_excel(self):
        with pd.ExcelWriter(r"D:\ecoinvent_scripts\blackBox_processFiles.xlsx") as writer:
            self.df["blackBox_file_update_meta&admin"].to_excel(writer, sheet_name="blackBox_file_update_meta&admin")


obj = GenerateBlackBoxProcess(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\propanol_eg_blackBox.xlsx",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\elEx_process_20200903",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\blackBox_Processes_20200903"
)

obj.generate_files()
