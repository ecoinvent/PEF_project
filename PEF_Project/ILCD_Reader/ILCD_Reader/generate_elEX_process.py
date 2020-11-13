from lxml import objectify
import pandas as pd
from util.file_utils import save_xml_file
from datetime import datetime
import sys
import os
from collections import defaultdict
import uuid


class GenerateElExProcess:

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
        self.df["elEx_file_update_meta&admin"]["UUID"] = (self.df["elEx_file_update_meta&admin"]["UUID"]
                                                              .apply(lambda x: str(uuid.uuid4())))

    def _convert_dfs_to_dictionary(self):
        self.meta_dictionary = self.df["elEx_file_update_meta&admin"].to_dict("list")
        self.exchanges_dictionary = self.df["elEx_file_update_exchanges"].to_dict("list")

    def _process_xmls(self):
        for filename, *args in zip(self.meta_dictionary["main process ID"],
                                   self.meta_dictionary["UUID"],
                                   self.meta_dictionary["baseName"],
                                   self.meta_dictionary["typeOfDataSet"],
                                   self.meta_dictionary["classificationInformation_class level='0'"],
                                   self.meta_dictionary["classificationInformation_class level='1'"],
                                   self.meta_dictionary["generalComment"],
                                   self.meta_dictionary["useAdviceForDataSet"]
                                   ):

            self._open_xml_file(filename)
            self._validate_xml_elements()
            self._update_xml_elements_contents(args)
            self._remove_complianceDeclarations_elements()
            self._create_compliance_elements()
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
            self.modelling = self.root.modellingAndValidation
            self.file_UUID = self.dataset_info[f"{GenerateElExProcess.common}UUID"]
            self.dataset_type = self.root.modellingAndValidation.LCIMethodAndAllocation.typeOfDataSet
            self.classification = self.dataset_info.classificationInformation[f"{GenerateElExProcess.common}classification"]
            self.general_comment = self.dataset_info[f"{GenerateElExProcess.common}generalComment"]
            self.use_advice = self.modelling.dataSourcesTreatmentAndRepresentativeness.useAdviceForDataSet
            self.time_stamp = self.root.administrativeInformation.dataEntryBy[f"{GenerateElExProcess.common}timeStamp"]
            self.revision_date = self.root.administrativeInformation.publicationAndOwnership[f"{GenerateElExProcess.common}dateOfLastRevision"]
            self.compliance_declaration = self.root.modellingAndValidation.complianceDeclarations
            self.exchanges = self.root.exchanges
        except AttributeError as error:
            print('Error Occured', error)
            sys.exit(1)

    def _update_xml_elements_contents(self, args):
        """[summary]

        Args:
            root ([type]): [description]
        """
        (file_UUID, base_name, typeOfDataSet, class_level_0, class_level_1, generalComment, use_advice) = args

        self.file_UUID._setText(file_UUID)
        self.base_name._setText(base_name)
        self.dataset_type._setText(typeOfDataSet)
        self.classification.getchildren()[0]._setText(class_level_0)
        self.classification.getchildren()[1]._setText(class_level_1)
        self.general_comment._setText(generalComment)
        self.use_advice._setText(use_advice)
        self.time_stamp._setText(datetime.now().astimezone().isoformat())
        self.revision_date._setText(datetime.now().astimezone().isoformat())

    def _remove_complianceDeclarations_elements(self):
        for element in self.compliance_declaration.getchildren():
            self.compliance_declaration.remove(element)

    def _create_compliance_elements(self):
        content_dict = {"firstchild": ["2f8a3ebd-befc-4ea9-a6de-34bb6d426d2f",
                                       "../sources/2f8a3ebd-befc-4ea9-a6de-34bb6d426d2f.xml",
                                       "Environmental Footprint non-primary supporting data set",
                                       "Fully compliant",
                                       "Fully compliant",
                                       "Fully compliant",
                                       "Fully compliant",
                                       "Fully compliant",
                                       "Not defined"
                                       ],
                        "secondchild": ["3f5b0b56-60e6-4df7-869d-a811830386d9",
                                        "../sources/3f5b0b56-60e6-4df7-869d-a811830386d9.xml",
                                        "Environmental Footprint 3.0",
                                        "Not defined",
                                        "Fully compliant",
                                        "Not defined",
                                        "Not defined",
                                        "Not defined",
                                        "Not defined"]}
        for element_list in content_dict.values():
            element_iter = iter(element_list)
            compliance = objectify.SubElement(self.compliance_declaration, "compliance")
            compliance_system = objectify.SubElement(compliance,
                                                     GenerateElExProcess.common + "referenceToComplianceSystem",
                                                     attrib={"type": "source data set",
                                                             "refObjectId": next(element_iter),
                                                             "version": "00.00.000",
                                                             "uri": next(element_iter)
                                                             })
            objectify.SubElement(compliance_system,
                                 GenerateElExProcess.common + "shortDescription",
                                 attrib={GenerateElExProcess.xml + "lang": "en"}
                                 )._setText(next(element_iter))

            objectify.SubElement(compliance,
                                 GenerateElExProcess.common + "approvalOfOverallCompliance",
                                 )._setText(next(element_iter))

            objectify.SubElement(compliance,
                                 GenerateElExProcess.common + "nomenclatureCompliance",
                                 )._setText(next(element_iter))

            objectify.SubElement(compliance,
                                 GenerateElExProcess.common + "methodologicalCompliance",
                                 )._setText(next(element_iter))

            objectify.SubElement(compliance,
                                 GenerateElExProcess.common + "reviewCompliance",
                                 )._setText(next(element_iter))

            objectify.SubElement(compliance,
                                 GenerateElExProcess.common + "documentationCompliance",
                                 )._setText(next(element_iter))

            objectify.SubElement(compliance,
                                 GenerateElExProcess.common + "qualityCompliance",
                                 )._setText(next(element_iter))

    def _create_exchanges_elements(self):
        index = 1
        for (ref, reftype, uri, shortname, direction, mean, result,
                deviation, src_type, data_derivation) in zip(
                    self.exchanges_dictionary["referenceToFlowDataSet.refObjectId"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.type"][:index],
                    self.exchanges_dictionary["referenceToFlowDataSet.uri"][:index],
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
                                                                  "version": "03.01.000"})
            short_description = objectify.SubElement(referenceToFlowDataSet,
                                                     GenerateElExProcess.common + "shortDescription",
                                                     attrib={GenerateElExProcess.xml + "lang": "en"})
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
        with pd.ExcelWriter(r'D:\ecoinvent_scripts\elEx_processFiles.xlsx') as writer:
            self.df["elEx_file_update_meta&admin"].to_excel(writer, sheet_name='elEx_file_update_meta&admin')


obj = GenerateElExProcess(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\propanol_eg_elEx.xlsx",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\agg_processes",
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output\elEx_process_20200903"
)

obj.generate_files()
