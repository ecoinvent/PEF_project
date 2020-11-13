import pandas as pd
from util.file_utils import create_file_list, write_df_to_excel
from lxml import objectify
from tqdm import tqdm
import files_path


class ParseXML:
    __slots__ = ["root"]

    def __init__(self, root):
        self.root = root

    @classmethod
    def parse_file(cls, xml_file):
        root = objectify.parse(xml_file).getroot()
        return cls(root)


class ParseProcessXML:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    ext = "{http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2017}"

    def __init__(self, source_dir=None):
        self.source_dir = source_dir
        self.files = []
        self.meta_df, self.UPR_df, self.LCI_df, self.p = None, None, None, None
        self._parse_files()

    @classmethod
    def from_input(cls):
        return cls(input(
                "Please Enter Path of Source folder that contain process files: "
                   ))

    def _create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_dir)

    def _parse_files(self):
        """[summary]
        """
        meta_rows = []
        self._create_name_list()
        for file in tqdm(self.files):
            try:
                with open(file, "r", encoding="utf-8") as xml_f:
                    parsed_file = ParseXML.parse_file(xml_f)
            except IOError as error:
                print(f"Couldnt process {file}, {error}")

            file_name = file.split("\\")[-1]
            elements_dict = self.read_meta_elements(parsed_file.root, file_name)
            meta_rows.append(elements_dict)
        self._create_df(meta_rows)

    def read_meta_elements(self, root, file_name):
        """[summary]

        Args:
            root ([type]): [description]
            file_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        elements_dict = {}
        dataSet = root.processInformation.dataSetInformation
        # classification = dataSet.classificationInformation[f"{ParseProcessXML.common}classification"]

        try:
            data_quality = root.modellingAndValidation.validation.review[f"{ParseProcessXML.common}dataQualityIndicators"]
        except AttributeError:
            data_quality = ""

        elements_dict["filename"] = file_name
        elements_dict["UUID"] = getattr(dataSet, f"{ParseProcessXML.common}UUID")
        elements_dict["baseName"] = getattr(dataSet.name, "baseName", "")
        elements_dict["treatmentStandardsRoutes"] = getattr(
            dataSet.name, "treatmentStandardsRoutes", ""
        )
        elements_dict["mixAndLocationTypes"] = getattr(
            dataSet.name, "mixAndLocationTypes", ""
        )
        elements_dict["functionalUnitFlowProperties"] = getattr(
            dataSet.name, "functionalUnitFlowProperties", ""
        )

        if(getattr(root.processInformation, "geography", "") != ""):
            elements_dict[
                "locationOfOperationSupplyOrProduction"
            ] = root.processInformation.geography.locationOfOperationSupplyOrProduction.attrib[
                "location"
            ]
        else:
            elements_dict[
                "locationOfOperationSupplyOrProduction"
            ] = ""

        elements_dict["generalComment"] = getattr(
            dataSet, f"{ParseProcessXML.common}generalComment", ""
        )
        # classification_list = classification.getchildren()
        # elements_dict["classificationInformation_class level='0'"] = classification_list[0].text
        # elements_dict["classificationInformation_class level='1'"] = classification_list[1].text

        if(getattr(root.processInformation, "technology", "") != ""):
            elements_dict["technologyDescriptionAndIncludedProcesses"] = getattr(
                root.processInformation.technology,
                "technologyDescriptionAndIncludedProcesses",
                "",
            )
        else:
            elements_dict["technologyDescriptionAndIncludedProcesses"] = ""

        elements_dict["typeOfDataSet"] = getattr(
            root.modellingAndValidation.LCIMethodAndAllocation, "typeOfDataSet", ""
        )

        if len(root.exchanges.getchildren()) > 0:
            first_exchange_ref = root.exchanges.exchange.referenceToFlowDataSet
            elements_dict["reference flow"] = getattr(first_exchange_ref, f"{ParseProcessXML.common}shortDescription")

        if data_quality and len(data_quality.getchildren()) > 0:
            for data in data_quality.getchildren():
                if data.attrib["name"] == "Technological representativeness" and (f"{ParseProcessXML.ext}numericValue" in data.attrib.keys()):
                    elements_dict["dataQualityIndicators.Technological representativenes"] = data.attrib[f"{ParseProcessXML.ext}numericValue"]
                if data.attrib["name"] == "Time representativeness" and (f"{ParseProcessXML.ext}numericValue" in data.attrib.keys()):
                    elements_dict["dataQualityIndicators.Time representativeness"] = data.attrib[f"{ParseProcessXML.ext}numericValue"]
                if data.attrib["name"] == "Geographical representativeness" and (f"{ParseProcessXML.ext}numericValue" in data.attrib.keys()):
                    elements_dict["dataQualityIndicators.Geographical representativeness"] = data.attrib[f"{ParseProcessXML.ext}numericValue"]
                if data.attrib["name"] == "Precision" and (f"{ParseProcessXML.ext}numericValue" in data.attrib.keys()):
                    elements_dict["dataQualityIndicators.Precision"] = data.attrib[f"{ParseProcessXML.ext}numericValue"]
        return elements_dict

    def _create_df(self, meta_rows):
        """[summary]

        Args:
            meta_rows ([type]): [description]
            exchanges_rows ([type]): [description]
        """
        self.meta_df = pd.DataFrame(meta_rows)
        write_df_to_excel(files_path.EXCEL_DESTINATION_DIRECTORY, "process_xml_to_excel.xlsx", self.meta_df)


obj = ParseProcessXML(
        r"C:\Dropbox (ecoinvent)\ei-int\technical\internal\IT Projects\format converters\ecoSpold2 and ILCD\Conversion tests\OpenLCA\large set of datasets 2\output\ILCD\ILCD\processes"
    )
