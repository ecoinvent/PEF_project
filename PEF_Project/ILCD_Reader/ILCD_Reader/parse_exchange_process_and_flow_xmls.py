import pandas as pd
from util.file_utils import create_file_list, write_df_to_excel
from lxml import objectify
from tqdm import tqdm
import files_path
from util.flowXML_to_Excel import parse_flow_xml
import os


class ParseXML:
    __slots__ = ["root"]

    def __init__(self, root):
        self.root = root

    @classmethod
    def parse_file(cls, xml_file):
        root = objectify.parse(xml_file).getroot()
        return cls(root)


class ParseExchangeOfProcessAndFlowsXmls:
    """generate excel file that contains 3 different tabs(meta, upr, pivot) from the process files"""
    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    flow_files_path = r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\flows\flows"

    def __init__(self, source_dir):
        self.source_dir = source_dir
        self.files = []
        self.meta_df, self.UPR_df, self.LCI_df, self.p = None, None, None, None
        self.parse_files()

    @classmethod
    def from_input(cls):
        return cls(
            input(
                "Please Enter Path of Source folder that contain matrices and indexes: "
            )
        )

    def __create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_dir)

    def parse_files(self):
        """[summary]
        """
        exchanges_rows = []
        meta_rows = []
        self.__create_name_list()
        for file in tqdm(self.files):
            try:
                with open(file, "r", encoding="utf-8") as xml_f:
                    parsed_file = ParseXML.parse_file(xml_f)
            except IOError as error:
                print(f"Couldnt process {file}, {error}")

            file_name = file.split("\\")[-1]
            elements_dict = self.__read_elements(parsed_file.root, file_name)
            exchange_dict = self.__read_exchanges(parsed_file.root, elements_dict)
            flow_dict = {}

            if(elements_dict["referenceToReferenceFlow"]):
                flow_file_name = os.path.join(self.flow_files_path, exchange_dict["referenceToFlowDataSet_refObjectId"] + ".xml")
                flow_dict = parse_flow_xml(flow_file_name)
            combined_dict = {**elements_dict, **exchange_dict, **flow_dict}
            meta_rows.append(combined_dict)
            # exchanges_rows.extend(exchanges_list)

        self.__create_df(meta_rows)

    def __read_elements(self, root, file_name):
        """[summary]

        Args:
            root ([type]): [description]
            file_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        elements_dict = {}
        dataSet = root.processInformation.dataSetInformation
        quantitativeRef = root.processInformation.quantitativeReference
        elements_dict["filename"] = file_name
        elements_dict["UUID"] = getattr(dataSet, f"{self.__class__.common}UUID")
        elements_dict["baseName"] = getattr(dataSet.name, "baseName", "")
        elements_dict["referenceToReferenceFlow"] = getattr(quantitativeRef, "referenceToReferenceFlow", "")

        return elements_dict

    def __read_exchanges(self, root, elements_dict):
        """[summary]

        Args:
            root ([type]): [description]
            elements_dict ([type]): [description]

        Returns:
            [type]: [description]
        """
        exchange_dict = {}
        for exchange in root.exchanges.getchildren():
            if exchange.attrib["dataSetInternalID"] == str(elements_dict["referenceToReferenceFlow"]):
                exchange_dict[
                    "referenceToFlowDataSet_refObjectId"
                ] = exchange.referenceToFlowDataSet.attrib["refObjectId"]
                exchange_dict["referenceToFlowDataSet.shortDescription"] = getattr(
                    exchange.referenceToFlowDataSet,
                    f"{self.__class__.common}shortDescription",
                )
                exchange_dict["location"] = getattr(exchange, "location", "")
                exchange_dict["meanAmount"] = getattr(exchange, "meanAmount", "")
                exchange_dict["resultingAmount"] = getattr(exchange, "resultingAmount", "")
        return exchange_dict

    def __create_df(self, meta_rows):
        """[summary]

        Args:
            meta_rows ([type]): [description]
            exchanges_rows ([type]): [description]
        """
        self.meta_df = pd.DataFrame(meta_rows)
        write_df_to_excel(files_path.EXCEL_DESTINATION_DIRECTORY, "processFlows_to_excel_thinkstep_complete.xlsx", self.meta_df)


if __name__ == "__main__":
    obj = ParseExchangeOfProcessAndFlowsXmls(
        r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\processes"
    )
