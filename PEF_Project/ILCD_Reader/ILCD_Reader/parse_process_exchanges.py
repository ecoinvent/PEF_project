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


class ParseProcessExchanges:
    """generate excel file that contains 3 different tabs(meta, upr, pivot) from the process files"""
    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"

    def __init__(self, source_dir):
        self.source_dir = source_dir
        self.files = []
        self.df_count = 0
        self.count = 0

    def __create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_dir)

    def parse_files(self):
        """[summary]
        """
        process_rows = []
        elements_list = []
        self.__create_name_list()
        last_file = self.files[-1]
        print(last_file)

        for file in tqdm(self.files):
            if self.count == 500:
                self.files = self.files[self.count:]
                self.__create_df(process_rows)
                process_rows = []
                self.count = 0
            if file == last_file:
                elements_list = self.__preparing_file_to_be_read(file)
                process_rows.extend(elements_list)
                self.__create_df(process_rows)

            elements_list = self.__preparing_file_to_be_read(file)
            process_rows.extend(elements_list)
            self.count = self.count + 1

    def __open_xml_file(self, file):
        parsed_file = None
        try:
            with open(file, "r", encoding="utf-8") as xml_f:
                parsed_file = ParseXML.parse_file(xml_f)
        except IOError as error:
            print(f"Couldnt process {file}, {error}")
        return parsed_file

    def __preparing_file_to_be_read(self, file):
        parsed_file = self.__open_xml_file(file)
        file_name = file.split("\\")[-1]
        elements_list = self.__read_elements(parsed_file.root, file_name)
        return elements_list

    def __read_elements(self, root, file_name):
        """[summary]

        Args:
            root ([type]): [description]
            file_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        elements_dict = {}
        exchanges_list = []
        dataSet = root.processInformation.dataSetInformation
        elements_dict["filename"] = file_name
        elements_dict["UUID"] = getattr(dataSet, f"{self.__class__.common}UUID")
        exchanges_list = self.__read_exchanges(root, elements_dict)
        return exchanges_list

    def __read_exchanges(self, root, elements_dict):
        """[summary]

        Args:
            root ([type]): [description]
            elements_dict ([type]): [description]

        Returns:
            [type]: [description]
        """
        exchange_dict = {}
        exchanges_list = []
        for exchange in root.exchanges.getchildren():
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
            combined_dict = {**elements_dict, **exchange_dict}
            exchanges_list.append(combined_dict)

        return exchanges_list

    def __create_df(self, process_rows):
        """[summary]

        Args:
            meta_rows ([type]): [description]
            exchanges_rows ([type]): [description]
        """
        print("")
        print(f"writing dataframe number {self.df_count}")
        meta_df = pd.DataFrame(process_rows)
        meta_df.to_csv(f"D:\\ecoinvent_scripts\\output\\smaller\\test\\file{self.df_count}.csv")
        self.df_count = self.df_count + 1
