import os
from lxml import objectify
from util.file_utils import create_file_list
import pandas as pd
from tqdm import tqdm
import files_path
import numpy as np


class ParseXML:
    __slots__ = ["root"]

    def __init__(self, root):
        self.root = root

    @classmethod
    def parse_file(cls, xml_file):
        root = objectify.parse(xml_file).getroot()
        return cls(root)


class ParseLciaScores:
    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"

    def __init__(self, source_dir):
        self.source_dir = source_dir
        self.files = []

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
            meta_rows.append(elements_dict)

            exchanges_list = self.__read_LCIA_results(parsed_file.root)
            exchanges_rows.extend(exchanges_list)
        self.__create_df(meta_rows, exchanges_rows)

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
        elements_dict["filename"] = file_name
        elements_dict["UUID"] = getattr(dataSet, f"{self.__class__.common}UUID")
        elements_dict["baseName"] = getattr(dataSet.name, "baseName", "")
        return elements_dict

    def __read_LCIA_results(self, root):
        """[read the contents of LCIA results]

        Args:
            root ([type]): [description]
            elements_dict ([type]): [description]

        Returns:
            [type]: [description]
        """
        lcia_list = []
        lcia_dict = {}
        for lcia_result in root.LCIAResults.getchildren():
            lcia_title = getattr(
                lcia_result.referenceToLCIAMethodDataSet,
                f"{self.__class__.common}shortDescription",
            )
            lcia_dict[str(lcia_title)] = getattr(lcia_result, "meanAmount", "")
        lcia_list.append(lcia_dict)
        return lcia_list

    def __create_df(self, meta_rows, lcia_rows):
        """[summary]

        Args:
            meta_rows ([type]): [description]
            exchanges_rows ([type]): [description]
        """
        meta_df = pd.DataFrame(meta_rows)
        lcia_df = pd.DataFrame(lcia_rows)
        print(lcia_df)
        df = pd.concat([meta_df, lcia_df], axis=1, sort=False)
        df.to_excel(r'D:\ecoinvent_scripts\lcia_process.xlsx')


obj = ParseLciaScores(files_path.PROCESS_FILES_SOURCE_DIR)
obj.parse_files()
