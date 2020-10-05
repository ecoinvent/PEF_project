import pandas as pd
from util.file_utils import create_file_list
from lxml import objectify
from tqdm import tqdm


class ParseXML:
    __slots__ = ["root"]

    def __init__(self, root):
        self.root = root

    @classmethod
    def parse_file(cls, xml_file):
        root = objectify.parse(xml_file).getroot()
        return cls(root)


class ParseProcessXML:
    """generate excel file that contains 3 different tabs(meta, upr, pivot) from the process files"""
    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"

    def __init__(self, source_dir):
        self.source_dir = source_dir
        self.files = []
        self.meta_df, self.UPR_df, self.LCI_df, self.p = None, None, None, None

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

    def start_process(self):
        """[summary]
        """
        self.__modify_df()
        self.__save2Excel()

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
            elements_dict = self.read_elements(parsed_file.root, file_name)
            meta_rows.append(elements_dict)

            exchanges_list = self.__read_exchanges(parsed_file.root, elements_dict)
            exchanges_rows.extend(exchanges_list)

        self.__create_df(meta_rows, exchanges_rows)

    def read_elements(self, root, file_name):
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
        elements_dict["treatmentStandardsRoutes"] = getattr(
            dataSet.name, "treatmentStandardsRoutes", ""
        )
        elements_dict["mixAndLocationTypes"] = getattr(
            dataSet.name, "mixAndLocationTypes", ""
        )
        elements_dict["functionalUnitFlowProperties"] = getattr(
            dataSet.name, "functionalUnitFlowProperties", ""
        )
        elements_dict["generalComment"] = getattr(
            dataSet, f"{self.__class__.common}generalComment", ""
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
        return elements_dict

    def __read_exchanges(self, root, elements_dict):
        """[summary]

        Args:
            root ([type]): [description]
            elements_dict ([type]): [description]

        Returns:
            [type]: [description]
        """
        exchanges_list = []
        dataset_type = elements_dict["typeOfDataSet"]
        baseName = elements_dict["baseName"]
        UUID = elements_dict["UUID"]
        location_Of_Operation = elements_dict["locationOfOperationSupplyOrProduction"]
        for exchange in root.exchanges.getchildren():
            exchange_dict = {}
            exchange_dict["UUID"] = UUID
            exchange_dict["dataset_type"] = dataset_type
            exchange_dict["baseName"] = baseName
            exchange_dict[
                "locationOfOperationSupplyOrProduction"
            ] = location_Of_Operation
            exchange_dict[
                "referenceToFlowDataSet_refObjectId"
            ] = exchange.referenceToFlowDataSet.attrib["refObjectId"]
            exchange_dict["referenceToFlowDataSet.shortDescription"] = getattr(
                exchange.referenceToFlowDataSet,
                f"{self.__class__.common}shortDescription",
            )

            exchange_dict["location"] = getattr(exchange, "location", "")
            exchange_dict["exchangeDirection"] = getattr(
                exchange, "exchangeDirection", ""
            )
            exchange_dict["resultingAmount"] = getattr(exchange, "resultingAmount", "")
            exchange_dict["dataSourceType"] = getattr(exchange, "dataSourceType", "")
            exchange_dict["dataDerivationTypeStatus"] = getattr(
                exchange, "dataDerivationTypeStatus", ""
            )
            exchanges_list.append(exchange_dict)
        return exchanges_list

    def __modify_columns_types(self, df, columns):
        """[summary]

        Args:
            df ([type]): [description]
            columns ([type]): [description]

        Returns:
            [type]: [description]
        """
        for col in columns:
            df[col] = df[col].astype(str)

        if df["resultingAmount"].dtype == object:
            df["resultingAmount"] = df["resultingAmount"].astype(float)
        return df

    def __create_df(self, meta_rows, exchanges_rows):
        """[summary]

        Args:
            meta_rows ([type]): [description]
            exchanges_rows ([type]): [description]
        """
        self.meta_df = pd.DataFrame(meta_rows)
        exchanges_df = pd.DataFrame(exchanges_rows)
        exchanges_df["dataset_type"] = exchanges_df["dataset_type"].astype(str)
        self.UPR_df = exchanges_df[~exchanges_df["dataset_type"].str.contains("LCI")]
        self.LCI_df = exchanges_df[exchanges_df["dataset_type"].str.contains("LCI")]

    def __modify_df(self):
        """[summary]
        """
        self.LCI_df["UUID//baseName [locationOfOperationSupplyOrProduction]"] = (
            self.LCI_df["UUID"].astype(str)
            + "//"
            + self.LCI_df["baseName"].astype(str)
            + " ["
            + self.LCI_df["locationOfOperationSupplyOrProduction"].astype(str)
            + "]"
        )

        self.LCI_df.drop(["UUID", "dataset_type", "baseName"], axis=1, inplace=True)
        self.__generate_pivot()

    def __generate_pivot(self):
        """[summary]
        """
        index_list = [
            "referenceToFlowDataSet_refObjectId",
            "referenceToFlowDataSet.shortDescription",
            "location",
            "exchangeDirection",
        ]

        self.p = pd.pivot_table(
            self.LCI_df,
            index=index_list,
            columns=["UUID//baseName [locationOfOperationSupplyOrProduction]"],
            values="resultingAmount",
            aggfunc="sum",
        )
        self.p.reset_index(inplace=True)

    def __save2Excel(self):
        """[summary]
        """
        with pd.ExcelWriter(r"D:\ecoinvent_scripts\processToExcel.xlsx") as writer:
            self.meta_df.to_excel(writer, sheet_name="meta", index=False)
            self.UPR_df.to_excel(writer, sheet_name="UPR", index=False)
            self.p.to_excel(writer, sheet_name="pivot", index=False)


def main():
    obj = ParseProcessXML(
        r"C:\Dropbox (ecoinvent)\ei-int\technical\internal\IT Projects\format converters\ecoSpold2 and ILCD\Conversion tests\OpenLCA\large set of datasets 2\output\ILCD\ILCD\processes"
    )
    obj.parse_files()
    obj.start_process()


if __name__ == "__main__":
    main()
