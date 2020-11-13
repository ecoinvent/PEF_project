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


class ParseProcessLCIAResults:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"
    ext = "{http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2017}"

    def __init__(self, source_dir=None):
        self.source_dir = source_dir
        self.files = []
        self.numbers_list = []
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
        self.__reading_pilot_LCIA_scores()
        parsed_file = None
        for file in tqdm(self.files):
            combined_dict = {}
            try:
                with open(file, "r", encoding="utf-8") as xml_f:
                    parsed_file = ParseXML.parse_file(xml_f)
            except IOError as error:
                print(f"Couldnt process {file}, {error}")

            file_name = file.split("\\")[-1]
            elements_dict = self.__read_meta_elements(parsed_file.root, file_name)
            lcia_dict = self.__read_lcia_results(parsed_file.root)
            combined_dict = {**elements_dict, **lcia_dict}
            meta_rows.append(combined_dict)
        self._create_df(meta_rows)

    def __read_meta_elements(self, root, file_name):
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
        elements_dict["UUID"] = getattr(dataSet, f"{ParseProcessLCIAResults.common}UUID")
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
            dataSet, f"{ParseProcessLCIAResults.common}generalComment", ""
        )

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

    def __reading_pilot_LCIA_scores(self):
        df = pd.read_excel(r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\eiPilot_fromNode_LCIAscores.xlsx")
        li = df.columns.tolist()
        for i in li:
            if not i.startswith("Unnamed"):
                self.numbers_list.append(i)

    def __read_lcia_results(self, root):
        """[summary]

        Args:
            root ([type]): [description]
            elements_dict ([type]): [description]

        Returns:
            [type]: [description]
        """
        lcia_results_dict = {}
        for result in root.LCIAResults.getchildren():
            if result.referenceToLCIAMethodDataSet.attrib["refObjectId"] in self.numbers_list:
                dict_key = getattr(result.referenceToLCIAMethodDataSet,
                                   f"{ParseProcessLCIAResults.common}shortDescription",
                                   )
                lcia_results_dict[dict_key] = result["meanAmount"]
        return lcia_results_dict

    def _create_df(self, meta_rows):
        """[summary]

        Args:
            meta_rows ([type]): [description]
            exchanges_rows ([type]): [description]
        """
        meta_df = pd.DataFrame(meta_rows)
        write_df_to_excel(files_path.EXCEL_DESTINATION_DIRECTORY, "process_xml_lcia_to_excel.xlsx", meta_df)


obj = ParseProcessLCIAResults(
        r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\EF2_0_Chemicals\processes"
    )
