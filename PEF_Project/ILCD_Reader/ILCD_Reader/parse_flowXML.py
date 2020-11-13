import pandas as pd
from util.file_utils import create_file_list
from lxml import objectify
from tqdm import tqdm
import sys


class ParseXML:
    __slots__ = ["root"]

    def __init__(self, root):
        self.root = root

    @classmethod
    def parse_file(cls, xml_file):
        """[summary]

        Args:
            xml_file ([type]): [description]

        Returns:
            [type]: [description]
        """
       
        parsedFile = objectify.parse(xml_file)
        root = parsedFile.getroot()
        return cls(root)


class FlowFiles:
    common = "{http://lca.jrc.it/ILCD/Common}"
    flow = "{http://lca.jrc.it/ILCD/Flow}"

    def __init__(self, source_dir, destination_dir):
        self.source_dir = source_dir
        self.destination_dir = destination_dir
        self.files = []
        self.df, self.df1, self.df2 = None, None, None

    @classmethod
    def input_fromUser(cls):
        """[summary]

        Returns:
            [type]: [description]
        """
        return cls(
            input("Please Enter Path of Source folder that contain source files: "),
            input("Please Enter Path of Destination folder to dump the Excel file: "),
        )

    def parse_flows(self):
        """[summary]
        """
        self.__create_name_list()
        flow_elements_list = []
        flowproperties_list = []
        # df_columns = [
        #     "filename",
        #     "UUID",
        #     "baseName",
        #     "elementaryFlowCategorization.category_level0",
        #     "elementaryFlowCategorization.category_level1",
        #     "elementaryFlowCategorization.category_level2",
        #     "elementaryFlowCategorization.category_level3",
        #     "elementaryFlowCategorization.category_level4",
        #     "classification.class_level0",
        #     "classification.class_level1",
        #     "classification.class_level2",
        #     "classification.class_level3",
        #     "CASNumber",
        #     "referenceToReferenceFlowProperty",
        #     "typeOfDataSet",
        # ]
        df_columns = None
        elements_dict = {}
        for file in tqdm(self.files):
            parsed_file = self.__parse_XMLroot(file)
            file_name = file.split("\\")[-1]
            elements_dict = self.__read_flow_files(file_name, parsed_file.root)
            flow_elements_list.append(elements_dict)
            properties_list = self.__read_flowProperties_in_flow(parsed_file.root)
            flowproperties_list.extend(properties_list)
        self.__convert_to_df(df_columns, flow_elements_list, flowproperties_list)

    def __parse_XMLroot(self, file):
        """[summary]

        Args:
            file ([type]): [description]

        Returns:
            [type]: [description]
        """
        try:
            with open(file, encoding="UTF-8") as xml_file:
                print("opening file", xml_file)
                parsed_file = ParseXML.parse_file(xml_file)
        except IOError as error:
            print(f"Couldnt process {file}, {error}")
        return parsed_file

    def __create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_dir)

    def parse_flowproperties(self):
        """[summary]
        """
        self.__create_name_list()
        flowproperties_elements_list = []
        elements_dict = {}
        for file in tqdm(self.files):
            parsed_file = self.__parse_XMLroot(file)
            file_name = file.split("\\")[-1]
            elements_dict = self.__read_flowProperties_files(
                file_name, parsed_file.root
            )
            flowproperties_elements_list.append(elements_dict)
        self.__convert_to_df(flowproperties_elements_list)

    def parse_unit_files(self):
        """[summary]
        """
        self.__create_name_list()
        unit_elements_list = []
        unit_properties_list = []
        elements_dict = {}
        for file in tqdm(self.files):
            parsed_file = self.__parse_XMLroot(file)
            file_name = file.split("\\")[-1]
            elements_dict = self.__read_unitGroups_files(file_name, parsed_file.root)
            if elements_dict == {}:
                continue
            unit_elements_list.append(elements_dict)
            properties_list = self.__read_units_in_unitGroups(parsed_file.root)
            unit_properties_list.extend(properties_list)
        self.__convert_to_df(unit_elements_list, unit_properties_list)

    @staticmethod
    def __validate_flow_parent_elements(root, file_name):
        """[summary]

        Args:
            root ([type]): [description]
            file_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        try:
            dataSet = root.flowInformation.dataSetInformation
            quantitative_Ref = root.flowInformation.quantitativeReference
        except AttributeError as err:
            print()
            print(f"Error: {err} @ fileName: {file_name}")
            sys.exit(1)
        return dataSet, quantitative_Ref

    @staticmethod
    def __validate_flowproperties_parent_elements(root, file_name):
        try:
            dataSet = root.flowPropertiesInformation.dataSetInformation
            quantitative_Ref = root.flowPropertiesInformation.quantitativeReference
        except AttributeError as err:
            print()
            print(f"Error: {err} @ fileName: {file_name}")
            sys.exit(1)
        return dataSet, quantitative_Ref

    @staticmethod
    def __validate_unitGroups_parent_elements(root, file_name):
        try:
            dataSet = root.unitGroupInformation.dataSetInformation
            quantitative_Ref = root.unitGroupInformation.quantitativeReference
        except AttributeError as err:
            print()
            print(f"Error: {err} @ fileName: {file_name}")
            sys.exit(1)
        return dataSet, quantitative_Ref

    def __read_flow_files(self, file_name, root):
        """[summary]

        Args:
            file_name ([type]): [description]
            root ([type]): [description]

        Returns:
            [type]: [description]
        """
        elements_dict = {}
        dataSet, quantitative_Ref = FlowFiles.__validate_flow_parent_elements(
            root, file_name
        )
        elements_dict["filename"] = file_name
        elements_dict["UUID"] = getattr(dataSet, f"{FlowFiles.common}UUID")
        elements_dict["baseName"] = getattr(dataSet.name, "baseName", "")
        elementsCategory = getattr(
            dataSet.classificationInformation,
            f"{FlowFiles.common}elementaryFlowCategorization",
            "",
        )
        classification = getattr(
            dataSet.classificationInformation, f"{FlowFiles.common}classification", ""
        )

        if type(elementsCategory) != str:
            for elem in elementsCategory.getchildren():
                level = elem.attrib["level"]
                elements_dict[
                    f"elementaryFlowCategorization.category_level{level}"
                ] = elem

        elif type(classification) != str:
            for elem in classification.getchildren():
                level = elem.attrib["level"]
                elements_dict[f"classification.class_level{level}"] = elem

        elements_dict["CASNumber"] = getattr(dataSet, "CASNumber", "")
        elements_dict["referenceToReferenceFlowProperty"] = getattr(
            quantitative_Ref, "referenceToReferenceFlowProperty", ""
        )
        elements_dict["typeOfDataSet"] = getattr(
            root.modellingAndValidation.LCIMethod, "typeOfDataSet", ""
        )
        return elements_dict

    def __read_flowProperties_in_flow(self, root):
        """[summary]

        Args:
            root ([type]): [description]

        Returns:
            [type]: [description]
        """
        properties_list = []
        dataSet = root.flowInformation.dataSetInformation
        for elem in root.flowProperties.getchildren():
            properties_dict = {}
            properties_dict["UUID"] = getattr(dataSet, f"{FlowFiles.common}UUID", "")
            properties_dict["baseName"] = getattr(dataSet.name, "baseName", "")
            properties_dict["flowProperty_dataSetInternalID"] = elem.attrib[
                "dataSetInternalID"
            ]
            properties_dict[
                "referenceToFlowPropertyDataSet_refObjectId"
            ] = elem.referenceToFlowPropertyDataSet.attrib["refObjectId"]
            properties_dict[
                "referenceToFlowPropertyDataSet.shortDescription"
            ] = getattr(
                elem.referenceToFlowPropertyDataSet,
                f"{FlowFiles.common}shortDescription",
            )
            properties_dict["flowProperty.meanValue"] = getattr(elem, "meanValue", "")
            properties_list.append(properties_dict)
        return properties_list

    def __read_flowProperties_files(self, file_name, root):
        """[summary]

        Args:
            file_name ([type]): [description]
            root ([type]): [description]

        Returns:
            [type]: [description]
        """
        elements_dict = {}
        dataSet, quantitative_Ref = FlowFiles.__validate_flowproperties_parent_elements(
            root, file_name
        )
        reference = quantitative_Ref.referenceToReferenceUnitGroup
        elements_dict["filename"] = file_name
        elements_dict["UUID"] = getattr(dataSet, f"{FlowFiles.common}UUID")
        elements_dict["dataSetInformation.common:name"] = getattr(
            dataSet, f"{FlowFiles.common}name"
        )
        elements_dict["referenceToReferenceUnitGroup_refObjectId"] = reference.attrib[
            "refObjectId"
        ]
        elements_dict["referenceToReferenceUnitGroup.shortDescription"] = getattr(
            reference, f"{FlowFiles.common}shortDescription"
        )
        return elements_dict

    def __read_unitGroups_files(self, file_name, root):
        """[summary]

        Args:
            file_name ([type]): [description]
            root ([type]): [description]

        Returns:
            [type]: [description]
        """
        elements_dict = {}
        elements_dict["filename"] = file_name
        dataSet, quantitative_Ref = self.__validate_unitGroups_parent_elements(
            root, file_name
        )
        elements_dict["UUID"] = getattr(dataSet, f"{FlowFiles.common}UUID")
        elements_dict["dataSetInformation.common:name"] = getattr(
            dataSet, f"{FlowFiles.common}name"
        )
        elements_dict["referenceToReferenceUnit"] = getattr(
            quantitative_Ref, "referenceToReferenceUnit", ""
        )
        return elements_dict

    def __read_units_in_unitGroups(self, root):
        """[summary]

        Args:
            root ([type]): [description]

        Returns:
            [type]: [description]
        """
        unitGroups_list = []
        dataSet = root.unitGroupInformation.dataSetInformation
        for unit in root.units.getchildren():
            units_dict = {}
            units_dict["dataSetInformation.common:name"] = getattr(
                dataSet, f"{FlowFiles.common}name"
            )
            units_dict["units.unit_dataSetInternalID"] = unit.attrib[
                "dataSetInternalID"
            ]
            units_dict["units.unit.name"] = getattr(unit, "name", "")
            units_dict["units.unit.meanValue"] = getattr(unit, "meanValue", "")
            units_dict["units.unit.generalComment"] = getattr(
                unit, "generalComment", ""
            )
            unitGroups_list.append(units_dict)
        return unitGroups_list

    def __convert_to_df(self, columns=None, *args):
        """[summary]

        Args:
            columns ([type], optional): [description]. Defaults to None.
        """
        if columns is not None and len(args) > 1:
            self.df1 = pd.DataFrame(args[0])
            print(self.df1.info())
            self.df1 = self.df1[columns]
            self.df2 = pd.DataFrame(args[1])
            self.__save2Excel(self.df1, self.df2)
        elif columns is None and len(args) > 1:
            self.df1 = pd.DataFrame(args[0])
            self.df2 = pd.DataFrame(args[1])
            self.__save2Excel(self.df1, self.df2)
        else:
            self.df = pd.DataFrame(args[0])
            self.__save2Excel(self.df)

    def __save2Excel(self, *args):
        """[summary]
        """
        with pd.ExcelWriter(
            f"{self.destination_dir}\\flows_to_excel.xlsx"
        ) as writer:
            for i, df in enumerate(args):
                df.to_excel(writer, sheet_name=f"sheet {str(i)} ", index=False)


def main():
    files = FlowFiles.input_fromUser()
    files.parse_flows()
    # files.parse_flowproperties()
    # files.parse_unit_files()


if __name__ == "__main__":
    main()
