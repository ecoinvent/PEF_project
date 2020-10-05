import os
from lxml import objectify
import pandas as pd
import time
from util.file_utils import create_file_list


def write_to_excel(folder, xml_dataframe):
    xml_dataframe.to_excel(os.path.join(folder, "List of ILCD processes, LCI only.xlsx"))


def parse_flow_xml(filename):
    """
      Parsing flow's xml files that comes in ILCD fromats and extract the basename from the file
    """
    folder = r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\00_ILCD_pilot_20190611\flows"
    flow_file = os.path.join(folder, filename)
    try:
        with open(flow_file, "r", encoding="utf-8") as xml_file:
            root = objectify.parse(xml_file).getroot()
    except FileNotFoundError as fnf_error:
        print(fnf_error)
    return root.flowInformation.dataSetInformation.name.baseName


def parse_process_xml():
    """
      Parsing process's xml files that comes in ILCD fromats and extract specific elements from the file
    """

    df_cols = ["process_filename", "process_baseName", "location", "common:shortDescription",
               "exchangeDirection", "meanAmount", "resultingAmount", "flow_baseName"]
    rows = []

    common_element_namespace = "{http://lca.jrc.it/ILCD/Common}"
    file_list = create_file_list(r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\00_ILCD_pilot_20190611\processes, LCI only")

    for file in file_list:
        try:
            with open(file, "r", encoding="utf-8") as xml_f:
                root = objectify.parse(xml_f).getroot()
        except IOError as error:
            print(f"Couldnt process {file}, {error}")

        exchange = root.exchanges.exchange
        flow_filename = exchange.referenceToFlowDataSet.get("uri").split("/")[-1]
        process_file_name = file.split("\\")[-1]
        baseName = root.processInformation.dataSetInformation.name.baseName
        geography = root.processInformation.geography.locationOfOperationSupplyOrProduction.attrib["location"]
        short_description = getattr(exchange.referenceToFlowDataSet, f"{common_element_namespace}shortDescription")
        exchange_direction = exchange.exchangeDirection
        mean_amount = exchange.meanAmount
        result_amount = exchange.resultingAmount
        flow_basename = parse_flow_xml(flow_filename)

        rows.append({"process_filename": process_file_name, "process_baseName": baseName, "location": geography,
                     "common:shortDescription": short_description, "exchangeDirection": exchange_direction,
                     "meanAmount": mean_amount, "resultingAmount": result_amount, "flow_baseName": flow_basename})

    df = pd.DataFrame(rows, columns=df_cols)
    return df


def main():
    t1 = time.perf_counter()
    dirpath = r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\output"
    dataframe = parse_process_xml()
    write_to_excel(dirpath, dataframe)
    t2 = time.perf_counter()
    print(f'time difference {t2 - t1}')


if __name__ == "__main__":
    main()
