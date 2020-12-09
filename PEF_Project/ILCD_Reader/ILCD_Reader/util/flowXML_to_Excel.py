from lxml import objectify
from .parse_flowProperties import parse_flowproperties_xml
import os

flowProperites_filePath = r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\7_12_2020_15_50_52_Turkey_energy_dataset_ilcd\flowproperties"


def parse_flow_xml(file_name):
    # xml_parsed_data = collections.defaultdict(list)
    elementsdict = {}
    try:
        with open(file_name, "r", encoding="utf-8") as xml_file:
            root = objectify.parse(xml_file).getroot()
    except IOError as error:
        print(f"Couldnt process {file_name}, {error}")
    elementsdict = read_xml_elements(root)
    return elementsdict


def read_xml_elements(root):
    common_element_namespace = "{http://lca.jrc.it/ILCD/Common}"
    elements_dict = {}
    flow_properties = root.flowProperties
    first_property = next(iter(flow_properties.getchildren()))
    elements_dict["short_description"] = getattr(first_property.referenceToFlowPropertyDataSet, f"{common_element_namespace}shortDescription", "")
    elements_dict["mean_value"] = getattr(first_property, "meanValue", "")

    flowProperty_file_name = first_property.referenceToFlowPropertyDataSet.attrib["refObjectId"]
    flowproperites_file_name = os.path.join(flowProperites_filePath, flowProperty_file_name + ".xml")
    return parse_flowProperties(flowproperites_file_name, elements_dict)


def parse_flowProperties(file_name, elements_dict):
    return parse_flowproperties_xml(file_name, elements_dict)
