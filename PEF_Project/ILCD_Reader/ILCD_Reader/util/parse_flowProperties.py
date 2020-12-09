from lxml import objectify
from .parse_unitGroups import parse_unitGroup_xml
import os

unitGroup_filePath = r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\7_12_2020_15_50_52_Turkey_energy_dataset_ilcd\unitgroups"


def parse_flowproperties_xml(file_name, elements_dict):
    # xml_parsed_data = collections.defaultdict(list)
    root = None
    try:
        with open(file_name, "r", encoding="utf-8") as xml_file:
            root = objectify.parse(xml_file).getroot()
    except IOError as error:
        print(f"Couldnt process {file_name}, {error}")
    return read_xml_elements(root, elements_dict)


def read_xml_elements(root, elements_dict):
    unit_group_id = root.flowPropertiesInformation.quantitativeReference.referenceToReferenceUnitGroup.attrib["refObjectId"]
    unitGroup_file_name = os.path.join(unitGroup_filePath, unit_group_id + ".xml")
    return parse_unitGroup_xml(unitGroup_file_name, elements_dict)
