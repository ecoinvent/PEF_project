from lxml import objectify


def parse_unitGroup_xml(file_name, elements_dict):
    # xml_parsed_data = collections.defaultdict(list)
    root = None
    try:
        with open(file_name, "r", encoding="utf-8") as xml_file:
            root = objectify.parse(xml_file).getroot()
    except IOError as error:
        print(f"Couldnt process {file_name}, {error}")
    return read_xml_elements(root, elements_dict)


def read_xml_elements(root, elements_dict):
    units = root.units
    units_childs = units.getchildren()
    for unit in units_childs:
        print(unit.attrib["dataSetInternalID"])
        if unit.attrib["dataSetInternalID"] == "0":
            elements_dict["unit"] = unit.name.text
    return elements_dict
