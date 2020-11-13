from lxml import objectify


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
    return elements_dict
