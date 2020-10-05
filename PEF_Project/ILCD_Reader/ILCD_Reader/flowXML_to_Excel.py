from lxml import objectify
import collections


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
    # print(root.flowInformation.dataSetInformation.getchildren())

    common_element_namespace = "{http://lca.jrc.it/ILCD/Common}"
    elements_dict = {}
    flow_properties = root.flowProperties
    first_property = next(iter(flow_properties.getchildren()))
    elements_dict["short_description"] = getattr(first_property.referenceToFlowPropertyDataSet, f"{common_element_namespace}shortDescription", "")
    elements_dict["mean_value"] = getattr(first_property, "meanValue", "")
    # xml_parsed_data['UUID'].append(getattr(root.flowInformation.dataSetInformation, f"{common_element_namespace}UUID"))
    # if hasattr(root.flowInformation.dataSetInformation, '{http://lca.jrc.it/ILCD/Common}synonyms'):
    #     print("yes")
    # else:
    #     print("no")
    # xml_parsed_data['baseName'].append(root.flowInformation.dataSetInformation.name.baseName)

    # xml_parsed_data['synonyms, en'].append(getattr(root.flowInformation.dataSetInformation, "{http://lca.jrc.it/ILCD/Common}synonyms"))
    return elements_dict


# if __name__ == '__main__':
#     folder_path = r'D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\pickles'
#     parse_flow_xml()