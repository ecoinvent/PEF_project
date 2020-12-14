import pandas as pd
from lxml import objectify

COMMON = "{http://lca.jrc.it/ILCD/Common}"
PROCESS = "{http://lca.jrc.it/ILCD/Process}"
FLOW = "{http://lca.jrc.it/ILCD/Flow}"


def _read_exchanges(root):
    exchanges_list = []
    ref = int(root.processInformation.quantitativeReference.referenceToReferenceFlow)
    for exchange in root.exchanges.getchildren():
        exchanges_list.append(
            {"referenceToFlowDataSet_refObjectId": exchange.referenceToFlowDataSet.attrib["refObjectId"],
             "location": str(getattr(exchange, "location", "GLO")),
             "isReferenceFlow": (int(exchange.attrib["dataSetInternalID"]) == ref),
             "meanAmount": float(getattr(exchange, "meanAmount", 0)),
             "resultingAmount": float(getattr(exchange, "resultingAmount", 0)),
             "referenceToFlowDataSet.shortDescription": str(getattr(
                 exchange.referenceToFlowDataSet, f"{COMMON}shortDescription"))
             })
    return pd.DataFrame(exchanges_list)


def _read_meta(root):
    return {
        "UUID": str(getattr(root.processInformation.dataSetInformation, f"{COMMON}UUID")),
        "baseName": str(root.processInformation.dataSetInformation.name.baseName),
        "geography": str(root.processInformation.geography.locationOfOperationSupplyOrProduction.attrib["location"])
    }


def _read_flow_files(root):
    dataSet = root.flowInformation.dataSetInformation
    quantitative_Ref = root.flowInformation.quantitativeReference
    elements_dict = {}
    elements_dict["UUID"] = str(getattr(dataSet, f"{COMMON}UUID"))
    elements_dict["baseName"] = str(getattr(dataSet.name, "baseName", ""))
    elementsCategory = getattr(
        dataSet.classificationInformation,
        f"{COMMON}elementaryFlowCategorization",
        "",
    )
    classification = getattr(
        dataSet.classificationInformation, f"{COMMON}classification", ""
    )

    if type(elementsCategory) != str:
        for elem in elementsCategory.getchildren():
            level = elem.attrib["level"]
            elements_dict[
                f"elementaryFlowCategorization.category_level{level}"
            ] = str(elem)

    elif type(classification) != str:
        for elem in classification.getchildren():
            level = elem.attrib["level"]
            elements_dict[f"classification.class_level{level}"] = elem

    elements_dict["CASNumber"] = getattr(dataSet, "CASNumber", "")
    elements_dict["referenceToReferenceFlowProperty"] = getattr(
        quantitative_Ref, "referenceToReferenceFlowProperty", ""
    )
    elements_dict["typeOfDataSet"] = str(getattr(
        root.modellingAndValidation.LCIMethod, "typeOfDataSet", ""
    ))
    return elements_dict


def _open_xml_file(file):
    with open(file, "r", encoding="utf-8") as xml_f:
        return objectify.parse(xml_f).getroot()


def _read_exchanges_from_file(file):
    return _read_exchanges(_open_xml_file(file))


def _read_exchanges_from_file_with_meta(file):
    root = _open_xml_file(file)
    return {"exchanges": _read_exchanges(root),
            "meta": _read_meta(root)}


def _read_flow_from_file(file):
    root = _open_xml_file(file)
    return _read_flow_files(root)
