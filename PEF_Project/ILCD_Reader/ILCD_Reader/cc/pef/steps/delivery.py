import os.path as path
from lxml import objectify
from lxml import etree
import pandas as pd
import os
import logging

log = logging.getLogger(__name__)


def open_xml_file(file):
    with open(file, "r", encoding="utf-8") as xml_f:
        return objectify.parse(xml_f).getroot()


COMMON = "{http://lca.jrc.it/ILCD/Common}"
PROCESS = "{http://lca.jrc.it/ILCD/Process}"
XML = "{http://www.w3.org/XML/1998/namespace}"


def create_exchange_element(dsInternalId, row):
    exchange = objectify.Element("exchange", attrib={"dataSetInternalID": str(dsInternalId)})
    id = str(row["ID"])
    descr = row["FLOW_descr"]
    amount = format(row["v"], "10.4E")
    referenceToFlowDataSet = objectify.SubElement(exchange,
                                                  "referenceToFlowDataSet",
                                                  attrib={"refObjectId": id,
                                                          "type": "flow data set",
                                                          "uri": "../flows/" + id + ".xml",
                                                          "version": "03.01.000"})
    short_description = objectify.SubElement(referenceToFlowDataSet,
                                             COMMON + "shortDescription",
                                             attrib={XML + "lang": "en"})
    short_description._setText(descr)
    objectify.SubElement(exchange, "exchangeDirection")._setText("Output")
    objectify.SubElement(exchange, "meanAmount")._setText(amount)
    objectify.SubElement(exchange, "resultingAmount")._setText(amount)
    objectify.SubElement(exchange, "relativeStandardDeviation95In")._setText("0.0")
    objectify.SubElement(exchange, "dataSourceType")._setText("Mixed primary / secondary")
    objectify.SubElement(exchange, "dataDerivationTypeStatus")._setText("Calculated")
    if not pd.isna(row["location"]):
        objectify.SubElement(exchange, "location")._setText(str(row["location"]))
    objectify.deannotate(exchange)
    return exchange


def create_lcia(row):
    el = objectify.Element("LCIAResult")
    id = str(row["LCIAMethod_uuid"])
    amount = format(row["v"], "10.4E")
    descr = row["LCIAMethod_name"]
    ref = objectify.SubElement(el,
                               "referenceToLCIAMethodDataSet",
                               attrib={"refObjectId": id,
                                       "type": "LCIA method data set",
                                       "uri": "../lciamethods/" + id + ".xml",
                                       "version": "03.01.000"}

                               )
    short_description = objectify.SubElement(el,
                                             COMMON + "shortDescription",
                                             attrib={XML + "lang": "en"})
    short_description._setText(descr)
    objectify.SubElement(el, "meanAmount")._setText(amount)
    return el


def add_exchanges(root, mapped_exchanges):
    init_len = len(root.exchanges)
    nzz = mapped_exchanges.loc[mapped_exchanges["v"] != 0]
    for i in range(len(nzz)):
        internalId = i + init_len
        ex = create_exchange_element(internalId, nzz.iloc[i])
        root.exchanges.append(ex)


def add_lcia(root, lcias):
    nzz = lcias.loc[lcias["TO BE REPORTED"] == "yes"]
    for i in range(len(nzz)):
        el = create_lcia(nzz.iloc[i])
        root.LCIAResults.append(el)


def write_to_file(root, file):
    objectify.deannotate(root, xsi_nil=True)
    etree.cleanup_namespaces(root)
    obj_xml = etree.tostring(
        root, encoding="utf-8",
        standalone=False,
        pretty_print=True,
        xml_declaration=True)
    with open(file, "wb") as xml_writer:
        xml_writer.write(obj_xml)


def fill(in_path, out_path, lci, lcia, ilcd_id, lci_v, lcia_v):
    os.makedirs(out_path, exist_ok=True)
    root = open_xml_file(path.join(in_path, ilcd_id + ".xml"))
    lci["v"] = lci_v
    lcia["v"] = lcia_v
    add_exchanges(root, lci.loc[~pd.isna(lci["ID"])])
    add_lcia(root, lcia)
    write_to_file(root, out_path + ilcd_id + ".xml")


def buildmapping(B_idx, mapping_ef):
    log.info("build elementary flow to ILCD nomenclature mapping")
    mapping = pd.read_excel(mapping_ef, sheet_name="eiEF3.0_mappinf_EF3.0", skiprows=[0])
    mapping = mapping[["exchange name", "compartment", "subcompartment",
                       "ID", "location", "FLOW_name", "FLOW_class0", "FLOW_class1", "FLOW_class2", "unit"]]
    mapping = B_idx.merge(
        mapping.rename(columns={"exchange name": "name"}),
        on=["name", "compartment", "subcompartment"],
        how="outer"
    ).drop_duplicates()

    mapping["FLOW_descr"] = mapping.loc[~pd.isna(mapping["FLOW_name"])][
        ["FLOW_name", "FLOW_class0", "FLOW_class1"]].agg(', '.join, axis=1)
    mapping.loc[~pd.isna(mapping["FLOW_class2"]), "FLOW_descr"] = mapping.loc[~pd.isna(mapping["FLOW_class2"])][
        ["FLOW_name", "FLOW_class0", "FLOW_class1", "FLOW_class2"]].agg(', '.join, axis=1)
    return mapping.drop_duplicates()


def fill_xml_template(conf, d):
    deliverables, B_idx, C_idx = [d[k] for k in ("deliverables", "B_idx", "C_idx")]
    in_path, out_path, lcia_ef3, mapping_ef = [conf[k] for k in
                                               ("XML_TEMPLATE_FOLDER", "XML_OUTPUT_FOLDER", "LCIA_EF3", "MAPPING_EF")]
    log.info("xml fill start, deliverables: %s", len(deliverables))
    lcia_methods = pd.read_excel(lcia_ef3)
    lcia = lcia_methods.merge(C_idx, left_on=["LCIAMethod_name"], right_on=["indicator"])
    lci = buildmapping(B_idx, mapping_ef)
    for (k, v) in deliverables.iterrows():
        log.debug("write %s - %s - %s", v["activityName"], v["geography"], v["reference product"])
        log.debug("blackbox:%s.xml, lci:%s.xml, elementary: %s.xml", v["blackboxId"],v["lciId"],v["elementaryId"])
        fill(in_path,
             out_path + "blackbox/",
             lci,
             lcia,
             v["blackboxId"],
             v["black_box_lci"],
             v["black_box_lcia"])
        log.debug("  - lci %s", v["lciId"])
        fill(in_path,
             out_path + "lci/",
             lci,
             lcia,
             v["lciId"],
             v["full_lci"],
             v["full_lcia"])
        log.debug("  - elementary %s", v["elementaryId"])
        fill(in_path,
             out_path + "elementary/",
             lci,
             lcia,
             v["elementaryId"],
             v["elementary_lci"],
             v["elementary_lcia"])
    log.info("xml fill completed")
    return d
