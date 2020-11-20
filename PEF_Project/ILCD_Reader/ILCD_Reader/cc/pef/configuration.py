import pef.steps.cleanup as cl
import pef.steps.replacement as rep
import pef.steps.disaggregation as di
import pef.steps.delivery as de


p = "data/"
A_PKL = p + "matrix/A.pkl"
B_PKL = p + "matrix/B.pkl"
C_PKL = p + "matrix/C.pkl"
INDEX_FILE = p + "matrix/indexes.xlsx"
THINKSTEP_PROCESSES = p + "ilcd/EF_30_all_datasets_TSv2/ILCD/processes/"
THINKSTEP_FLOWS = p + "ilcd/EF_30_all_datasets_TSv2/ILCD/flows/"
TS_EXCHANGES_TO_BE_MAPPED = p + "config/TSexchangesToBeMapped_spec.xlsx"
TS_INTEGRATE_EOL_PACK = p + "config/09.2_TSintegrateEoL&pack.xlsx"
PILOT_TS_DATA_USED = p + "config/PilotThinkstepDataUsed_spec_with_conv.xlsx"
XML_TEMPLATE_FOLDER = p + "ilcd/deliverable_templates/ILCD/processes/"
LCIA_EF3 = p + "config/LCIAmethods.xlsx"
MAPPING_EF = p + "config/mapping_eiEF_to_EF3.0_From_matrixC_20201001.xlsx"
DELIVERABLES = p + "config/pef_matrix_to_file_id.xlsx"
XML_OUTPUT_FOLDER = p + "output/deliverables/"
REVIEW_OUTPUT_FOLDER = p + "output/steps/"

_steps = [
    {
        "step_id": "step_0",
        "step_fn": cl.cleanup,
        "CALC_CONTRIB": True,
        "STORE_CONTRIB": True,
        "CALC_LCIA": True,
        "STORE_LCIA": True,
        "STORE_MATRICES": True
    },
    {
        "step_id": "step_9a",
        "step_fn": rep.energy_and_transport,
        "THINKSTEP_PROCESSES": THINKSTEP_PROCESSES,
        "TS_EXCHANGES_TO_BE_MAPPED": TS_EXCHANGES_TO_BE_MAPPED,
        "PILOT_TS_DATA_USED": PILOT_TS_DATA_USED,
    },
    {
        "step_id": "step_9b",
        "step_fn": rep.packaging_and_eol,
        "THINKSTEP_PROCESSES": THINKSTEP_PROCESSES,
        "TS_EXCHANGES_TO_BE_MAPPED": TS_EXCHANGES_TO_BE_MAPPED,
        "TS_INTEGRATE_EOL_PACK": TS_INTEGRATE_EOL_PACK,
    },
    {
        "step_id": "step_12a",
        "step_fn": di.disaggregation,
    },
    {
        "step_id": "step_12b",
        "step_fn": de.fill_xml_template,
        "XML_TEMPLATE_FOLDER": XML_TEMPLATE_FOLDER,
        "XML_OUTPUT_FOLDER": XML_OUTPUT_FOLDER,
        "LCIA_EF3": LCIA_EF3,
        "MAPPING_EF": MAPPING_EF,
        "CALC_CONTRIB": True,
        "STORE_CONTRIB": True,
        "CALC_LCIA": True,
        "STORE_LCIA": True,
        "STORE_MATRICES": True
    }

]
pipeline_config = {
    "REVIEW_OUTPUT_FOLDER": REVIEW_OUTPUT_FOLDER,
    "CALC_CONTRIB": False,
    "STORE_CONTRIB": False,
    "CALC_LCIA": True,
    "STORE_LCIA": True,
    "STORE_MATRICES": True
}

steps = [dict(x, **pipeline_config) for x in _steps]