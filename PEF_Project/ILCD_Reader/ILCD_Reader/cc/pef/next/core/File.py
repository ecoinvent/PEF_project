import logging
import os
from enum import Enum
from os.path import join

log = logging.getLogger(__name__)

BASE_DIR = join(os.path.dirname(__file__), "../../../data/")


class File(Enum):
    A_PKL = join(BASE_DIR, "matrix", "3.7.1", "A.pkl")
    B_PKL = join(BASE_DIR, "matrix", "3.7.1", "B.pkl")
    C_PKL = join(BASE_DIR, "matrix", "3.7.1", "C.pkl")
    INDEX_FILE = join(BASE_DIR, "matrix", "3.7.1", "indexes.xlsx")
    THINKSTEP_PROCESSES = join(BASE_DIR, "ilcd/EF_30_all_datasets_TSv2/ILCD/processes/")
    THINKSTEP_FLOWS = join(BASE_DIR, "ilcd/EF_30_all_datasets_TSv2/ILCD/flows/")
    TS_EXCHANGES_TO_BE_MAPPED = join(BASE_DIR, "config/TSexchangesToBeMapped_spec.xlsx")
    TS_INTEGRATE_EOL_PACK = join(BASE_DIR, "config/09.2_TSintegrateEoL&pack.xlsx")
    PILOT_TS_DATA_USED = join(BASE_DIR, "config/PilotThinkstepDataUsed_spec_with_conv.xlsx")
    XML_TEMPLATE_FOLDER = join(BASE_DIR, "ilcd/deliverable_templates/ILCD/processes/")
    LCIA_EF3 = join(BASE_DIR, "config/LCIAmethods.xlsx")
    LCIA_CF_EF3 = join(BASE_DIR, "config/EF3_cf.csv")
    MAPPING_EF = join(BASE_DIR, "config/mapping_eiEF_to_EF3.0_From_matrixC_20201001.xlsx")
    DELIVERABLES = join(BASE_DIR, "config/pef_matrix_to_file_id.xlsx")
    XML_OUTPUT_FOLDER = join(BASE_DIR, "output_next/deliverables/")
    REVIEW_OUTPUT_FOLDER = join(BASE_DIR, "output_next/steps/")
    GLAD_MAPPING = join(BASE_DIR, "/home/cedric/test_Ecoinvent_3.6_EFv.3.0.xlsx")
    CACHE = join(BASE_DIR, "cache")


for f in File:
    if not os.path.exists(f.value):
        log.warn("%s file not found, path: %s", f.name, f.value)
