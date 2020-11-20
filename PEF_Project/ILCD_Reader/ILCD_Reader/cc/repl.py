import pandas as pd
import numpy as np
import pickle
import scipy.sparse as sp
import pef.steps.disaggregation as d
import pef.steps.cleanup as c

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

A = pickle.load(open(A_PKL, "rb"))
B = pickle.load(open(B_PKL, "rb"))
C = pickle.load(open(C_PKL, "rb"))
deliverables = pd.read_excel(DELIVERABLES)
A_idx = pd.read_excel(INDEX_FILE, sheet_name="ie")
B_idx = pd.read_excel(INDEX_FILE, sheet_name="ee")
C_idx = pd.read_excel(INDEX_FILE, sheet_name="LCIA")


data = {
    "A": A,
    "B": B,
    "C": C,
    "A_idx": A_idx,
    "B_idx": B_idx,
    "C_idx": C_idx,
    "deliverables": deliverables
}

data
d.disaggregation({},c.cleanup({},data))


