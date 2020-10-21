
import os

# BASE_DIR => \ecoinvent_scripts\PEF_Project\ILCD_Reader
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UPDATE_AMOUNTS_FOR_WATER_RESOURCES = os.path.join(BASE_DIR, "Data", "input", "excel, updated amounts for correction_MatrixZ.xlsx")
INDEXES_PEF_ALLOCATION = os.path.join(BASE_DIR, "Data", "input", "excel", "indexes_PEF-phase 2-start_Allocation, cut-off.xlsx")
INDEXES_WATERBALANCE_CORRECTION = os.path.join(BASE_DIR, "Data", "input", "excel", "indexes_waterBalance_correction_20200326.xlsx")
DATASETS_TO_EXCLUDE_FROM_CONVERSION = os.path.join(BASE_DIR, "Data", "input", "excel", "datasets to exclude from amount conversion for water, Resources.xlsx")
WATER_AMOUNT_AND_UNIT_CORRECTION = os.path.join(BASE_DIR, "Data", "input", "excel", "water amount and unit correction_20200326.xlsx")
MATRIX_B_PICKLE = os.path.join(BASE_DIR, "Data", "input", "pickles", "B.pkl")
MATRIX_A_PICKLE = os.path.join(BASE_DIR, "Data", "input", "pickles", "A.pkl")
MATRIX_Z_PICKLE = os.path.join(BASE_DIR, "Data", "input", "pickles", "Z.pkl")
FLOW_CHANGES_SPEC = os.path.join(BASE_DIR, "Data", "input", "excel", "flow_changes_spec.xlsx")
PICKLES_SOURCE_DIRECTORY = os.path.join(BASE_DIR, "Data", "input", "pickles")
EXCEL_SOURCE_DIRECTORY = os.path.join(BASE_DIR, "Data", "input", "excel")
EXCEL_DESTINATION_DIRECTORY = os.path.join(BASE_DIR, "Data", "output", "excels")
PROCESS_FILES_SOURCE_DIR = os.path.join(BASE_DIR, "Data", "input", "processes")
PROCESS_FILES_Destination_DIR = os.path.join(BASE_DIR, "Data", "output", "process_files")
DATASET_LIST_FLOWCHANGES = os.path.join(BASE_DIR, "Data", "input", "excel", "datasetList_flowChange.xlsx")
MAPPED_EXCHANGES = os.path.join(BASE_DIR, "Data", "input", "excel", "TSexchangesToBeMapped_spec.xlsx")
PILOT_THINKSTEP = os.path.join(BASE_DIR, "Data", "input", "excel", "PilotThinkstepDataUsed_spec.xlsx")
