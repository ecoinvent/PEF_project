from parse_process_exchanges import ParseProcessExchanges
from util.file_utils import create_file_list
import files_path
import pandas as pd
import os


class ThinkstepIntegration:
    def __init__(self):
        self.exchange_mapped_df, self.processes_df, self.index_pef_df = None, None, None
        self.files = []

    def start_processing(self):
        # self.create_exchanges_df()
        mapped_exchanges_df = self.__read_mapping_template()
        index_pef_ee = self.__read_index_PEF("ee")
        self.fetch_list_of_files("D:\\ecoinvent_scripts\\output")
        for i, file in enumerate(self.files, start=1):
            process_exchanges_df = self.read_exchanges_csv(i, file)
            process_mappingTable_df = self.__merge_processExchanges_with_mapping_template(process_exchanges_df, mapped_exchanges_df)
            second_merged_df = self.__merge_process_mapping_df_with_indexes(process_mappingTable_df, index_pef_ee)
            self.__write_to_CSV(second_merged_df, "D:\\ecoinvent_scripts\\output\\merged", i)
        self.fetch_list_of_files("D:\\ecoinvent_scripts\\output\\merged")
        print(self.files)
        thinkstep_pilot_df = self.__read_pilot_thinkstep_template()
        index_pef_ie = self.__read_index_PEF("ie")
        for i, file in enumerate(self.files, start=1):
            merged_file_df = self.__read_generated_merged_templates(file)
            merged_thinkstep_df = self.__merge_generateFiles_with_thinkstep_pilot(thinkstep_pilot_df, merged_file_df)
            merged_thinkstep_ie_df = self.__merge_thinkstep_with_index_ie(merged_thinkstep_df, index_pef_ie)
            self.__write_to_CSV(merged_thinkstep_ie_df, "D:\\ecoinvent_scripts\\output\\merges_results_thinkstep_pilot", i)

    def create_exchanges_df(self):
        parse_obj = ParseProcessExchanges(files_path.PROCESS_FILES_SOURCE_DIR)
        parse_obj.parse_files()

    def fetch_list_of_files(self, src_dir):
        ext = "csv"
        self.files = create_file_list(src_dir, ext)

    def read_exchanges_csv(self, index, file):
        print(f"processing number {index} file")
        exchanges_df = pd.read_csv(file)
        return exchanges_df

    def __read_index_PEF(self, sheet):
        print("read index PEF file...")
        index_pef_df = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name=sheet)
        return index_pef_df

    def __read_mapping_template(self):
        print("read mapping file...")
        mapped_exchanges_df = pd.read_excel(files_path.MAPPED_EXCHANGES, sheet_name="mapped", header=1)
        return mapped_exchanges_df

    def __merge_processExchanges_with_mapping_template(self, process_exchanges_df, exchanges_mapped_df):
        print("merging process exchanges with mapping template started...")
        first_merged_df = pd.merge(
            process_exchanges_df,
            exchanges_mapped_df,
            how="left",
            left_on=["referenceToFlowDataSet_refObjectId", "location"],
            right_on=["referenceToFlowDataSet_refObjectId", "location"],
        )
        return first_merged_df

    def __merge_process_mapping_df_with_indexes(self, first_merged_df, index_pef_ee):
        print("merging the result of process, mapping template with indexes ee...")
        second_merged_df = pd.merge(
            first_merged_df,
            index_pef_ee,
            how="left",
            left_on=["exchange name", "compartment", "subcompartment"],
            right_on=["name", "compartment", "subcompartment"],
        )
        return second_merged_df

    def __read_pilot_thinkstep_template(self):
        print("Reading Pilot Thinkstep...")
        thinkstep_df = pd.read_excel(files_path.PILOT_THINKSTEP, header=1)
        return thinkstep_df

    def __read_generated_merged_templates(self, file):
        print("start reading the generated merged templates...")
        cols = ["filename", "UUID", "resultingAmount", "exchange name", "index"]
        merged_file = pd.read_csv(file, usecols=cols)
        return merged_file

    def __merge_generateFiles_with_thinkstep_pilot(self, thinkstep_df, merged_files_with_eeIndex):
        print("Started merging with thinkstep pilot...")
        merged_thinkstep_df = pd.merge(
            thinkstep_df,
            merged_files_with_eeIndex,
            how="right",
            left_on=["filename"],
            right_on=["UUID"],
        )
        return merged_thinkstep_df

    def __merge_thinkstep_with_index_ie(self, merged_thinkstep_df, index_ie_df):
        print("Started merging with PEF index ie...")
        merged_thinkstep_index_ie_df = pd.merge(
            merged_thinkstep_df,
            index_ie_df,
            how="left",
            left_on=["activityName", "geography", "reference product"],
            right_on=["activityName", "geography", "product"],
        )
        return merged_thinkstep_index_ie_df

    def __write_to_CSV(self, merged_file, output_dir, index):
        print("writing to csv...")
        file_path = os.path.join(output_dir, f"file{index}.csv")
        merged_file.to_csv(file_path)


obj = ThinkstepIntegration()
obj.start_processing()
