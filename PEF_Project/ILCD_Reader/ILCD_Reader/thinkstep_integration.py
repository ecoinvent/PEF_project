from parse_process_exchanges import ParseProcessExchanges
from util.file_utils import create_file_list, load_pkl_file, dump_to_pickle
import files_path
import pandas as pd
import os
import numpy as np
from scipy import sparse
import pickle


class ThinkstepIntegration:
    def __init__(self):
        self.exchange_mapped_df, self.processes_df, self.index_pef_df = None, None, None
        self.files = []

    def start_processing(self):
        # self.create_exchanges_df()
        # mapped_exchanges_df = self.__read_mapping_template()
        # index_pef_ee = self.__read_index_PEF("ee")
        # temp_df = None
        # self.fetch_list_of_files("D:\\ecoinvent_scripts\\output\\smaller")
        # for i, file in enumerate(self.files, start=1):
        #     process_exchanges_df = self.read_exchanges_csv(i, file)
        #     process_mappingTable_df = self.__merge_processExchanges_with_mapping_template(process_exchanges_df, mapped_exchanges_df)
        #     concatenated_df = pd.concat([temp_df, process_mappingTable_df])
        #     temp_df = concatenated_df
        # grouped_df = self.__groupby_dataframe(temp_df)
        # print("grouped", grouped_df.columns)
        # second_merged_df = self.__merge_process_mapping_df_with_indexes(grouped_df, index_pef_ee)
        # print("nulling", second_merged_df['index'].isnull().sum())
        # null_df = second_merged_df[second_merged_df['index'].isnull()]
        # null_df.drop_duplicates(subset=["exchange name", "compartment", "subcompartment"], keep='first', inplace=True)
        # print("nulling again", second_merged_df['index'].isnull().sum())
        # print("index max", second_merged_df["index"].max())
        # self.__write_to_Excel(null_df, "D:\\ecoinvent_scripts\\output\\merged\\checking")
        # self.__write_to_csv(second_merged_df, "D:\\ecoinvent_scripts\\output\\merged")

        # self.fetch_list_of_files("D:\\ecoinvent_scripts\\output\\merged", "csv")

        # thinkstep_pilot_df = self.__read_pilot_thinkstep_template()
        # merged_file_df = self.__read_generated_merged_templates("D:\\ecoinvent_scripts\\output\\merged\\file0.csv")
        # for i, file in enumerate(self.files, start=1):
        #     merged_thinkstep_df = self.__merge_generateFiles_with_thinkstep_pilot(thinkstep_pilot_df, merged_file_df)
        #     # merged_thinkstep_ie_df = self.__merge_thinkstep_with_index_ie(merged_thinkstep_df, index_pef_ie)
        #     self.__write_to_csv(merged_thinkstep_df, "D:\\ecoinvent_scripts\\output\\merges_results_thinkstep_pilot", i)

        # self.__update_matrix_B()
        # self.__update_matrix_B_with_resultiAmount()
        # self.__scaling_matrix_B()
        # self.__read_process_flows_excel()

        self.__update_matrix_A_and_Z()

    def create_exchanges_df(self):
        parse_obj = ParseProcessExchanges(files_path.PROCESS_FILES_SOURCE_DIR)
        parse_obj.parse_files()

    def fetch_list_of_files(self, src_dir, ext="csv"):
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
        first_merged_df.dropna(axis=0, how="any", subset=["exchange name", "compartment", "subcompartment"], inplace=True)
        return first_merged_df

    def __groupby_dataframe(self, dataframe):
        print("Grouping.... ")
        df_groupby_role = dataframe.groupby(["UUID", "exchange name", "compartment", "subcompartment", "unit"])
        df_groupby_role = df_groupby_role[["UUID", "exchange name", "compartment", "subcompartment", "unit", "resultingAmount"]]
        df_groupby_role_sum = df_groupby_role.sum().reset_index()
        return df_groupby_role_sum

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

    def __read_pilot_thinkstep_template(self, type_replacement="1 to 1"):
        print("Reading Pilot Thinkstep...")
        thinkstep_df = pd.read_excel(files_path.PILOT_THINKSTEP, header=1)
        filt = (thinkstep_df["type of replacement"] == type_replacement)
        thinkstep_df = thinkstep_df.loc[filt, :]
        print("writing filtered...")
        thinkstep_df.to_excel(r"D:\ecoinvent_scripts\output\merges_results_thinkstep_pilot\type_filtered.xlsx",  index=False)
        return thinkstep_df

    def __read_generated_merged_templates(self, file):
        print("start reading the generated merged templates...")
        # cols = ["filename", "UUID", "resultingAmount", "exchange name", "index",
        #         "FLOW_name", "FLOW_class0", "FLOW_class1", "FLOW_class2"]
        merged_file = pd.read_csv(file)
        return merged_file

    def __merge_generateFiles_with_thinkstep_pilot(self, thinkstep_df, merged_files_with_eeIndex):
        print("Started merging with thinkstep pilot...")
        merged_thinkstep_df = pd.merge(
            thinkstep_df,
            merged_files_with_eeIndex,
            how="left",
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
        print("final merged", merged_thinkstep_index_ie_df.columns)
        return merged_thinkstep_index_ie_df

    def __write_to_Excel(self, merged_file, output_dir, index=0):
        print("writing to excel...")
        file_path = os.path.join(output_dir, f"file{index}.xlsx")
        merged_file.to_excel(file_path,  index=False)

    def __write_to_csv(self, merged_file, output_dir, index=0):
        print("writing to csv...")
        file_path = os.path.join(output_dir, f"file{index}.csv")
        merged_file.to_csv(file_path,  index=False)

    def __update_matrix_B(self, dataframe=None):
        print("reading dataframe...")
        self.dataframe = pd.read_excel(r"D:\ecoinvent_scripts\output\merges_results_thinkstep_pilot\type_filtered.xlsx")
        ie_list = self.__find_unique_ie_index()
        B_matrix_csc = load_pkl_file(files_path.MATRIX_B_PICKLE)
        self.__set_B_matix_columns_to_zero(B_matrix_csc,  ie_list)
        # self.__read_indexes_columns()
        self.__write2Pickle()

    def __find_unique_ie_index(self):
        ie_list = self.dataframe["matrix ie index"].unique().tolist()
        print("len", len(ie_list))
        return ie_list

    def __set_B_matix_columns_to_zero(self, B_matrix_csc, ie_list):
        print("setting columns to zero...")
        self.B_matrix_array = B_matrix_csc.todense()
        # self.B_matrix_array = np.squeeze(np.asarray(self.B_matrix_array))
        print("B shape", ie_list)
        for i in ie_list:
            self.B_matrix_array[:, i] = 0

    def __write2Pickle(self):
        print("Writing matrix to pickle file...")
        csc_B_matrix = sparse.csc_matrix(self.B_matrix_array)
        dump_to_pickle("B", csc_B_matrix)

    def __update_matrix_B_with_resultiAmount(self):
        df = pd.read_csv(r"D:\ecoinvent_scripts\output\merges_results_thinkstep_pilot\file1.csv")
        B = load_pkl_file(r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\pickles\B.pkl")
        Bcoo = B.tocoo()
        B_coordinates_list = list(zip(Bcoo.row, Bcoo.col))
        # B_data = Bcoo.data.tolist()
        ee_index_list = df["index"].tolist()
        ie_index_list = df["matrix ie index"].tolist()
        result_list = df["resultingAmount"].tolist()
        new_B_coordinates = list(zip(ee_index_list, ie_index_list))
        B_coordinates_list.extend(new_B_coordinates)
        # if len(set(B_coordinates_list)) == len(B_coordinates_list):
        #     print("no issueee")
        print("from dataframe", max(ee_index_list))
        list1, list2 = zip(*B_coordinates_list)
        print("shape", Bcoo.data.shape)
        data_list = Bcoo.data.tolist()
        print(type(data_list))
        print(type(result_list))
        data_list.extend(result_list)
        print("list1 len", len(list1))
        print("list2 len", len(list2))
        print("max value1", max(list1))
        print("max value2", max(list2))
        print("combined data len", len(data_list))
        coo_matrix = sparse.coo_matrix((data_list, (list1, list2)), shape=(max(list1)+1, max(list2)+1))
        print(coo_matrix.shape)
        csc_matrix = coo_matrix.tocsc()
        print("csc", csc_matrix.shape)
        self.__write_csc_matrix_2Pickle("issueresolvedB", csc_matrix)
        # B_matrix_array = B.todense()
        # count = 0
        # print(len(B_coordinates_list))
        # for index, value in enumerate(B_coordinates_list):
        #     print(index)
        #     if value in new_B_coordinates:
        #         print("valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", value)
        #         count = count + 1
        # print("total", count)
        # max_ee_value = df["index"].max()
        # print("maxx", max_ee_value)
        # row_numbers = max_ee_value - B.shape[0] + 1
        # X = np.zeros((row_numbers, B_matrix_array.shape[1]))
        # B_matrix_array = np.vstack((B_matrix_array, X))
        # csc_B_matrix = sparse.csc_matrix(B_matrix_array)
        # print(df.shape)
        # for i, row in df[["index", "matrix ie index", "resultingAmount"]].iterrows():
        #     print(i)
        #     x = int(row['index'])
        #     y = int(row['matrix ie index'])
        #     v = row["resultingAmount"]
        #     csc_B_matrix[x, y] = v
        # self.__write_csc_matrix_2Pickle("newwB", csc_B_matrix)

    def __read_indexes_columns(self):
        ee_list = self.dataframe["index_x"].tolist()
        ie_list = self.dataframe["index_y"].tolist()
        resulting_amount_list = self.dataframe["resultingAmount"].tolist()
        print("ee_len", len(ee_list))
        print("ie_len", len(ie_list))
        print("result_len", len(resulting_amount_list))
        self.__plug_amount_in_matrix(ee_list, ie_list, resulting_amount_list)

    def __plug_amount_in_matrix(self, ee_list, ie_list, result_list):
        print("plugging the values in the matrix")
        max_ie_value = max(ie_list)
        row_numbers = max_ie_value - self.B_matrix_array.shape[0] + 1
        X = np.zeros((row_numbers, self.B_matrix_array.shape[1]))
        self.B_matrix_array = np.vstack((self.B_matrix_array, X))
        for i, j, result in zip(ee_list, ie_list, result_list):
            print("i: ", i, " j: ", j)
            self.B_matrix_array[int(i), int(j)] = result

    def __scaling_matrix_B(self):
        pilot_thinkstep_df = self.__read_pilot_thinkstep_template()
        process_flows_df = self.__read_process_flows_excel()
        merged_df = self.__merge_processFlows_with_Pilot_thinkstep(pilot_thinkstep_df, process_flows_df)
        # merged_df.to_excel(r"D:\ecoinvent_scripts\PEF_project\processflow_pilot.xlsx")
        # resultAmount = resultAmount_column.values()
        self.__divide_matrix_B_columns(merged_df)

    def __read_process_flows_excel(self):
        print("read index PEF file...")
        process_flows_df = pd.read_excel(r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\input\excel\processFlows_to_excel_scaling.xlsx")
        return process_flows_df

    def __merge_processFlows_with_Pilot_thinkstep(self, pilot_thinkstep_df, process_flows_df):
        print("Started merging processFlows with pilot thinkstep...")
        merged_processFlows_pilot_df = pd.merge(
            pilot_thinkstep_df,
            process_flows_df,
            how="left",
            left_on=["filename"],
            right_on=["UUID"],
        )
        return merged_processFlows_pilot_df

    def __divide_matrix_B_columns(self, df):
        csc_MatrixB = load_pkl_file(r"D:\ecoinvent_scripts\PEF_project\PEF_Project\ILCD_Reader\Data\output\pickles\issueresolvedB.pkl")

        for i, row in df.iterrows():
            csc_MatrixB[:, row["matrix ie index"]] = csc_MatrixB[:, row["matrix ie index"]] / row["resultingAmount"]
        self.__write_csc_matrix_2Pickle("newofnewB", csc_MatrixB)

    def __update_matrix_A_and_Z(self):
        pilot_thinkstep_df = self.__read_pilot_thinkstep_template("1 to many")
        # df = pd.read_excel(r"PilotThinkstepDataUsed_spec.xlsx", skiprows=[0])
        A = load_pkl_file(files_path.MATRIX_A_PICKLE)
        for _, row in pilot_thinkstep_df[["matrix ie index", "process ie index", "share"]].iterrows():
            x = int(row['matrix ie index'])
            y = int(row['process ie index'])
            v = row["share"]
            A[x, y] = v
        self.__write_csc_matrix_2Pickle("anotherA", A)
        self.__update_matrix_Z(A)

    def __update_matrix_Z(self, A):
        Z = -A
        Z.setdiag(0)
        self.__write_csc_matrix_2Pickle("anotherZ", Z)

    def __write_csc_matrix_2Pickle(self, matrix_name, csc_matrix):
        dump_to_pickle(matrix_name, csc_matrix)


obj = ThinkstepIntegration()
obj.start_processing()