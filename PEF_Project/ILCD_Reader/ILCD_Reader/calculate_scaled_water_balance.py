import pandas as pd
from util import file_utils
import files_path
import os


class CalculateScaledWaterBalance:
    def __init__(self):
        self.files_list = []

    def calculate(self):
        temp_df = None
        files_list = self._construct_files_list()
        self._load_data_set_list()
        self._load_total_water_balance()
        print("Started Concatenating...")
        for file in files_list:
            merged_df = self._merge_dataset_with_total_water(file)
            merged_df = pd.concat([temp_df, merged_df])
            temp_df = merged_df
        print("Finished Concatenating...")
        df_groupby = temp_df.groupby(["activityName", "geography", "product", "unitName_x"])
        df_groupby = df_groupby.mean().reset_index()
        df_groupby["abs_water_balance"] = df_groupby["water balance"].abs()
        df_groupby.sort_values(by="abs_water_balance", ascending=False, inplace=True)
        df_groupby = df_groupby[["activityName",
                                 "geography",
                                 "product",
                                 "unitName_x",
                                 "reference product amount",
                                 "water in",
                                 "water out",
                                 "water balance",
                                 "abs_water_balance"]]
        self._save2Excel(df_groupby)

    def _construct_files_list(self):
        list_of_files = self._load_data_set_list()
        fullpath_list = []
        for file in list_of_files:
            file_path = os.path.join(files_path.SCALING_FOLDER, file + ".pkl")
            fullpath_list.append(file_path)
        print("length", len(fullpath_list))
        return fullpath_list

    def _merge_dataset_with_total_water(self, file):
        concatenated_df = self._concatenate_scaling_with_indexes(file)
        merged_df = pd.merge(
            self.total_water_df,
            concatenated_df,
            left_on=["activityName", "geography", "product"],
            right_on=["activityName", "geography", "product"],
            how="outer",
        )

        merged_df.drop(['water balance/water in', 'water balance/water out'], axis=1, inplace=True)
        merged_df[["water in", "water out", "water balance"]] = merged_df[["water in", "water out", "water balance"]].multiply(merged_df["scaling amount"], axis="index")
        return merged_df
        # self.__save2Excel()

    def _concatenate_scaling_with_indexes(self, file):
        scaling_list_df = self._load_scaling_vector_file(file)
        df_indexes_ie = self._load_indexes_file()
        concatenated_dataframe = pd.concat([df_indexes_ie, scaling_list_df], axis=1)
        return concatenated_dataframe

    def _load_indexes_file(self):
        df_indexes_ie = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name="ie")
        return df_indexes_ie

    def _load_scaling_vector_file(self, file):
        numpy_array = file_utils.load_pkl_file(file)
        scaling_list = numpy_array.tolist()
        df = pd.DataFrame(scaling_list, columns=["scaling amount"])
        return df

    def _load_data_set_list(self):
        self.dataset_list_df = pd.read_excel(files_path.DATASET_LIST)
        self.dataset_list_df["index"] = self.dataset_list_df["index"].apply(str)
        index_list = self.dataset_list_df["index"].tolist()
        return index_list

    def _load_total_water_balance(self):
        self.total_water_df = pd.read_excel(files_path.TOTAL_WATER_BALANCE)

    @staticmethod
    def _save2Excel(df):
        print("Writing to excel...")
        try:
            df.to_excel(
                r"D:\ecoinvent_scripts\mean_scaled_TWB.xlsx", index=False
            )
        except ValueError as err:
            print(f"Error {err}")


obj = CalculateScaledWaterBalance()
obj.calculate()
