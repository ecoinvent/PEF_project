import pandas as pd
from util.file_utils import load_pkl_file
from tqdm import tqdm
import os
import sys


class DetailWaterBalance:
    def __init__(self, source_dir_pklFiles, source_dir_excelFiles, destination_dir):
        """[initialize instance variables with source_directory and destination directory]

        Args:
            source_dir_pklFiles ([type]): [file path to source directory]
            destination_dir ([type]): [file path to destination directory]
        """
        self.source_dir_pklFiles = source_dir_pklFiles
        self.source_dir_excelFiles = source_dir_excelFiles
        self.destination_dir = destination_dir
        self.matrix_A_data, self.matrix_B_data = None, None
        self.wb = None
        self.my_dataframes_dict = None

    def load_A_df(self):
        """[load the merged Index and matrix A pickle file]

        Returns:
            [type]: [description]
        """
        print("Reading_merged_matrix_A...")
        matA_df = load_pkl_file(self.source_dir_pklFiles, 'MergedmatrixA.pkl')
        print("Create direction and data columns...")
        matA_df["direction"] = matA_df["data"].apply(lambda x: -1 if x >= 0 else 1)
        matA_df["data"] = matA_df["data"].abs()
        return matA_df

    def load_B_df(self):
        """[summary]

        Returns:
            [type]: [description]
        """
        print("Reading_merged_matrix_B...")
        matB_df = load_pkl_file(self.source_dir_pklFiles, "MergedmatrixB.pkl")
        print("Create direction column...")
        matB_df["direction"] = matB_df["compartment"].apply(
            lambda x: -1 if "Emissions" in str(x) else (1 if "Resources" in str(x) else None)
        )
        return matB_df

    def read_water_balance_template(self):
        """[summary]
        """
        print("Reading index water balance...")
        index_water_balance_file = os.path.join(self.source_dir_excelFiles, "indexes_PEF-phase 2-start_Allocation, cut-off.xlsx")
        try:
            self.wb = pd.read_excel(index_water_balance_file, sheet_name=None)
        except FileNotFoundError:
            print("indexes water balance not found")
            sys.exit()

        self.wb["ie"].rename(
            columns={"activityName": "activityName_wb", "geography": "geography_wb"},
            inplace=True,
        )

    def read_dataset_list_water(self):
        """[summary]
        """
        dataset_list_file = os.path.join(self.source_dir_excelFiles, "datasetList_flowChange.xlsx")
        try:
            self.dataset_df = pd.read_excel(dataset_list_file)
        except FileNotFoundError:
            print("dataset list water not found")
            sys.exit()

    def fetch_column_names(self, exchange):
        """[summary]

        Args:
            exchange ([type]): [description]

        Returns:
            [type]: [description]
        """
        if exchange == "ie":
            return (
                (
                    "activityLink_activityName",
                    "activityLink_geography",
                    "exchange name",
                ),
                ("activityName_wb", "geography_wb", "product"),
            )
        elif exchange == "ee":
            return (
                ("exchange name", "compartment", "subcompartment"),
                ("name", "compartment", "subcompartment"),
            )

    def merge_dataframes_with_WB(self, func, exchange, left_cols, right_cols):
        """[summary]

        Args:
            func ([type]): [description]
            exchange ([type]): [description]
            left_cols ([type]): [description]
            right_cols ([type]): [description]

        Returns:
            [type]: [description]
        """
        mat_df = func()
        merged_df = pd.merge(
            mat_df,
            self.wb[exchange],
            left_on=left_cols,
            right_on=right_cols,
            how="left",
        )
        return merged_df

    def modify_dataframe(self, merged_df3):
        """[summary]

        Args:
            merged_df3 ([type]): [description]
        """
        print("Start Modifying dataframe...")
        merged_df3.rename(columns={"data": "exchange amount"}, inplace=True)
        merged_df3.drop(
            [
                "column_index",
                "index",
                "name",
                "product",
                "activityName_wb",
                "geography_wb",
                "reference product unitName",
                "row_index",
                "unitName",
            ],
            axis=1,
            inplace=True,
        )

        merged_df3["contribution to water balance"] = merged_df3.apply(
            lambda row: row["direction"]
            * row["exchange amount"]
            * row["water in wet mass"],
            axis=1,
        )

        merged_df3 = merged_df3[
            [
                "Dataset_type",
                "activityName",
                "geography",
                "reference product",
                "exchange name",
                "compartment",
                "subcompartment",
                "activityLink_activityName",
                "activityLink_geography",
                "exchange unitName",
                "exchange amount",
                "direction",
                "water in wet mass",
                "water region",
                "contribution to water balance",
            ]
        ]

        merged_df3.sort_values(
            by=[
                "activityName",
                "geography",
                "reference product",
                "exchange name",
                "compartment",
                "subcompartment",
                "activityLink_activityName",
                "activityLink_geography",
            ],
            inplace=True,
        )
        return merged_df3

    def generate_DWB_df(self):
        """[summary]

        Returns:
            [type]: [description]
        """
        my_list = ["ie", "ee"]
        my_func = [self.load_A_df, self.load_B_df]
        my_dict = {}
        self.read_water_balance_template()

        for exchange, func in zip(my_list, my_func):
            exchange = exchange
            left_cols, right_cols = self.fetch_column_names(exchange)
            merged_df = self.merge_dataframes_with_WB(
                func, exchange, left_cols, right_cols
            )
            my_dict[exchange] = merged_df

        concat_dfs = pd.concat(
            [my_dict["ie"], my_dict["ee"]], ignore_index=True, sort=False
        )

        self.read_dataset_list_water()

        merged_df3 = pd.merge(
            concat_dfs,
            self.dataset_df,
            on=["activityName", "geography", "reference product"],
            how="left",
        )

        modified_Dataframe = self.modify_dataframe(merged_df3)
        return modified_Dataframe

    def generate_DWB_template(self):
        """[summary]
        """
        merged_df3 = self.generate_DWB_df()
        Dataset_type_list = self.dataset_df["Dataset_type"].unique().tolist()
        self.my_dataframes_dict = {
            dataset: merged_df3.loc[merged_df3["Dataset_type"] == dataset]
            for dataset in Dataset_type_list
        }
        self.save2Excel()

    def save2Excel(self):
        """[summary]
        """
        df_shape_list = []
        print("Writing Dataframes to Excel in process...")
        writer = pd.ExcelWriter(r"D:\ecoinvent_scripts\Detailed Water Balance.xlsx", engine="xlsxwriter")
        for key in tqdm(self.my_dataframes_dict):
            df_shape_list.append(self.my_dataframes_dict[key].shape)
            self.my_dataframes_dict[key].to_excel(writer, sheet_name=key, index=False)

        print()
        print("Applying autofilter and Freezing Panes on all Excel sheets...")

        for sheet, shape in zip(writer.sheets, df_shape_list):
            worksheet = writer.sheets[sheet]
            worksheet.autofilter(0, 0, shape[0] - 1, shape[1] - 1)
            worksheet.freeze_panes(1, 0)

        print()
        print("Saving Excel File in process...")
        writer.save()


def main():
    obj = DetailWaterBalance(
        r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\pickles",
        r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel",
        r"D:\ecoinvent_scripts",
    )
    obj.generate_DWB_template()


if __name__ == "__main__":
    main()
