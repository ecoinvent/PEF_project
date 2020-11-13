from detail_water_balance_PEF2 import DetailWaterBalancePEF2
import numpy as np
import pandas as pd
from util import file_utils
import files_path


class TotalWaterBalancePEF2:
    def __init__(self):
        """[initialize directories path]

        Args:
            pickles_source_dir ([str]): [description]
            excels_source_dir ([str]): [description]
            output_dir ([str]): [description]
        """
        self.generate_DWB()

    def get_matrix_A_diag(self):
        csc_matrix = file_utils.load_pkl_file(files_path.MATRIX_A_PICKLE)
        matrix_diagonal = csc_matrix.diagonal()
        diag_list = matrix_diagonal.tolist()
        df_A = pd.DataFrame(diag_list, columns=["reference product amount"])
        return df_A

    def load_indexes(self):
        df_indexes_ie = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name="ie")
        return df_indexes_ie

    def concat_indexes_A_diag(self):
        df_A_diag = self.get_matrix_A_diag()
        df_indexes_ie = self.load_indexes()
        concatenated_dataframe = pd.concat([df_indexes_ie, df_A_diag], axis=1)
        return concatenated_dataframe

    def generate_DWB(self):
        """[Generate detail water balance dataframe]
        """
        DWB_Obj = DetailWaterBalancePEF2()
        self.DWB_df = DWB_Obj.generate_DWB_df()
        self.__apply_replacement()

    def __apply_replacement(self):
        """[apply replacement on direction column in the dataframe]
        """
        self.DWB_df["direction"].replace({-1: "water out", 1: "water in"}, inplace=True)
        self.__generate_pivot_table()

    def __generate_pivot_table(self):
        """[generate pivot table out of the dataframe]
        """
        pivotTable = pd.pivot_table(
            self.DWB_df,
            values="contribution to water balance",
            index=["activityName", "geography", "reference product"],
            columns=["direction"],
            aggfunc=np.sum,
            fill_value=0,
        )
        pivotTable["water balance"] = pivotTable.apply(
            lambda row: row["water in"] + row["water out"], axis=1
        )

        concatenated_df = self.concat_indexes_A_diag()

        self.merged_df = pd.merge(
            concatenated_df,
            pivotTable,
            left_on=["activityName", "geography", "product"],
            right_on=["activityName", "geography", "reference product"],
            how="inner",
        )
        self.__create_ratio_column()

    @staticmethod
    def __divide(wb, w):
        """[dividing the water balance values over the water in or out values]

        Args:
            wb ([float]): [water balance value]
            w ([float]): [water in or water out value]

        Returns:
            [float]: [result of division]
        """
        try:
            value = wb / w
        except ZeroDivisionError:
            value = np.nan
        return value

    def __create_ratio_column(self):
        """[introduce two new columns to the dataframe by dividing the water
           balance over water in and water balance over water out]
        """
        self.merged_df["water balance/water in"] = self.merged_df.apply(
            lambda row: self.__divide(row["water balance"], row["water in"]), axis=1
        )
        self.merged_df["water balance/water out"] = self.merged_df.apply(
            lambda row: self.__divide(row["water balance"], row["water out"]), axis=1
        )
        self.__save2Excel()

    def __save2Excel(self):
        self.merged_df.reset_index(inplace=True)
        try:
            self.merged_df.to_excel(
                r"D:\ecoinvent_scripts\Total water balance.xlsx", index=False
            )
        except ValueError as err:
            print(f"Error {err}")


def main():
    TotalWaterBalancePEF2()


if __name__ == "__main__":
    main()
