from DetailWaterBalance import DetailWaterBalance
import numpy as np
import pandas as pd


class TotalWaterBalance:
    def __init__(self, pickles_source_dir, excels_source_dir, output_dir):
        """[initialize directories path]

        Args:
            pickles_source_dir ([str]): [description]
            excels_source_dir ([str]): [description]
            output_dir ([str]): [description]
        """
        self.pickles_source_dir = pickles_source_dir
        self.excels_source_dir = excels_source_dir
        self.output_dir = output_dir

        self.generate_DWB()

    def generate_DWB(self):
        """[Generate detail water balance dataframe]
        """
        DWB_Obj = DetailWaterBalance(
            self.pickles_source_dir, self.excels_source_dir, self.output_dir,
        )
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
        self.pivotTable = pd.pivot_table(
            self.DWB_df,
            values="contribution to water balance",
            index=["activityName", "geography", "reference product"],
            columns=["direction"],
            aggfunc=np.sum,
            fill_value=0,
        )
        self.pivotTable["water balance"] = self.pivotTable.apply(
            lambda row: row["water in"] + row["water out"], axis=1
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
        self.pivotTable["water balance/water in"] = self.pivotTable.apply(
            lambda row: self.__divide(row["water balance"], row["water in"]), axis=1
        )
        self.pivotTable["water balance/water out"] = self.pivotTable.apply(
            lambda row: self.__divide(row["water balance"], row["water out"]), axis=1
        )
        self.__save2Excel()

    def __save2Excel(self):
        self.pivotTable.reset_index(inplace=True)
        try:
            self.pivotTable.to_excel(
                r"D:\ecoinvent_scripts\Total water balance.xlsx", index=False
            )
        except ValueError as err:
            print(f"Error {err}")


def main():
    TotalWaterBalance(
        r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\pickles",
        r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel",
        r"D:\ecoinvent_scripts",
    )


if __name__ == "__main__":
    main()
