import pandas as pd


class ReplaceIndexUnits:
    def __init__(self):
        pass

    @classmethod
    def take_filePath_input(cls):
        return cls(
            input("Please Enter Path of Matrix B File: "),
            input("Please Enter Path of water correction template: "),
            input("Please Enter Path of index PEF template: "),
        )

    def updatedIndex(self):
        self.__read_water_correctionTempalte()
        self.__read_index_Template()
        self.__apply_filter_on_index_df()
        self.__merge_waterCorrection_with_index()
        self.__apply_filter_on_merged_df()
        self.__copy_dataframe()
        self.__save2Excel()

    def __read_water_correctionTempalte(self):
        """[summary]
        """
        self.waterCorrection_df = pd.read_excel(
            r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\water amount and unit correction_20200326.xlsx"
        )
        self.waterCorrection_df.dropna(subset=["unit"], inplace=True)

    def __read_index_Template(self):
        """[summary]
        """
        self.df_index = pd.read_excel(
            r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\pilotPhase_water_correction\indexes_PEF-phase 2-start_Allocation, cut-off.xlsx",
            sheet_name=None,
        )

    def __apply_filter_on_index_df(self):
        """[summary]
        """
        filter = self.df_index["ee"]["name"].str.contains("regionalized", regex=True)
        self.df_index["ee"]["tempname"] = self.df_index["ee"]["name"]
        self.df_index["ee"].loc[filter, "tempname"] = (
            self.df_index["ee"]
            .loc[filter, "name"]
            .str.split(",")
            .str[0:2]
            .str.join(",")
        )

    def __merge_waterCorrection_with_index(self):
        """[summary]
        """
        left_cols = ["tempname", "compartment", "subcompartment"]
        right_cols = ["flow", "compartment", "subcompartment"]
        self.merged_index_waterCorrection_df = pd.merge(
            self.df_index["ee"],
            self.waterCorrection_df,
            left_on=left_cols,
            right_on=right_cols,
            how="left",
            indicator=True
        )

    def __apply_filter_on_merged_df(self):
        """[summary]
        """
        filter = self.merged_index_waterCorrection_df["unit"].str.contains(
            "m3", regex=True, na=False
        )
        self.merged_index_waterCorrection_df.loc[
            filter, "unitName"
        ] = self.merged_index_waterCorrection_df.loc[filter, "unitName"].str.replace("kg", "m3")
        self.merged_index_waterCorrection_df.drop(
            [
                "tempname",
                "type of search",
                "flow",
                "exchange unitName",
                "amount",
                "unit",
                "exclude some datasets from conversion",
            ],
            axis=1,
            inplace=True,
        )

    def __copy_dataframe(self):
        """[summary]
        """
        self.df_index["ee"] = self.merged_index_waterCorrection_df

    def __save2Excel(self):
        """[summary]
        """
        with pd.ExcelWriter(r"D:\ecoinvent_scripts\indexes_PEF-phase 2-start_Allocation.xlsx") as writer:
            self.df_index["ie"].to_excel(writer, sheet_name="ie", index=False)
            self.df_index["ee"].to_excel(writer, sheet_name="ee", index=False)
            self.df_index["LCIA"].to_excel(writer, sheet_name="LCIA", index=False)


def main():
    obj = ReplaceIndexUnits()
    obj.updatedIndex()


if __name__ == "__main__":
    main()
