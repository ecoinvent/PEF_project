import pandas as pd


class ChangeIndexUnit:

    def __init__(self):
        self.water_updated_resources_df = None
        self.index_pef_df = None

    def replace_units(self):
        self.__read_updated_water_resource_template()
        self.__read_index_PEF()
        self.__merge_updated_resources_with_index()

    def __read_updated_water_resource_template(self):
        self.water_updated_resources_df = pd.read_excel(r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\water amount and unit correction_20200326.xlsx")

    def __read_index_PEF(self):
        self.index_pef_df = pd.read_excel(r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\pilotPhase_water_correction\indexes_PEF-phase 2-start_Allocation, cut-off.xlsx", sheet_name=None)

    def __merge_water_amount_with_index(self, drop_column=None):
        self.__create_column_in_index()
        self.__modify_column_names()
        self.index_pef_df["ee"] = pd.merge(self.water_updated_resources_df,
                                           self.index_pef_df['ee'],
                                           how='right',
                                           left_on=["flow", "water_compartment", "water_subcompartment"],
                                           right_on=["name", "compartment", "subcompartment"]
                                           )

    def __create_column_in_index(self):
        filter = self.index_pef_df["ee"]["name"].str.contains("regionalized", regex=True)
        self.index_pef_df["ee"]["new_name"] = self.index_pef_df["name"]
        self.index_pef_df["ee"].loc[filter, "new_name"] = (
            self.index_pef_df["ee"].loc[filter, "new_name"].str.split(",").str[0:2].str.join(",")
        )

    def __modify_column_names(self):
        self.water_updated_resources_df.rename(columns={"compartment": "water_compartment", "subcompartment": "water_subcompartment"}, inplace=True)

    def __replace_exchange_units(self):
        self.index_pef_df["ee"].loc[self.index_pef_df["ee"]["unit"] == "m3", 'unitName'] = self.index_pef_df["ee"]["unit"]

