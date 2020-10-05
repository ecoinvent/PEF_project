import pandas as pd
import files_path
import pickle


class MatrixIndex:
    # handy object to go from matrix index to names, with units
    def __init__(self):
        self.toggle = {}
        self.index_types = ['ie', 'ee', 'LCIA']
        self.df_columns = [['activityName', 'geography', 'product', 'unitName', 'index'],
                           ['name', 'compartment', 'subcompartment', 'unitName', 'index'],
                           ['method', 'category', 'indicator', 'indicator unitName', 'index']
                           ]
        for index_type in self.index_types:
            setattr(self, index_type, [])
            self.toggle[index_type] = {}
        self.units = {}
        self.dfs = {}
        self.convert_to_indexes_pickle()

    def convert_to_indexes_pickle(self):
        self.__read_indexes_tempalte()
        self.__read_elemnts()
        self.__to_dfs()

    def __read_indexes_tempalte(self):
        self.index_PEF_df = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name=None)

    def __read_elemnts(self):
        for index, columns in zip(self.index_types, self.df_columns):
            for x in range(len(self.index_PEF_df[index])):
                exchange = tuple(self.index_PEF_df[index].loc[x, columns])
                self.__add_index(index, exchange)

    def __add_index(self, index_type, exchange):
        # when all the info about an index type (ie, ee, LCIA) is gathered, it is saved
        element, unit, index = exchange[:-2], exchange[-2], exchange[-1]
        if element not in getattr(self, index_type):
            getattr(self, index_type).append(element)
            self.toggle[index_type][element] = index
            self.toggle[index_type][index] = element
            self.units[element] = unit

    def __to_dfs(self):
        for index_type in self.index_types:
            self.dfs[index_type] = self.index_PEF_df[index_type]
