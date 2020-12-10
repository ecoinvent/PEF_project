import pandas as pd
import pickle as pickle

class Matrix_Index:
    # handy object to go from matrix index to names, with units
    def __init__(self):
        self.toggle = {}
        self.index_types = ['ie', 'ee', 'LCIA']
        for index_type in self.index_types:
            # this will only contain tuples and can be used for iteration
            setattr(self, index_type, [])
            self.toggle[index_type] = {}

        # units are also stored here
        self.units = {}

def index_pkl_to_xlsx(index_pkl,index_xlsx):
    idx = pickle.load(open(index_pkl, "rb"))
    df1 = pd.DataFrame(idx.ie)
    df1.columns = ["activityName", "geography", "product"]
    df1["unitName"] = df1.apply(lambda d: idx.units[(d[0], d[1], d[2])], axis=1)
    df2 = pd.DataFrame(idx.ee)
    df2.columns = ["name", "compartment", "subcompartment"]
    df2["unitName"] = df2.apply(lambda d: idx.units[(d[0], d[1], d[2])], axis=1)
    df3 = pd.DataFrame(idx.LCIA)
    df3.columns = ["method", "category", "indicator"]
    df3["unitName"] = df3.apply(lambda d: idx.units[(d[0], d[1], d[2])], axis=1)
    writer = pd.ExcelWriter(index_xlsx, engine='xlsxwriter')
    df1.to_excel(writer, sheet_name='ie', index=False)
    df2.to_excel(writer, sheet_name='ee', index=False)
    df3.to_excel(writer, sheet_name='LCIA', index=False)
    writer.save()