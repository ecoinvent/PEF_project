# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os
os.chdir(r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\ILCD_Reader")


# %%
from DetailWaterBalance import DetailWaterBalance


# %%
obj = DetailWaterBalance(r'D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\pickles',
                             r'D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel',
                             r'D:\ecoinvent_scripts')


# %%
df = obj.generate_DWB_df()


# %%
df.info()


# %%
filter = df['reference product'] == 'cable yarding'


# %%
new_df = df[filter]


# %%
new_df.activityName.unique()


# %%
df['direction'].head()


# %%
df['direction'].replace({-1: 'water out', 1: 'water in'}, inplace=True)


# %%
df["direction"].head()


# %%
df["direction"].tail()


# %%
import numpy as np
import pandas as pd
table = pd.pivot_table(df, values='contribution to water balance', index=['activityName', 'geography', 'reference product'], columns=['direction'], aggfunc=np.sum, fill_value=0)


# %%
table


# %%
table.info()


# %%
table['water balance'] = table.apply(lambda row: row['water in'] + row['water out'], axis=1)


# %%
table.head()


# %%
def divide(wb, w):
    try:
        value = wb/w
    except ZeroDivisionError:
        value = np.nan
    return value


# %%
table['water balance/water in'] = table.apply(lambda row: divide(row['water balance'], row['water in']), axis=1)


# %%
table.head()


# %%
table['water balance/water out'] = table.apply(lambda row: divide(row['water balance'], row['water out']), axis=1)


# %%
table.head()


# %%
import os
os.chdir(r"D:\ecoinvent_scripts")


# %%
table.reset_index(inplace=True)
table.to_excel("total water balance.xlsx")


# %%


