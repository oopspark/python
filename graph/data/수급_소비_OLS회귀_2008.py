import pandas as pd
from data_load_save import *

df = pd.read_csv("/home/user/문서/workspace/python/graph/data/수급_1970_1989.csv")
# df = pd.read_csv("/home/user/문서/workspace/python/graph/data/수급_1990_2008.csv")

df = df.sort_values("Year").reset_index(drop=True)

df["Import_minus_Export"] = df["Import Quantity"] - df["Export Quantity"]
df["-Stock Variation"] = df["Stock Variation"]

# 원하는 4개 컬럼 선택
df_mod = df[["Year", "Domestic supply quantity", "Import_minus_Export", "Production", "-Stock Variation"]].copy()

# 차분 + 첫행 제거
df_diff = df_mod.copy()
df_diff[["ΔDomestic_supply_quantity", "ΔImport_minus_Export", "ΔProduction", "neg_ΔStock_Variation"]] = \
    df_mod[["Domestic supply quantity", "Import_minus_Export", "Production", "-Stock Variation"]].diff()


# 첫 행 제거
df_diff = df_diff.iloc[1:].reset_index(drop=True)

# 🔥 정수 변환
df_diff = df_diff.astype({
    "Year": int,
    "ΔDomestic_supply_quantity": int,
    "ΔImport_minus_Export": int,
    "ΔProduction": int,
    "neg_ΔStock_Variation": int
})

print(df_diff)
# save_df_to_csv(df_diff, "/home/user/문서/workspace/python/graph/data/콩_수급_2010_modified.csv")


import statsmodels.api as sm

X = df_diff["ΔDomestic_supply_quantity"]
X = sm.add_constant(X)  # 상수항 (e는 잔차)


a1 = sm.OLS(df_diff["ΔProduction"], X).fit()
a2 = sm.OLS(df_diff["ΔImport_minus_Export"], X).fit()
a3 = sm.OLS(df_diff["neg_ΔStock_Variation"], X).fit()

# print(a1.params, a2.params, a3.params)



print(a1.summary())
print(a2.summary())
print(a3.summary())


# from linearmodels.system import SUR

# formulas = {
#     "eq1": "ΔImport_minus_Export ~ ΔDomestic_supply_quantity",
#     "eq2": "ΔProduction ~ ΔDomestic_supply_quantity",
#     "eq3": "neg_ΔStock_Variation ~ ΔDomestic_supply_quantity",
# }

# model = SUR.from_formula(formulas, data=df_diff)
# res = model.fit(restrictions="eq1_ΔDomestic_supply_quantity + eq2_ΔDomestic_supply_quantity + eq3_ΔDomestic_supply_quantity = 1")
# print(res)
