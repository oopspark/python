import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 데이터 (P(X|Y))
data = np.array([
[0.1385,0.1385,0.1538,0.1231,0.1385,0.1077,0.0769,0.0462,0.0308,0.0462],
[0.0290,0.1014,0.1884,0.0870,0.1304,0.1159,0.1159,0.1159,0.0870,0.0290],
[0.0693,0.1188,0.1089,0.0495,0.1188,0.1584,0.1683,0.1386,0.0396,0.0297],
[0.3939,0.3939,0.1212,0.0606,0.0303,0,0,0,0,0],
[0.0325,0.0747,0.1071,0.1006,0.1331,0.1039,0.1526,0.1266,0.1364,0.0325],
[0.0096,0.0431,0.0431,0.0574,0.0766,0.0957,0.2010,0.2584,0.1148,0.1005],
[0.0196,0.0588,0.0392,0.0686,0.0980,0.1275,0.1961,0.1863,0.1275,0.0784],
[0.0154,0.0308,0.0923,0.1077,0.1538,0.1077,0.1385,0.1231,0.1231,0.1077],
[0.0145,0.1594,0.1014,0.0870,0.0580,0.0725,0.1159,0.1304,0.2029,0.0580]
])

x_labels = [0.5,1.5,2.5,3.5,4.5,5.5,6.7,8.8,12.5,17.5]
y_labels = [-0.25,-0.18,-0.05,0,0.05,0.15,0.25,0.40,0.50]

df = pd.DataFrame(data, index=y_labels, columns=x_labels)

plt.figure(figsize=(10,6))

ax = sns.heatmap(
    df,
    annot=True,
    fmt=".3f",
    cmap="Reds",
    linewidths=0   # ❌ 격자 제거
)

# ✅ 가로선만 직접 추가
for y in range(1, df.shape[0]):
    ax.hlines(y, *ax.get_xlim(), colors='black', linewidth=1)

plt.title("Conditional Probability P(X|Y)")
plt.xlabel("Income (X)")
plt.ylabel("Savings Rate (Y)")

save_path = r"H:\내 드라이브\workspace\python\1_src\assignments\econometrics"
plt.tight_layout()
plt.savefig(save_path + "\heatmap_P_X_given_Y.png", dpi=300)
plt.show()