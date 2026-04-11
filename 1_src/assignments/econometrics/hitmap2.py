import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 데이터 (P(Y|X))
data = np.array([
[0.1915,0.0978,0.1031,0.0952,0.0804,0.0648,0.0321,0.0195,0.0177,0.0517],
[0.0426,0.0761,0.1340,0.0714,0.0804,0.0741,0.0513,0.0519,0.0531,0.0345],
[0.1489,0.1304,0.1134,0.0595,0.1071,0.1481,0.1090,0.0909,0.0354,0.0517],
[0.2766,0.1413,0.0412,0.0238,0.0089,0,0,0,0,0],
[0.2128,0.2500,0.3402,0.3690,0.3661,0.2963,0.3013,0.2532,0.3717,0.1724],
[0.0426,0.0978,0.0928,0.1429,0.1429,0.1852,0.2692,0.3506,0.2124,0.3621],
[0.0426,0.0652,0.0412,0.0833,0.0893,0.1204,0.1282,0.1234,0.1150,0.1379],
[0.0213,0.0217,0.0619,0.0833,0.0893,0.0648,0.0577,0.0519,0.0708,0.1207],
[0.0213,0.1196,0.0722,0.0714,0.0357,0.0463,0.0513,0.0584,0.1239,0.0690]
])

x_labels = [0.5,1.5,2.5,3.5,4.5,5.5,6.7,8.8,12.5,17.5]
y_labels = [-0.25,-0.18,-0.05,0,0.05,0.15,0.25,0.40,0.50]

df = pd.DataFrame(data, index=y_labels, columns=x_labels)

plt.figure(figsize=(10,6))

ax = sns.heatmap(
    df,
    annot=True,
    fmt=".3f",
    cmap="Blues",
    linewidths=0   # ❌ 격자 제거
)

# ✅ 세로선만 추가
for x in range(1, df.shape[1]):
    ax.vlines(x, *ax.get_ylim(), colors='black', linewidth=1)

plt.title("Conditional Probability P(Y|X)")
plt.xlabel("Income (X)")
plt.ylabel("Savings Rate (Y)")

save_path = r"H:\내 드라이브\workspace\python\1_src\assignments\econometrics"
plt.tight_layout()
plt.savefig(save_path + "\heatmap_P_Y_given_X.png", dpi=300)
plt.show()