import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd

### 폰트
font_path = "/usr/share/fonts/truetype/nanum/NanumMyeongjo.ttf"
font_manager.fontManager.addfont(font_path)
mpl.rc("font", family="NanumMyeongjo")
mpl.rcParams["axes.unicode_minus"] = False

### 디자인 일반
pt = 1 / 72
mpl.rcParams.update({
    "figure.figsize": (600 * pt, 200 * pt),
    "font.size": 9,
    "axes.titlesize": 11,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
})

colors = ["#264653", "#2A9D8F", "#E9C46A", "#F4A261", "#E76F51"]

### 데이터 입력
csv_file = "/home/user/문서/workspace/python/graph/data/콩_비축량.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()

df["시점"] = pd.to_datetime(df["시점"], format="%Y")
x = df["시점"].values
idx = np.arange(len(x))
y = df["SUM(데이터)"].astype(float).values

### 그래프 작성 (구간별 linestyle)
fig, ax = plt.subplots()

for i in range(1, len(y)):
    seg = slice(i-1, i+1)
    linestyle = "--" if df["시점"].dt.year.iloc[i] == 2025 else "-"
    ax.plot(idx[seg], y[seg],
            marker="o", markersize=3,
            color=colors[0], linewidth=1.2,
            linestyle=linestyle)

### x축 텍스트
tick_idx = []
tick_labels = []

for i, date in enumerate(df["시점"]):
    if i == 0 or date.year != df["시점"][i - 1].year:
        tick_idx.append(i)
        tick_labels.append(date.strftime("%Y"))

ax.set_xticks(tick_idx)
ax.set_xticklabels(tick_labels)

### y축 텍스트
# 데이터가 10~60 수준이라 자동으로 설정되게 수정
y_min = 0
y_max = np.nanmax(y) * 1.15
step = 10
ax.set_yticks(np.arange(y_min, y_max + step, step))

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

### 출처
fig.text(
    0.25, 0,
    "출처: 국가농식품통계서비스(KASS) 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=7, color="#555"
)

### 단위
fig.text(
    0.83, 0.93,
    "(단위: 원/10a)",
    ha="center", va="top",
    fontsize=7, color="#555"
)

### 저장
pic_name = "콩_비축량"
save_path = f"/home/user/문서/workspace/python/graph/image"
sample_file = f"/home/user/문서/workspace/python/temp/chart.png"

# sample
mpl.use("Agg")
fig.savefig(f"{sample_file}", dpi=300, bbox_inches="tight")
plt.close(fig)


# # PNG 저장
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# ----- OR -----

# # PGF 저장
# mpl.use("pgf")
# fig.savefig(f"{save_path}/{pic_name}.pgf")
# plt.close(fig)

