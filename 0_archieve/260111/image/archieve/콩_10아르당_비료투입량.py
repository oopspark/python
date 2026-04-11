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
    "figure.figsize": (450 * pt, 230 * pt),
    "font.size": 9, "axes.titlesize": 11, "axes.labelsize": 9,
    "xtick.labelsize": 9, "ytick.labelsize": 9,
    "legend.fontsize": 9, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})


colors = ["#264653", "#2A9D8F", "#E9C46A", "#F4A261", "#E76F51"]


### 데이터 입력

csv_file = "/home/user/문서/workspace/python/graph/data/콩_10아르당_비료투입량.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()


### 데이터 지정

df["시점"] = pd.to_datetime(df["시점"], format="%Y")
x = df["시점"].values
idx = np.arange(len(x))

y = df["SUM(데이터)"].values


### 그래프 작성

fig, ax = plt.subplots()
bars = ax.bar(idx, y, color=colors[0],  edgecolor="#444", zorder=3)




### 데이터 표시 텍스트

for bar in bars:
    h = bar.get_height()
    ax.annotate(f"{h:.0f}", (bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=8, fontweight="bold")


### x축 텍스트

tick_idx = []
tick_labels = []

for i, date in enumerate(df["시점"]):
    # 같은 연·월의 첫 번째 날짜만 tick
    if i == 0 or date.year != df["시점"][i-1].year:
        tick_idx.append(i)
        tick_labels.append(date.strftime("%Y"))   # "%Y"만 쓰면 연도만 표시

ax.set_xticks(tick_idx)
ax.set_xticklabels(tick_labels, rotation = 45)


### y축 텍스트


y_min, y_max = 0, 300
step = 30  # 원하는 간격
ax.set_yticks(np.arange(y_min, y_max + step, step))


### 그리드 


ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)


fig.subplots_adjust(right=0.9, bottom=0.2)


### 출처 텍스트

fig.text(
    0.35, 0.05,
    "출처: 국가농식품통계서비스(KASS) 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=9, color="#555"
)


### 단위 텍스트
fig.text(
    0.85, 0.95,
    "(단위: kg/10a)",
    ha="center", va="top",
    fontsize=9, color="#555"
)

# print(df)

pic_name = "콩_10아르당_비료투입량"
save_path = f"/home/user/문서/workspace/python/graph/image"

sample_file = f"/home/user/문서/workspace/python/chart.png"

latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"


# sample
# mpl.use("Agg")
# fig.savefig(f"{sample_file}", dpi=300, bbox_inches="tight")
# plt.close(fig)


# PNG 저장
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# ----- OR -----

# PGF 저장
mpl.use("pgf")
fig.savefig(f"{latex_path}/{pic_name}.pgf")
plt.close(fig)
