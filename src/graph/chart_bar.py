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
    "figure.figsize": (400 * pt, 200 * pt),
    "font.size": 5, "axes.titlesize": 5, "axes.labelsize": 5,
    "xtick.labelsize": 5, "ytick.labelsize": 5,
    "legend.fontsize": 5, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})


colors = ["#3A316D","#264653", "#2A9D8F", "#E9C46A",  "#E76F51"]


### 데이터 입력

csv_file = "/home/user/문서/workspace/python/graph/data/직접생산비_합계.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()


### 데이터 지정

df["시점"] = pd.to_datetime(df["시점"], format="%Y")
x = df["시점"].values
idx = np.arange(len(x))

y = df["SUM(데이터)"].values


### 그래프 작성

fig, ax = plt.subplots()
bars = ax.bar(idx, y, color=colors[0], width=0.7,  edgecolor="#444", zorder=3)




### 데이터 표시 텍스트

for bar in bars:
    h = bar.get_height()
    ax.annotate(f"{h:,.0f}", (bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=5, fontweight="bold")


### x축 텍스트

tick_idx = []
tick_labels = []

for i, date in enumerate(df["시점"]):
    # 같은 연·월의 첫 번째 날짜만 tick
    if i == 0 or date.year != df["시점"][i-1].year:
        tick_idx.append(i)
        tick_labels.append(date.strftime("%Y"))   # "%Y"만 쓰면 연도만 표시

ax.set_xticks(tick_idx)
ax.set_xticklabels(tick_labels)


### y축 텍스트


# y_min, y_max = 0, 180_000
# step = 20_000  # 원하는 간격
# ax.set_yticks(np.arange(y_min, y_max + step, step))

y_min, y_max = 0, 700_000
step = 100_000

ticks = np.arange(y_min, y_max + step, step)
ax.set_yticks(ticks)

labels = []
for t in ticks:
    if t == 0:
        labels.append("0")
    else:
        labels.append(f"{t/10000:.0f}만")

ax.set_yticklabels(labels)


### 그리드 


ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)



fig.subplots_adjust(right=0.9, bottom=0.2)


### 출처 텍스트
fig.text(
    0.25, 0.12,
    "출처: 국가농식품통계서비스(KASS) 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=5, color="#555"
)

### 단위 텍스트
fig.text(
    0.65, 0.93,
    "(단위: %)",
    ha="center", va="top",
    fontsize=5, color="#555"
)



# print(df)

pic_name = "직접생산비_합계"
save_path = f"/home/user/문서/workspace/python/graph/image"

sample_file = f"/home/user/문서/workspace/python/temp/chart.png"

latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# sample
mpl.use("Agg")
fig.savefig(f"{sample_file}", dpi=300, bbox_inches="tight")
plt.close(fig)


# # PNG 저장
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# ----- OR -----

# latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# # PGF 저장
# mpl.use("pgf")
# fig.savefig(f"{latex_path}/{pic_name}.pgf")
# plt.close(fig)