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
    "font.size": 9, "axes.titlesize": 9, "axes.labelsize": 9,
    "xtick.labelsize": 9, "ytick.labelsize": 9,
    "legend.fontsize": 9, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})


colors = ["#3A316D","#264653", "#2A9D8F", "#E9C46A",  "#E76F51"]


### 데이터 입력

csv_file = "/home/user/문서/workspace/python/graph/data/부안군_25년_평년_강수량.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()


### 데이터 지정

x = df["시점"].astype(str).values      # ← 문자열 그대로 사용
idx = np.arange(len(x))

y = df["25년도"].values


### 그래프 작성

fig, ax = plt.subplots()
bars = ax.bar(idx, y, color=colors[0], width=0.7,  edgecolor="#444", zorder=3, label="25년도")

### 🔥 추가 — 라인 하나 (예: "비료비")
line_col = "평년"       # ← 여기에서 라인에 쓸 컬럼 선택
ax.plot(idx, df[line_col], color=colors[2], marker="o", linewidth=0.9,
        markersize=2.7, zorder=4, label=line_col)


### 데이터 표시 텍스트

for bar in bars:
    h = bar.get_height()
    ax.annotate(f"{h:,.0f}", (bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=7, fontweight="bold")


### x축 텍스트

# 📌 기존 로직(date.year 비교)은 문자열에서 오류 → 모든 tick 사용
tick_idx = idx
tick_labels = x

ax.set_xticks(tick_idx)
ax.set_xticklabels(tick_labels)


### y축 텍스트

y_min, y_max = 0, 400
step = 50

ticks = np.arange(y_min, y_max + step, step)
ax.set_yticks(ticks)

labels = []
for t in ticks:
    if t == 0:
        labels.append("0")
    else:
        labels.append(f"{t:.0f}")

ax.set_yticklabels(labels)


### 그리드 

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)


### 🔥 범례 추가 (막대 + 라인 1개)
ax.legend(loc="center left", bbox_to_anchor=(1.0, 0.93), frameon=False)


fig.subplots_adjust(right=0.8, bottom=0.2)


### 출처 텍스트
fig.text(
    0.3, 0.1,
    "출처: 기상자료개방포털 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=9, color="#555"
)

### 단위 텍스트
fig.text(
    0.75, 0.93,
    "(단위: mm)",
    ha="center", va="top",
    fontsize=9, color="#555"
)



# print(df)

pic_name = "부안군_25년_평년_강수량"
save_path = f"/home/user/문서/workspace/python/graph/image"

sample_file = f"/home/user/문서/workspace/python/temp/chart.png"

latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# sample
# mpl.use("Agg")
# fig.savefig(f"{sample_file}", dpi=300, bbox_inches="tight")
# plt.close(fig)

# # PNG 저장
mpl.use("Agg")
fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
plt.close(fig)

# ----- OR -----

# latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# # PGF 저장
# mpl.use("pgf")
# fig.savefig(f"{latex_path}/{pic_name}.pgf")
# plt.close(fig)