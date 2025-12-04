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
    "figure.figsize": (300 * pt, 200 * pt),
    "font.size": 9, "axes.titlesize": 11, "axes.labelsize": 9,
    "xtick.labelsize": 8, "ytick.labelsize": 8,
    "legend.fontsize": 8, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})



colors = ["#1B3A6F", "#0072B2", "#009E73", "#E69F00", "#D55E00"]




### 데이터 입력

df = pd.DataFrame({
    "Year": ["2020", "2021", "2022", "2023"],
    "한국": [23, 28, 31, 37],
    "미국": [40, 45, 47, 49],
    "일본": [18, 20, 22, 24],
})


### 데이터 저장

df["Year"] = pd.to_datetime(df["Year"])
x = df["Year"].values
idx = np.arange(len(x))


y_cols = ["한국", "미국", "일본"]


### 그래프 작성

fig, ax = plt.subplots()
bottom = np.zeros(len(x))
stack_values = []

for i, col in enumerate(y_cols):
    vals = df[col].values
    ax.bar(idx, vals, bottom=bottom,
           color=colors[i % len(colors)],
           edgecolor="#444", linewidth=0.5, width=0.6, label=col, zorder=3)
    stack_values.append(vals.copy())
    bottom += vals


### 데이터 텍스트

# total labels
for i, total in enumerate(bottom):
    ax.annotate(f"{total:.0f}", (idx[i], total),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=8)

# part labels
cumulative = np.zeros(len(x))
for vals in stack_values:
    cumulative += vals
    for i, v in enumerate(vals):
        ax.annotate(f"{v:.0f}", (idx[i], cumulative[i] - 5),
                    ha="center", va="center", fontsize=7, color="#FFFFFF")


### x축 텍스트


tick_idx = []
tick_labels = []

for i, date in enumerate(df["Year"]):
    # 같은 연·월의 첫 번째 날짜만 tick
    if i == 0 or date.year != df["Year"][i-1].year:
        tick_idx.append(i)
        tick_labels.append(date.strftime("%Y"))   # "%Y"만 쓰면 연도만 표시


ax.set_xticks(tick_idx)
ax.set_xticklabels(tick_labels)


### y축 텍스트

y_min, y_max = 0, 120
step = 20  # 원하는 간격
ax.set_yticks(np.arange(y_min, y_max + step, step))



### 그리드

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)



### 레전드

ax.legend(loc="upper left", bbox_to_anchor=(0.98, 1.0), frameon=False)


### 출처 텍스트

fig.text(
    0.3, 0,
    "출처: FAOSTAT, 2025 데이터 활용",
    ha="center", va="top",
    fontsize=7, color="#555"
)


### 단위 텍스트
fig.text(
    0.76, 0.93,
    "(단위: Dollar/Bushell)",
    ha="center", va="top",
    fontsize=7, color="#555"
)



# fig.subplots_adjust(right=0.70, bottom=0.1)


### 경계 확인용 텍스트
# fig.text(
#     1, 1,
#     "ㅁ",
#     ha="center", va="top",
#     fontsize=7, color="#555"
# )
# fig.text(
#     0, 0,
#     "ㅁ",
#     ha="center", va="top",
#     fontsize=7, color="#555"
# )




# PNG 저장
mpl.use("Agg")
fig.savefig("chart.png", dpi=300, bbox_inches="tight")
plt.close(fig)

# ----- OR -----

# PGF 저장
# mpl.use("pgf")
# fig.savefig("/path/to/chart.pgf")
# plt.close(fig)
