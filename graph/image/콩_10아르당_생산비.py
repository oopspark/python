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



colors = [

    "#3C7DC3",  # 소프트 블루 / 부드러운 대비
    "#56B6A0",  # 청록 / 과하지 않은 컬러 포인트
    "#B97C2A",  # 소프트 오렌지 / 강렬하지만 톤 다운
    "#8C3F1F"   # 소프트 레드브라운 / 최종 강조
    "#0F0F70",  # 메인: 네이비
    "#274B97",  # 밝은 네이비 / 동일 계열
]



### 데이터 입력

csv_file = f"/home/user/문서/workspace/python/graph/data/콩_10아르당_생산비.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()


### 데이터 저장

df["시점"] = pd.to_datetime(df["시점"], format="%Y")
x = df["시점"].values
idx = np.arange(len(x))


y_cols = ["직접생산비②", "토지용역비③", "자본용역비⑤"]

legend_labels = {
    "직접생산비②": "직접 생산비",
    "토지용역비③": "토지 용역비",
    "자본용역비⑤": "자본 용역비"
}

### 그래프 작성

fig, ax = plt.subplots()
bottom = np.zeros(len(x))
stack_values = []

for i, col in enumerate(y_cols):
    vals = df[col].values
    ax.bar(idx, vals, bottom=bottom,
           color=colors[i % len(colors)],
           edgecolor="#444", width = 0.7 ,label=legend_labels[col], zorder=3)
    stack_values.append(vals.copy())
    bottom += vals


### 데이터 텍스트

# total labels
for i, total in enumerate(bottom):
    ax.annotate(f"{total:,.0f}", (idx[i], total),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=5)


# part labels
show_cols = ["직접생산비②", "토지용역비③"]   # ← 라벨 표시할 항목 리스트

cumulative = np.zeros(len(x))
for col, vals in zip(y_cols, stack_values):
    cumulative += vals
    for i, v in enumerate(vals):
        if col in show_cols:     # ← 리스트 기준 필터
            ax.annotate(
                f"{v:,.0f}",
                (idx[i], cumulative[i] - 30_000),
                ha="center", va="center",
                fontsize=5, color="#FFFFFF"
            )




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

y_min, y_max = 0, 900_000
step = 100_000

ticks = np.arange(y_min, y_max + step, step)
ax.set_yticks(ticks)

# 숫자를 '만' 단위로 표시 (0은 "0")
labels = []
for t in ticks:
    if t == 0:
        labels.append("0")
    else:
        labels.append(f"{t/10000:.0f}만")

ax.set_yticklabels(labels)



### 그리드

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)



### 레전드

ax.legend(loc="upper left", bbox_to_anchor=(1, 1.0), frameon=False)


### 출처 텍스트

fig.text(
    0.32, 0.05,
    "출처: 국가농식품통계서비스(KASS) 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=9, color="#555"
)


### 단위 텍스트
fig.text(
    0.75, 0.93,
    "(단위: 원/10a)",
    ha="center", va="top",
    fontsize=9, color="#555"
)



fig.subplots_adjust(right=0.8, bottom=0.2)


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



pic_name = "콩_10아르당_생산비"
save_path = f"/home/user/문서/workspace/python/graph/image"

sample_file = f"/home/user/문서/workspace/python/chart.png"
latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"


# # # sample
# mpl.use("Agg")
# fig.savefig(f"{sample_file}", dpi=300, bbox_inches="tight")
# plt.close(fig)


# # PNG 저장
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# ----- OR -----

# PGF 저장
mpl.use("pgf")
fig.savefig(f"{latex_path}/{pic_name}.pgf")
plt.close(fig)