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
    "figure.figsize": (400 * pt, 150 * pt),
    "font.size": 5, "axes.titlesize": 11, "axes.labelsize": 5,
    "xtick.labelsize": 5, "ytick.labelsize": 5,
    "legend.fontsize": 5, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})


colors = [
    "#3C7DC3",
    "#56B6A0",
    "#B97C2A",
    "#8C3F1F",
    "#BB52BB",
    "#B3C25C"
]


### 데이터 입력
csv_file = "/home/user/문서/workspace/python/graph/data/한국_주요_콩수입국.csv"
df = pd.read_csv(csv_file).convert_dtypes()

# ✔ long format 가정
# columns: 국가, 연도, 값
df["year"] = df["year"].astype(int)
df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)


### ▶ 기준 연도 선택
target_year = 2022   # ← 원하는 연도


### ▶ 기준 연도 값으로 국가 정렬
order = (
    df[df["year"] == target_year]
    .groupby("exporter")["value"]
    .sum()
    .sort_values(ascending=False)
    .index
    .tolist()
)


### 그래프 작성
years = sorted(df["year"].unique())
x = np.arange(len(years))

fig, ax = plt.subplots()
bottom = np.zeros(len(years))
stack_values = []


### ▶ 중요 항목 / 레전드 항목 / 수치표기 항목 정의

# ▶ 2022년 기준 상위 3개 수출국
legend_cols = (
    df[df["year"] == target_year]
    .groupby("exporter")["value"]
    .sum()
    .sort_values(ascending=False)
    .head(3)
    .index
    .tolist()
)

print("2022 Top 3 exporters:", legend_cols)
      # 레전드 개별 표시
      
value_label_cols = legend_cols[:2]



other_color = "#727272"

handles = []
legend_labels = []
other_handle = None


for i, country in enumerate(order):
    vals = np.array([
        df.loc[
            (df["exporter"] == country) & (df["year"] == y),
            "value"
        ].sum()
        for y in years
    ])

    # 색상 지정
    if country in legend_cols and legend_cols.index(country) < len(colors):
        color = colors[legend_cols.index(country)]
    else:
        color = other_color

    bar = ax.bar(
        x, vals,
        bottom=bottom,
        color=color,
        edgecolor="#444",
        width=0.7,
        zorder=3
    )

    # 레전드 구성
    if country in legend_cols:
        handles.append(bar[0])
        legend_labels.append(country)
    else:
        other_handle = bar[0]

    stack_values.append(vals.copy())
    bottom += vals


### 데이터 텍스트
# total labels
for i, total in enumerate(bottom):
    ax.annotate(
        f"{total/1000:,.0f}",
        (x[i], total),
        xytext=(0, 2),
        textcoords="offset points",
        ha="center",
        va="bottom",
        fontsize=5
    )


# part labels
cumulative = np.zeros(len(years))
for country, vals in zip(order, stack_values):
    cumulative += vals
    if country in value_label_cols:
        for i, v in enumerate(vals):
            if v > 0:
                ax.annotate(
                    f"{v/1000:,.0f}",
                    (x[i], cumulative[i] - 80_000),
                    ha="center",
                    va="center",
                    fontsize=5,
                    color="#FFFFFF"
                )


### x축 텍스트
ax.set_xticks(x)
ax.set_xticklabels(years)


### y축 텍스트
y_min, y_max = 0, 1_500_000
step = 300_000

ticks = np.arange(y_min, y_max + step, step)
ax.set_yticks(ticks)

labels = []
for t in ticks:
    if t == 0:
        labels.append("0")
    else:
        labels.append(f"{t/1000:,.0f}")

ax.set_yticklabels(labels)



### 그리드
ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)


### 레전드 — 주요 국가 + 기타
if other_handle is not None:
    handles.append(other_handle)
    legend_labels.append("기타")

# ▶ 범례: 하단 중앙, 자동 줄바꿈
n_legend = len(legend_labels)

ax.legend(
    handles,
    legend_labels,
    loc="upper center",
    bbox_to_anchor=(0.5, -0.1),   # 그래프 아래
    ncol=min(n_legend, 4),         # 한 줄 최대 3개 (넘치면 자동 줄바꿈)
    frameon=False,
    columnspacing=1.0,
    handletextpad=0.4
)



### 출처 텍스트
fig.text(
    0.15, 0.05,
    "출처: FAOSTAT",
    ha="center", va="top",
    fontsize=5, color="#555"
)

### 단위 텍스트
fig.text(
    0.65, 0.93,
    "(단위: 1,000톤)",
    ha="center", va="top",
    fontsize=5, color="#555"
)

fig.subplots_adjust(right=0.70, bottom=0.2)


### 저장
pic_name = "한국_콩수입_주요국"
save_path = f"/home/user/문서/workspace/python/graph/image"

sample_file = f"/home/user/문서/workspace/python/temp/chart.png"

latex_path = f"/home/user/문서/workspace/latex/project/paper/agri_develope/asset"

# # sample
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