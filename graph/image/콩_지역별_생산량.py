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
    "figure.figsize": (500 * pt, 200 * pt),
    "font.size": 5, "axes.titlesize": 11, "axes.labelsize": 5,
    "xtick.labelsize": 5, "ytick.labelsize": 5,
    "legend.fontsize": 5, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})


colors = [
    "#3C7DC3",  # 소프트 블루 / 부드러운 대비
    "#56B6A0",  # 청록 / 과하지 않은 컬러 포인트
    "#B97C2A",  # 소프트 오렌지 / 강렬하지만 톤 다운
    "#8C3F1F",   # 소프트 레드브라운 / 최종 강조
    "#BB52BB",
    "#B3C25C"
]


### 데이터 입력

csv_file = f"/home/user/문서/workspace/python/graph/data/콩_지역별_생산량.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()


### 데이터 저장

df["시점"] = pd.to_datetime(df["시점"], format="%Y")
x = df["시점"].values
idx = np.arange(len(x))


### ▶ 기준 연도 선택
target_year = 2023   # ← 원하는 연도

# y_cols 자동 추출 (시점 컬럼 제외)
y_cols = [c for c in df.columns if c != "시점"]


### ▶ 기준 연도 값으로 정렬
df_target = df[df["시점"].dt.year == target_year].iloc[0]
y_cols = sorted(
    y_cols,
    key=lambda col: df_target[col],
    reverse=True        # 큰 값 → 작은 값
)


### 그래프 작성

fig, ax = plt.subplots()
bottom = np.zeros(len(x))
stack_values = []

### ▶ 중요 항목 / 레전드 항목 / 수치표기 항목 정의
value_label_cols = ["경상북도", "전라북도", "전라남도"]                 # 수치표시할 2개
legend_cols = ["경상북도", "전라북도", "전라남도", "충청북도", "충청남도", "경기도"]   # 레전드에 개별 표시할 4개
other_color = "#727272"                                     # 기타 색상

handles = []
legend_labels = []
other_handle = None

for i, col in enumerate(y_cols):
    vals = pd.to_numeric(df[col], errors="coerce").fillna(0).values


    # 색상: legend_cols는 팔레트, 그 외는 회색
    if col in legend_cols and legend_cols.index(col) < len(colors):
        color = colors[legend_cols.index(col)]
    else:
        color = other_color

    bar = ax.bar(idx, vals, bottom=bottom,
                 color=color,
                 edgecolor="#444", width=0.7, label=col, zorder=3)

    # 레전드 구성
    if col in legend_cols:
        handles.append(bar[0])
        legend_labels.append(col)
    else:
        other_handle = bar[0]  # 기타 대표 핸들만 기록

    stack_values.append(vals.copy())
    bottom += vals


### 데이터 텍스트

# total labels
for i, total in enumerate(bottom):
    ax.annotate(f"{total:,.0f}", (idx[i], total),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=5)


# part labels (value_label_cols 2개만 라벨 표시)
cumulative = np.zeros(len(x))
for col, vals in zip(y_cols, stack_values):
    cumulative += vals
    for i, v in enumerate(vals):
        if col in value_label_cols:
            ax.annotate(
                f"{v:,.0f}",
                (idx[i], cumulative[i] - 8_000),
                ha="center", va="center",
                fontsize=5, color="#FFFFFF"
            )


### x축 텍스트

tick_idx = []
tick_labels = []

for i, date in enumerate(df["시점"]):
    if i == 0 or date.year != df["시점"][i-1].year:
        tick_idx.append(i)
        tick_labels.append(date.strftime("%Y"))

ax.set_xticks(tick_idx)
ax.set_xticklabels(tick_labels)


### y축 텍스트

y_min, y_max = 0, 180_000
step = 20_000

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

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)


### 레전드 — 4개 항목 + 기타 1개

if other_handle is not None:
    handles.append(other_handle)
    legend_labels.append("기타")

ax.legend(handles, legend_labels, loc="upper left", bbox_to_anchor=(0.98, 1.0), frameon=False)


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
    "(단위: 톤)",
    ha="center", va="top",
    fontsize=5, color="#555"
)


fig.subplots_adjust(right=0.70, bottom=0.2)


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


pic_name = "콩_지역별_생산량"
save_path = f"/home/user/문서/workspace/python/graph/image"

sample_file = f"/home/user/문서/workspace/python/temp/chart.png"

latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# # sample
# mpl.use("Agg")
# fig.savefig(f"{sample_file}", dpi=300, bbox_inches="tight")
# plt.close(fig)


# # PNG 저장
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# ----- OR -----

# latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# PGF 저장
mpl.use("pgf")
fig.savefig(f"{latex_path}/{pic_name}.pgf")
plt.close(fig)
