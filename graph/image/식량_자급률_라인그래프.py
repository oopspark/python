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

### 디자인
pt = 1 / 72
mpl.rcParams.update({
    "figure.figsize": (400 * pt, 200 * pt),
    "font.size": 5, "axes.titlesize": 11, "axes.labelsize": 5,
    "xtick.labelsize": 5, "ytick.labelsize": 5,
    "legend.fontsize": 5, "lines.linewidth": 0.75, "axes.linewidth": 0.75,
    "xtick.major.width": 0.75, "ytick.major.width": 0.75,
    "xtick.major.size": 1, "ytick.major.size": 1,
})

colors = [
    "#3C7DC3", "#56B6A0", "#B97C2A",
    "#8C3F1F", "#0F0F70", "#274B97"
]

### 데이터 입력
csv_file = f"/home/user/문서/workspace/python/graph/data/식량_자급률.csv"
df = pd.read_csv(csv_file, encoding="utf-8").convert_dtypes()

### 컬럼 자동 인식
y_cols = [c for c in df.columns if c != "시점"]

### x축
df["시점"] = pd.to_datetime(df["시점"], format="%Y")
x = df["시점"].values
idx = np.arange(len(x))


### 그래프
fig, ax = plt.subplots()

for i, col in enumerate(y_cols):
    y = df[col].astype("Float64").values
    ax.plot(idx, y,
            marker="o", markersize=3,
            color=colors[i % len(colors)],
            linewidth=1.2,
            label=col)

    # 데이터 라벨
    for j, v in enumerate(y):
        if pd.notna(v):
            ax.annotate(
                f"{v:.1f}",
                (idx[j], v),
                xytext=(0, 2),
                textcoords="offset points",
                ha="center", va="bottom",
                fontsize=5, color="#222"
            )

### x축 표시
ax.set_xticks(idx)
ax.set_xticklabels(df["시점"].dt.strftime("%Y"))

### y축 자동 설정
y_data = df[y_cols].astype(float).values
y_min = 0
y_max = np.nanmax(y_data) * 1.15   # 15% 여유
step = 5
ax.set_yticks(np.arange(y_min, y_max + step, step))


### 그리드 / 스파인
ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

### 레전드
ax.legend(bbox_to_anchor=(1.15, 1.0), frameon=False)

### 출처 텍스트
fig.text(
    0.28, 0.1,
    "출처: 국가농식품통계서비스(KASS) 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=5, color="#555"
)

### 단위 텍스트
fig.text(
    0.75, 0.93,
    "(단위: %)",
    ha="center", va="top",
    fontsize=5, color="#555"
)


fig.subplots_adjust(right=0.80, bottom=0.2)

### 저장

pic_name = "식량_자급률_라인그래프"
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

latex_path = f"/home/user/문서/workspace/latex/project/presentation/policy/asset"

# PGF 저장
mpl.use("pgf")
fig.savefig(f"{latex_path}/{pic_name}.pgf")
plt.close(fig)