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

### 스타일
pt = 1/72
mpl.rcParams.update({
    "figure.figsize": (400 * pt, 150 * pt),
    "font.size": 5, "axes.titlesize": 5, "axes.labelsize": 5,
    "xtick.labelsize": 5, "ytick.labelsize": 5,
    "legend.fontsize": 5, "lines.linewidth": 0.75,
})

### ⬇️ 지역별 대표 색 (논·밭은 같은 계열)
region_colors = {
    "경북": "#3C7DC3",   # 파랑
    "전남": "#B97C2A",   # 오렌지
    "전북": "#56B6A0",   # 초록
}

### CSV 읽기
csv_file = "/home/user/문서/workspace/python/graph/data/콩_논밭_면적_경북전남전북.csv"
wide = pd.read_csv(csv_file).convert_dtypes()

### wide → long 변환
records = []
for col in wide.columns:
    if col == "시점":  # first column
        continue
    region, category = col.replace('"', "").split(",")
    region, category = region.strip(), category.strip()
    for _, row in wide.iterrows():
        records.append({
            "연도": int(row["시점"]),
            "지역": region,
            "구분": category,
            "값": float(row[col]) if pd.notna(row[col]) else 0
        })

df = pd.DataFrame(records)

### 그래프 준비
years = sorted(df["연도"].unique())
# regions = sorted(df["지역"].unique())
regions = ["전남", "경북", "전북"]   # ← 원하는 순서로


# categories = sorted(df["구분"].unique())  # 논/밭
categories = ["밭", "논"]



fig, ax = plt.subplots()
clusters = len(years)
bars_per_cluster = len(regions)
bar_width = 0.7 / bars_per_cluster
cluster_x = np.arange(clusters)

### 그래프: 지역별 색, 구분은 같은 계열+투명도 차이
for r_i, region in enumerate(regions):
    bottoms = np.zeros(clusters)
    bar_x = cluster_x + (r_i - (bars_per_cluster - 1) / 2) * bar_width

    base_color = region_colors.get(region, "#999999")

    for c_i, cat in enumerate(categories):
        vals = []
        for year in years:
            v = df.loc[(df["연도"] == year) & (df["지역"] == region) & (df["구분"] == cat), "값"]
            if len(v):
                v = float(v.iloc[0]) if pd.notna(v.iloc[0]) else 0
            else:
                v = 0
            vals.append(v)

        vals = np.array(vals)

        # 같은 색 계열로 alpha 구분 (논/밭)
        alpha = 0.7 if cat == "논" else 1

        ax.bar(bar_x, vals, bottom=bottoms,
               width=bar_width,
               label=f"{region}-{cat}",
               color=base_color,
               alpha=alpha,
               edgecolor="#444",
               zorder=3)
        bottoms += vals
                # 라벨 표시 (각 스택의 중앙)

        # 스택 내부 라벨
        for idx, v in enumerate(vals):
            if v > 0:
                ax.annotate(f"{v:,.0f}",
                            (bar_x[idx], bottoms[idx] - 500),
                            ha="center", va="center",
                            fontsize=5, color="#FFFFFF", zorder=5)

    # 🔥 여기서 단 한 번 지역 합계 표시
    for idx, total in enumerate(bottoms):
        ax.annotate(
            f"{total:,.0f}",
            (bar_x[idx], total),
            xytext=(0, 3), textcoords="offset points",
            ha="center", va="bottom",
            fontsize=5, fontweight="bold",
            color="#000000",
            zorder=6
        )


### X축
ax.set_xticks(cluster_x)
ax.set_xticklabels(years)


### y축 텍스트

y_min, y_max = 0, 18_000
step = 2_000

ticks = np.arange(y_min, y_max + step, step)
ax.set_yticks(ticks)

labels = []
for t in ticks:
    labels.append(f"{t:,}")   # ← 쉼표만 추가

ax.set_yticklabels(labels)





ax.grid(axis='y', linestyle='--', linewidth=0.6, alpha=0.4, zorder=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)



ax.legend(bbox_to_anchor=(1.0, 1.0), frameon=False)


### 출처 텍스트

fig.text(
    0.23, 0.1,
    "출처: 국가농식품통계서비스(KASS) 자료 기반 저자 작성",
    ha="center", va="top",
    fontsize=5, color="#555"
)


### 단위 텍스트
fig.text(
    0.75, 0.95,
    "(단위: ha)",
    ha="center", va="top",
    fontsize=5, color="#555"
)


fig.subplots_adjust(right=0.80, bottom=0.2)



### 저장


pic_name = "콩_논밭_면적_경북전남전북"
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
