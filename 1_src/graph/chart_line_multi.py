import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd

from matplotlib.lines import Line2D

# ======================================================
# 1. 폰트 설정
# ======================================================
font_path = "/usr/share/fonts/truetype/nanum/NanumMyeongjo.ttf"
font_manager.fontManager.addfont(font_path)
mpl.rc("font", family="NanumMyeongjo")
mpl.rcParams["axes.unicode_minus"] = False

# ======================================================
# 2. 디자인 설정
# ======================================================
pt = 1 / 72
mpl.rcParams.update({
    "figure.figsize": (600 * pt, 150 * pt),
    "font.size": 7,
})

# 색상: 구성(파랑), 규모(노랑)
color_comp = "#3C7DC3"   # composition
color_scale = "#F2B705"  # scale

# ======================================================
# 3. 데이터 입력
# ======================================================
csv_file = "/home/user/문서/workspace/python/1_src/graph/data/grain_network_distances.csv"

df = pd.read_csv(csv_file, encoding="utf-8", thousands=",").convert_dtypes()

# 마지막에 ,, 들어가 있는 빈 행 제거
df = df.dropna(subset=["year"])

# year를 datetime으로 변환
df["year"] = df["year"].astype(int)
df["year_dt"] = pd.to_datetime(df["year"], format="%Y")

# 필요한 열만 float로 캐스팅
df["D_comp"] = pd.to_numeric(df["D_comp"], errors="coerce")
df["D_scale"] = pd.to_numeric(df["D_scale"], errors="coerce")

# x축 인덱스
x = df["year_dt"].values
idx = np.arange(len(x))

# y 데이터
y_comp = df["D_comp"].values
y_scale = df["D_scale"].values

# ======================================================
# 4. 그래프 생성 (단일 y축)
# ======================================================
fig, ax = plt.subplots()

# 구성: 파랑 실선
line_comp, = ax.plot(
    idx, y_comp,
    marker="o", markersize=3,
    color=color_comp,
    linewidth=1.2,
    label="Composition",
)

# 규모: 노랑 점선
line_scale, = ax.plot(
    idx, y_scale,
    marker="o", markersize=3,
    color=color_scale,
    linewidth=1.2,
    linestyle="--",
    label="Scale",
)

# ======================================================
# 5. 축 설정
# ======================================================

# x축 눈금
ax.set_xticks(idx)
ax.set_xticklabels(df["year_dt"].dt.strftime("%Y"), rotation=45)

# y축: 두 지표를 함께 고려해 범위/틱 설정
y_all = np.concatenate([y_comp, y_scale])
y_max = np.nanmax(y_all)

# 0부터 시작, 약간 여유
ax.set_ylim(0, y_max * 1.05)

# 🔹 y축 틱 간격 (원하는 간격으로 조정 가능)
step = 2_000_000
ax.set_yticks(np.arange(0, y_max * 1.05 + step, step))

# # y축 라벨
# ax.set_ylabel("Index of structural change")

# 그리드 / 스파인
ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

# ======================================================
# 6. 레전드 (위 가로 배치)
# ======================================================

legend_lines = [
    Line2D(
        [0], [0],
        color=color_comp,
        linewidth=1.2,
        linestyle="-",
    ),
    Line2D(
        [0], [0],
        color=color_scale,
        linewidth=1.2,
        linestyle="--",
    ),
]
legend_labels = ["Composition", "Scale"]

fig.legend(
    legend_lines, legend_labels,
    loc="upper center",
    bbox_to_anchor=(0.5, 0.9),
    ncol=2,
    frameon=False,
    fontsize=9,    
    handlelength=3.0,   # 🔹 레전드 선 길이 늘리기
    handletextpad=0.6,  # (옵션) 선과 텍스트 사이 간격
)

# 여백 조정
fig.subplots_adjust(
    bottom=0.15,
)

# ======================================================
# 7. 저장
# ======================================================

pic_name = "곡물네트워크_multi_single_axis"
save_path = "/home/user/문서/workspace/latex/project/abstract/AAEA"
sample_file = "/home/user/문서/workspace/python/1_src/graph/260121/chart.png"

# PNG 저장 (샘플)
# mpl.use("Agg")
# fig.savefig(sample_file, dpi=300, bbox_inches="tight")
# plt.close(fig)

# # PNG 저장 (실제 사용)
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# PGF 저장 (LaTeX용)
mpl.use("pgf")
fig.savefig(f"{save_path}/{pic_name}.pgf")
plt.close(fig)
