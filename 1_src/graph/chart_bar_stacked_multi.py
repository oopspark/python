import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd

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
    "figure.figsize": (450 * pt, 230 * pt),
    "font.size": 9,
    "axes.titlesize": 11,
    "axes.labelsize": 9,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "lines.linewidth": 1.0,
    "axes.linewidth": 0.75,
    "xtick.major.width": 0.75,
    "ytick.major.width": 0.75,
    "xtick.major.size": 1,
    "ytick.major.size": 1,
})

# 색상 정의
COLOR_SCALE = "#1F4E79"   # 어두운 파란색 (막대)
COLOR_COMP  = "#F2B705"   # 밝은 노란색 (라인)

# ======================================================
# 3. 데이터 로드
# ======================================================
csv_file = "/home/user/문서/workspace/python/1_src/graph/data/grain_network_distances.csv"
df = pd.read_csv(csv_file, encoding="utf-8", thousands=",").convert_dtypes()

df = df.dropna(subset=["year"])
df["year"] = df["year"].astype(int)
df["year_dt"] = pd.to_datetime(df["year"], format="%Y")

df["D_comp"] = pd.to_numeric(df["D_comp"], errors="coerce")
df["D_scale"] = pd.to_numeric(df["D_scale"], errors="coerce")

idx = np.arange(len(df))
y_comp = df["D_comp"].values
y_scale = df["D_scale"].values

# ======================================================
# 4. 그래프 생성
# ======================================================
fig, ax_left = plt.subplots()



# ▶ 규모: 막대 (오른쪽 축)
ax_right = ax_left.twinx()
bars_scale = ax_right.bar(
    idx, y_scale,
    width=0.65,
    color=COLOR_SCALE,
    edgecolor="black",
    linewidth=0.5,
    alpha=0.9,
    label="Scale $D^{\\text{scale}}_t$",
    zorder=2,
)
# 🔥 라인을 막대 위로 올리기 (twinx 필수 처리)
ax_left.set_zorder(ax_right.get_zorder() + 1)
ax_left.patch.set_visible(False)
ax_right.set_ylabel("Scale index $D^{\\text{scale}}_t$")

# ▶ 구성: 라인 (왼쪽 축)
line_comp, = ax_left.plot(
    idx, y_comp,
    color=COLOR_COMP,
    marker="o",
    markersize=3,
    linewidth=1.2,
    label="Composition $D^{\\text{comp}}_t$",
    zorder=5,   # 🔥 충분히 크게
)

ax_left.set_ylabel("Composition index $D^{\\text{comp}}_t$")

# ======================================================
# 5. 축 설정
# ======================================================
ax_left.set_xticks(idx)
ax_left.set_xticklabels(df["year_dt"].dt.strftime("%Y"))

# y축 범위 자동 여유
def set_ylim_with_margin(ax, y):
    ymin, ymax = np.nanmin(y), np.nanmax(y)
    margin = 0.05 * (ymax - ymin) if ymax > ymin else 1.0
    ax.set_ylim(ymin - margin, ymax + margin)

set_ylim_with_margin(ax_left, y_comp)
set_ylim_with_margin(ax_right, y_scale)

# 그리드 & 스파인
ax_left.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4)
ax_left.spines["top"].set_visible(False)
ax_right.spines["top"].set_visible(False)

# ======================================================
# 6. 레전드 (하단, 가로)
# ======================================================
handles = [line_comp, bars_scale]
labels = [h.get_label() for h in handles]

fig.legend(
    handles,
    labels,
    loc="upper center",
    bbox_to_anchor=(0.5, 0.03),
    ncol=2,
    frameon=False,
)

fig.subplots_adjust(
    left=0.12,
    right=0.88,
    top=0.95,
    bottom=0.2,
)

# ======================================================
# 7. 저장
# ======================================================
mpl.use("Agg")
out_file = "/home/user/문서/workspace/python/1_src/graph/260121/chart.png"
fig.savefig(out_file, dpi=300, bbox_inches="tight")
plt.close(fig)


# # PNG 저장 (실제 사용)
# mpl.use("Agg")
# fig.savefig(f"{save_path}/{pic_name}.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# ----- OR -----
# # PGF 저장 (LaTeX용, png 대신 쓸 경우)
# mpl.use("pgf")
# fig.savefig(f"{save_path}/{pic_name}.pgf")
# plt.close(fig)
