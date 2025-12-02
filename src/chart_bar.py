import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np


# ===================== FONT SETUP =====================
font_path = "/usr/share/fonts/truetype/nanum/NanumMyeongjo.ttf"
font_manager.fontManager.addfont(font_path)
mpl.rc("font", family="NanumMyeongjo")
mpl.rcParams["axes.unicode_minus"] = False


# ===================== GLOBAL STYLE =====================
pt = 1 / 72
base_style = {
    "figure.figsize": (300 * pt, 200 * pt),  # 기본: 300×200 pt
    "font.size": 9,
    "axes.titlesize": 11,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "lines.linewidth": 0.75,
    "axes.linewidth": 0.75,
    "xtick.major.width": 0.75,
    "ytick.major.width": 0.75,
    "xtick.major.size": 1,
    "ytick.major.size": 1,
}
mpl.rcParams.update(base_style)


# ===================== CHART FUNCTION =====================
def draw_chart():
    categories = ["A", "B", "C", "D", "E"]
    values = [23, 17, 35, 29, 12]
    x = np.arange(len(categories))

    fig, ax = plt.subplots()

    bar_colors = ["#FF8C42", "#FFB347", "#FFD166", "#E29578", "#FF6F61"]
    bars = ax.bar(
        x,
        values,
        color=bar_colors,
        edgecolor="#444444",
        linewidth=0.7,
    )

    ax.set_title("샘플바 차트 with Full Styling", pad=10, fontweight="bold")
    ax.set_xlabel("Category")
    ax.set_ylabel("Value")

    ax.set_xticks(x)
    ax.set_xticklabels(categories)

    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45)

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            f"{height:.0f}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 2),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
            fontweight="bold",
            color="#333",
        )

    fig.tight_layout()
    return fig


# ===================== STACKED BAR FUNCTION =====================
def draw_bar_stack():
    categories = ["A", "B", "C", "D", "E"]
    values1 = [23, 17, 35, 29, 12]
    values2 = [12, 9, 14, 10, 6]
    x = np.arange(len(categories))

    fig, ax = plt.subplots()

    c1 = "#FF8C42"
    c2 = "#E29578"

    # ---- STACK ----
    bars1 = ax.bar(x, values1, color=c1, edgecolor="#444444", linewidth=0.7, label="1단계")
    bars2 = ax.bar(x, values2, bottom=values1, color=c2, edgecolor="#444444", linewidth=0.7, label="2단계")

    # ---- TITLE (중앙) ----
    ax.set_title("스택형 바 차트", pad=12, fontweight="bold", loc="center")

    # ---- LEGEND (오른쪽 내부/밖 경계) ----
    # bbox_to_anchor = (x, y) 는 figure 좌표계 기준
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.0, 1.0),
        borderaxespad=0.,
        frameon=False,
    )

    # ---- Axis & Style ----
    ax.set_xlabel("Category")
    ax.set_ylabel("Value")
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    # ---- TOTAL VALUE (막대 가장 위) ----
    for i in range(len(x)):
        total = values1[i] + values2[i]
        ax.annotate(
            f"{total}",
            (x[i], total),
            xytext=(0, 2),
            textcoords="offset points",
            ha="center", va="bottom",
            fontsize=8,
            fontweight="bold",
        )

    # ---- PART VALUES (각 경계 바로 아래) ----
    for i, v1 in enumerate(values1):
        ax.annotate(
            f"{v1}",
            (x[i], v1 - 0.4),
            ha="center", va="top",
            fontsize=7, color="#222",
        )
    for i, (v1, v2) in enumerate(zip(values1, values2)):
        boundary = v1 + v2
        ax.annotate(
            f"{v2}",
            (x[i], boundary - 0.4),
            ha="center", va="top",
            fontsize=7, color="#222",
        )

    # ---- 여백 확보 (legend 잘림 방지: PNG 기준) ----
    fig.subplots_adjust(right=0.7, bottom=0.15)
    return fig


# ===================== SAVE: PNG =====================
print("Saving PNG...")
mpl.use("Agg")  # Agg backend for raster (PNG)
png_fig = draw_bar_stack()
png_fig.savefig("chart.png", dpi=300, bbox_inches="tight")
plt.close(png_fig)
print("Saved PNG → chart.png")


# ===================== SAVE: PGF =====================
print("Saving PGF...")
mpl.use("pgf")  # PGF (LaTeX) backend
mpl.rcParams.update({
    "pgf.texsystem": "xelatex",
    "text.usetex": True,
    "pgf.rcfonts": False,
    "mathtext.default": "regular",
})

pgf_fig = draw_bar_stack()
pgf_path = "/home/user/문서/workspace/latex/project/presentation/policy/asset/chart_bar.pgf"

pgf_fig.savefig(pgf_path)  # PGF는 bbox_inches 생략이 레이아웃에 더 안전
plt.close(pgf_fig)
print(f"Saved PGF → {pgf_path}")
