import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd


# ===================== FONT SETUP =====================
font_path = "/usr/share/fonts/truetype/nanum/NanumMyeongjo.ttf"
font_manager.fontManager.addfont(font_path)
mpl.rc("font", family="NanumMyeongjo")
mpl.rcParams["axes.unicode_minus"] = False


# ===================== GLOBAL STYLE =====================
pt = 1 / 72
base_style = {
    "figure.figsize": (300 * pt, 200 * pt),
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


# ===================== COMMON COLOR PALETTE =====================
# 컬러풀 + 흑백 대응 팔레트
default_colors = [
    "#264653",  # navy dark
    "#2A9D8F",  # teal
    "#E9C46A",  # sand
    "#F4A261",  # light orange
    "#E76F51",  # coral
]


# ===================== BAR CHART =====================
def draw_bar(df: pd.DataFrame, x_col: str, y_col: str):
    x = df[x_col].values
    y = df[y_col].values
    idx = np.arange(len(x))

    fig, ax = plt.subplots()

    bars = ax.bar(
        idx,
        y,
        color=default_colors[:len(y)],
        edgecolor="#444444",
        linewidth=0.7,
    )

    ax.set_title("막대 그래프", pad=12, fontweight="bold")
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    ax.set_xticks(idx)
    ax.set_xticklabels(x)

    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    for bar in bars:
        h = bar.get_height()
        ax.annotate(
            f"{h:.0f}",
            (bar.get_x() + bar.get_width() / 2, h),
            textcoords="offset points",
            xytext=(0, 2),
            ha="center", va="bottom",
            fontsize=8, fontweight="bold"
        )

    fig.text(
        0.5, -0.05,
        "출처: FAOSTAT, 2025 데이터 활용",
        ha="center", va="top",
        fontsize=7, color="#555"
    )

    fig.subplots_adjust(bottom=0.22)
    return fig


# ===================== STACKED BAR CHART =====================
def draw_bar_stack(df: pd.DataFrame, x_col: str, y_cols: list):
    x = df[x_col].values
    idx = np.arange(len(x))

    fig, ax = plt.subplots()

    bottom = np.zeros(len(x))
    bars_all = []

    # 각 스택 값 저장 (레이블 계산용)
    stack_values = []

    for i, col in enumerate(y_cols):
        color = default_colors[i % len(default_colors)]
        vals = df[col].values
        bars = ax.bar(
            idx,
            vals,
            bottom=bottom,
            color=color,
            edgecolor="#444444",
            linewidth=0.7,
            label=col
        )
        stack_values.append(vals.copy())
        bottom += vals
        bars_all.append(bars)

    ax.set_title("스택형 바 차트", pad=12, fontweight="bold")
    ax.set_xlabel(x_col)
    ax.set_ylabel("값")
    ax.set_xticks(idx)
    ax.set_xticklabels(x)
    ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0), frameon=False)

    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.45)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    # ---- TOTAL LABEL (가장 위) ----
    for i, total in enumerate(bottom):
        ax.annotate(
            f"{total:.0f}",
            (idx[i], total),
            textcoords="offset points",
            xytext=(0, 2),
            ha="center", va="bottom",
            fontsize=8, fontweight="bold"
        )

    # ---- PART LABELS (각 스택 경계 바로 아래) ----
    cumulative = np.zeros(len(x))
    for vals in stack_values:         # y1 → y2 → y3 순서로 올라감
        cumulative += vals
        for i, v in enumerate(vals):
            y_pos = cumulative[i] - 2  # 딱 중앙 아닌 “경계 바로 아래” 연출
            ax.annotate(
                f"{v:.0f}",
                (idx[i], y_pos),
                ha="center", va="center",
                fontsize=7, color="#222"
            )

    fig.text(
        0.5, -0.05,
        "출처: FAOSTAT, 2025 데이터 활용",
        ha="center", va="top",
        fontsize=7, color="#555"
    )

    fig.subplots_adjust(right=0.70, bottom=0.22)
    return fig



# ===================== MULTI LINE CHART =====================
def draw_line_multi(df: pd.DataFrame, x_col: str, y_cols: list):
    x = df[x_col].values
    idx = np.arange(len(x))

    fig, ax = plt.subplots()

    # y 컬럼마다 선 생성
    for i, col in enumerate(y_cols):
        color = default_colors[i % len(default_colors)]
        y = df[col].values

        ax.plot(
            idx,
            y,
            marker="o",
            markersize=3,
            color=color,
            linewidth=1.2,
            label=col
        )

        # 각 점 위에 수치 라벨
        for j, v in enumerate(y):
            ax.annotate(
                f"{v:.0f}",
                (idx[j], v),
                xytext=(0, 2),
                textcoords="offset points",
                ha="center", va="bottom",
                fontsize=7, color="#222"
            )

    # ax.set_title("꺾은선 그래프", pad=12, fontweight="bold")
    # ax.set_xlabel(x_col)
    # ax.set_ylabel("값")
    ax.set_xticks(idx)
    ax.set_xticklabels(x)

    ax.grid(axis="both", linestyle="--", linewidth=0.6, alpha=0.4)

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    # 레전드는 오른쪽에
    ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0), frameon=False)

    # 출처 텍스트
    fig.text(
        0.25, 0.12,
        "출처: FAOSTAT, 2025 데이터 활용",
        ha="center", va="top",
        fontsize=7, color="#555"
    )
    fig.text(
        0.6, 0.93,
        "(단위: $/bushell)",
        ha="center", va="top",
        fontsize=7, color="#555"
    )

    fig.subplots_adjust(right=0.70, bottom=0.22)
    return fig



# ===================== SAVE UTILS =====================
def save_png(fig, filename="chart.png"):
    mpl.use("Agg")
    fig.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close(fig)


def save_pgf(fig, filename):
    mpl.use("pgf")
    fig.savefig(filename)
    plt.close(fig)


# df = pd.DataFrame({
#     "Category": ["A","B","C","D"],
#     "1단계": [23,17,35,29],
#     "2단계": [12,9,14,10],
#     "3단계": [5,4,3,6]
# })
# fig = draw_bar_stack(df, "Category", ["1단계", "2단계", "3단계"])
# save_png(fig, "stack.png")
# # save_pgf(fig, "stack.pgf")



df = pd.DataFrame({
    "Year": [2020, 2021, 2022, 2023],
    "한국": [23, 28, 31, 37],
    "미국": [40, 45, 47, 49],
    "일본": [18, 20, 22, 24],
})

fig = draw_line_multi(df, "Year", ["한국", "미국", "일본"])
save_png(fig, "line.png")
# save_pgf(fig, "line.pgf")
