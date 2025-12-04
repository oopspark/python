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
    "font.size": 9, 
    "axes.titlesize": 11, 
    "axes.labelsize": 9,
    "xtick.labelsize": 8, 
    "ytick.labelsize": 8,
})

colors = ["#264653", "#2A9D8F", "#E9C46A", "#F4A261", "#E76F51"]


### 데이터 입력

df = pd.DataFrame({
    "Year": ["2020-01-31", "2020-12-01", "2022-12-01", "2023-12-01"],
    "한국": [23, 28, 31, 37],
})


### 데이터 지정

df["Year"] = pd.to_datetime(df["Year"])
x = df["Year"].values
idx = np.arange(len(x))

y = df["한국"].values

### 그래프 작성

fig, ax = plt.subplots()
ax.plot(idx, y, marker="o", markersize=3, color=colors[0], linewidth=1.2)


### 데이터 표시 텍스트

for j, v in enumerate(y):
    ax.annotate(f"{v:.0f}", (idx[j], v), xytext=(0, 2),
                textcoords="offset points", ha="center", va="bottom",
                fontsize=7, color="#222")
    
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

y_min, y_max = 25, 35
step = 5  # 원하는 간격
ax.set_yticks(np.arange(y_min, y_max + step, step))



### 그리드

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)




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

### 저장

# PNG 저장
mpl.use("Agg")
fig.savefig("chart.png", dpi=300, bbox_inches="tight")
plt.close(fig)

# ----- OR -----

# PGF 저장
# mpl.use("pgf")
# fig.savefig("/path/to/chart.pgf")
# plt.close(fig)
