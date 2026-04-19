import pandas as pd
import numpy as np
from scipy import stats

# =========================
# 1. 데이터 불러오기
# =========================
file_path = "/home/wlals788/workspace/python/1_src/assignments/econometrics_2/HW2 data.csv"   # CSV 파일명
df = pd.read_csv(file_path)

print("컬럼 목록:", df.columns.tolist())
print(df.head())

# -------------------------
# 사용할 변수 선택
# -------------------------
if "profit" in df.columns:
    y = df["profit"].dropna().astype(float)
else:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        raise ValueError("숫자형 컬럼을 찾지 못했습니다. CSV 파일을 확인하세요.")
    print(f"'profit' 컬럼이 없어서 첫 번째 숫자형 컬럼 '{numeric_cols[0]}'을 사용합니다.")
    y = df[numeric_cols[0]].dropna().astype(float)

# numpy array로 변환
y = y.to_numpy()
n = len(y)

if n == 0:
    raise ValueError("관측치가 없습니다.")

print(f"\n표본 크기 n = {n}")

# =========================
# 2. 평균과 분산 추정
# =========================
y_bar = np.mean(y)

# MLE 분산추정량 (분모 n)
sigma2_ml = np.mean((y - y_bar) ** 2)

# 불편분산추정량 (분모 n-1)
s2_unbiased = np.var(y, ddof=1)

print("\n[1] Mean estimation")
print(f"Sample mean = {y_bar:.6f}")

print("\n[2] Variance estimation")
print(f"MLE variance = {sigma2_ml:.6f}")
print(f"Unbiased variance = {s2_unbiased:.6f}")

# =========================
# 3. H0: mu = 10 검정
#    (단위가 $1,000 이므로 $10,000 = 10)
# =========================
mu0 = 10.0

# small-sample t-test
s = np.sqrt(s2_unbiased)
t_stat = (y_bar - mu0) / (s / np.sqrt(n))
p_value_t = 2 * (1 - stats.t.cdf(abs(t_stat), df=n - 1))

# finite-sample LR equivalent statistic = t^2
F_stat = t_stat ** 2
p_value_F = 1 - stats.f.cdf(F_stat, 1, n - 1)

# restricted variance under H0
sigma2_0 = np.mean((y - mu0) ** 2)

# asymptotic LR statistic
LR = n * np.log(sigma2_0 / sigma2_ml)
p_value_LR = 1 - stats.chi2.cdf(LR, df=1)

print("\n[3] Hypothesis test: H0: mu = 10 vs H1: mu != 10")
print(f"Restricted variance under H0 = {sigma2_0:.6f}")

print("\nSmall-sample version")
print(f"t statistic = {t_stat:.6f}")
print(f"two-sided p-value (t-test) = {p_value_t:.6f}")
print(f"F statistic = t^2 = {F_stat:.6f}")
print(f"p-value (F test) = {p_value_F:.6f}")

print("\nAsymptotic LR version")
print(f"LR statistic = {LR:.6f}")
print(f"p-value (chi-square(1)) = {p_value_LR:.6f}")

# =========================
# 4. 5% 유의수준 결론
# =========================
alpha = 0.05

print("\n[4] Decision at 5% significance level")
if p_value_t < alpha:
    print("Small-sample test: Reject H0")
else:
    print("Small-sample test: Fail to reject H0")

if p_value_LR < alpha:
    print("Asymptotic LR test: Reject H0")
else:
    print("Asymptotic LR test: Fail to reject H0")