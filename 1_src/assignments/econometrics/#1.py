import numpy as np
import pandas as pd

# =========================================================
# [기본 데이터 설정]
# =========================================================

# Income categories
# 단위: $1000/year
# 예: 0.5 는 연소득 500달러, 17.5 는 연소득 17,500달러를 의미
x = np.array([0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.7, 8.8, 12.5, 17.5])

# Savings rate categories
# 단위: 비율 자체
# 예: 0.05 = 5%, -0.25 = -25%
y = np.array([-0.25, -0.18, -0.05, 0.00, 0.05, 0.15, 0.25, 0.40, 0.50])

# Frequency matrix N
# 행(row) = savings rate category
# 열(col) = income category
# 각 원소 n_ij = (저축률 i, 소득 j)에 속하는 가구 수
N = np.array([
    [ 9,  9, 10,  8,  9,  7,  5,  3,  2,  3],
    [ 2,  7, 13,  6,  9,  8,  8,  8,  6,  2],
    [ 7, 12, 11,  5, 12, 16, 17, 14,  4,  3],
    [13, 13,  4,  2,  1,  0,  0,  0,  0,  0],
    [10, 23, 33, 31, 41, 32, 47, 39, 42, 10],
    [ 2,  9,  9, 12, 16, 20, 42, 54, 24, 21],
    [ 2,  6,  4,  7, 10, 13, 20, 19, 13,  8],
    [ 1,  2,  6,  7, 10,  7,  9,  8,  8,  7],
    [ 1, 11,  7,  6,  4,  5,  8,  9, 14,  4]
])

# 전체 표본수 n
# 문제에서 총 1021가구라고 했고, 실제로 합이 1021인지 확인
n = N.sum()
print("Total sample size n =", n)

# Joint probability matrix P
# 각 셀의 도수를 전체 표본수 n으로 나눈 값
# 즉, p_ij = n_ij / n
P = N / n

# 보기 좋게 표 형태로 확인하기 위한 라벨
income_labels = [f"{v:g}" for v in x]
savings_labels = [f"{v:g}" for v in y]

# 도수표 DataFrame
freq_df = pd.DataFrame(
    N,
    index=savings_labels,
    columns=income_labels
)

# 결합확률표 DataFrame
prob_df = pd.DataFrame(
    P,
    index=savings_labels,
    columns=income_labels
)

print("\nFrequency table N:")
print(freq_df)

print("\nJoint probability table P = N/n:")
print(prob_df.round(4))


# =========================================================
# [자주 쓰는 기본 합]
# =========================================================

# income의 주변도수 = 열합
# 각 income category에 속한 전체 가구 수
col_freq = N.sum(axis=0)

# savings rate의 주변도수 = 행합
# 각 savings category에 속한 전체 가구 수
row_freq = N.sum(axis=1)

# income의 주변확률 = 열합 / n
col_prob = P.sum(axis=0)

# savings의 주변확률 = 행합 / n
row_prob = P.sum(axis=1)


# =========================================================
# 1번: Marginal frequencies of income and savings rate
# =========================================================

# Income marginal frequencies
# 열 기준 합 -> 각 소득범주의 전체 빈도
income_marg_freq = N.sum(axis=0)

# Savings marginal frequencies
# 행 기준 합 -> 각 저축률범주의 전체 빈도
savings_marg_freq = N.sum(axis=1)

# 주변확률 = 주변도수 / 전체표본수
income_marg_prob = income_marg_freq / n
savings_marg_prob = savings_marg_freq / n

print("\n[1] Income marginal frequencies:")
print(income_marg_freq)

print("\n[1] Income marginal probabilities:")
print(np.round(income_marg_prob, 4))

print("\n[1] Savings marginal frequencies:")
print(savings_marg_freq)

print("\n[1] Savings marginal probabilities:")
print(np.round(savings_marg_prob, 4))


# =========================================================
# 2번: Conditional frequencies of income, conditional on savings
# =========================================================

# P(X | Y)
# 특정 savings rate가 주어졌을 때 income 분포
# 각 행을 그 행의 합(row total)으로 나눔
# 즉, P(X=x_j | Y=y_i) = n_ij / sum_j n_ij
row_freq = N.sum(axis=1)
cond_income_given_savings = N / row_freq[:, None]

cond_df = pd.DataFrame(
    cond_income_given_savings,
    index=savings_labels,
    columns=income_labels
)

print("\n[2] Conditional probabilities of income given savings, P(X|Y):")
print(cond_df.round(4))


# =========================================================
# 3번: Conditional frequencies of savings rate, conditional on income
# =========================================================

# P(Y | X)
# 특정 income이 주어졌을 때 savings rate 분포
# 각 열을 그 열의 합(column total)으로 나눔
# 즉, P(Y=y_i | X=x_j) = n_ij / sum_i n_ij
col_freq = N.sum(axis=0)
cond_savings_given_income = N / col_freq[None, :]

cond_df2 = pd.DataFrame(
    cond_savings_given_income,
    index=savings_labels,
    columns=income_labels
)

print("\n[3] Conditional probabilities of savings given income, P(Y|X):")
print(cond_df2.round(4))


# =========================================================
# 4번: Marginal mean of income and savings rate
# =========================================================

# income의 주변확률벡터 p_X
income_marg_prob = P.sum(axis=0)

# savings의 주변확률벡터 p_Y
savings_marg_prob = P.sum(axis=1)

# E[X] = sum(x_j * p_Xj)
mu_X = x @ income_marg_prob

# E[Y] = sum(y_i * p_Yi)
mu_Y = y @ savings_marg_prob

print("\n[4] Marginal mean of income =", round(mu_X, 4))
print("[4] Marginal mean of savings rate =", round(mu_Y, 4))


# =========================================================
# 5번: Conditional mean of savings rate, conditional on income
# =========================================================

# 이미 구한 P(Y|X)를 다시 사용
col_freq = N.sum(axis=0)
cond_savings_given_income = N / col_freq[None, :]

# 각 income category별 평균 savings rate
# E[Y|X=x_j] = sum(y_i * P(Y=y_i | X=x_j))
cond_mean_savings_given_income = y @ cond_savings_given_income

cond_mean_df = pd.DataFrame({
    "Income": x,
    "E[Savings rate | Income]": cond_mean_savings_given_income
})

print("\n[5] Conditional mean of savings rate given income:")
print(cond_mean_df.round(4))


# =========================================================
# 6번: Marginal variance and standard deviation
# =========================================================

# 주변확률
income_marg_prob = P.sum(axis=0)
savings_marg_prob = P.sum(axis=1)

# 주변평균
mu_X = x @ income_marg_prob
mu_Y = y @ savings_marg_prob

# 제2모멘트
# E[X^2], E[Y^2]
EX2 = (x**2) @ income_marg_prob
EY2 = (y**2) @ savings_marg_prob

# 분산 공식
# Var(X) = E[X^2] - (E[X])^2
# Var(Y) = E[Y^2] - (E[Y])^2
var_X = EX2 - mu_X**2
var_Y = EY2 - mu_Y**2

# 표준편차 = 분산의 제곱근
sd_X = np.sqrt(var_X)
sd_Y = np.sqrt(var_Y)

print("\n[6] Income variance =", round(var_X, 4))
print("[6] Income standard deviation =", round(sd_X, 4))
print("[6] Savings variance =", round(var_Y, 4))
print("[6] Savings standard deviation =", round(sd_Y, 4))


# =========================================================
# 7번: Conditional variance and standard deviation of savings, given income
# =========================================================

# 조건부확률 P(Y|X)
col_freq = N.sum(axis=0)
cond_savings_given_income = N / col_freq[None, :]

# 조건부평균
# E[Y|X=x_j]
cond_mean = y @ cond_savings_given_income

# 조건부 제2모멘트
# E[Y^2 | X=x_j]
cond_second_moment = (y**2) @ cond_savings_given_income

# 조건부분산
# Var(Y|X=x_j) = E[Y^2|X=x_j] - (E[Y|X=x_j])^2
cond_var = cond_second_moment - cond_mean**2

# 조건부표준편차
cond_sd = np.sqrt(cond_var)

cond_var_df = pd.DataFrame({
    "Income": x,
    "E[Y|X]": cond_mean,
    "Var(Y|X)": cond_var,
    "SD(Y|X)": cond_sd
})

print("\n[7] Conditional mean, variance, and standard deviation of savings given income:")
print(cond_var_df.round(4))