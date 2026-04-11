import pandas as pd
import unicodedata
import re

# ===== 입력 파일 =====
UNSD_PATH = "/home/user/문서/workspace/python/out_powerbi/UNSD — Methodology.csv"
ARR_PATH  = "/home/user/문서/workspace/python/out_powerbi/UN_tourism_arrival_monthly.csv"
DIFF_PATH = "/home/user/문서/workspace/python/out_powerbi/diff_country_name_m49.csv"   # columns: UN_tourism_country,M49

# ===== 출력 파일 =====
OUT_PATH  = "/home/user/문서/workspace/python/out_powerbi/UN_tourism_arrival_monthly_with_m49.csv"

def norm(s: str) -> str:
    """이름 차이(악센트/대소문자/&/구두점) 흡수용"""
    if pd.isna(s):
        return ""
    s = str(s).strip()

    # 악센트 제거
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # & -> and, 콤마/점 등 정리
    s = s.replace("&", "and")
    s = re.sub(r"[\u2010-\u2015]", "-", s)      # 다양한 대시 -> -
    s = re.sub(r"[^\w\s\-]", " ", s)            # 문자/숫자/공백/대시 외 제거
    s = re.sub(r"\s+", " ", s).strip().casefold()
    return s

def read_csv_auto(path: str) -> pd.DataFrame:
    # 구분자(콤마/탭/세미콜론 등) 자동 추정
    return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")

# 1) 로드
unsd = read_csv_auto(UNSD_PATH)
arr  = read_csv_auto(ARR_PATH)
diff = read_csv_auto(DIFF_PATH)

# 2) 기본 매핑(정확 문자열)
unsd_map = dict(zip(unsd["Country or Area"].astype(str), pd.to_numeric(unsd["M49 Code"], errors="coerce")))

# 3) 수동 매핑(네 파일)
diff_map = dict(zip(diff["UN_tourism_country"].astype(str), pd.to_numeric(diff["M49"], errors="coerce")))

# 4) 정규화 매핑(backup)
unsd_norm_map = {}
for k, v in unsd_map.items():
    nk = norm(k)
    if nk and pd.notna(v) and nk not in unsd_norm_map:
        unsd_norm_map[nk] = int(v)

diff_norm_map = {}
for k, v in diff_map.items():
    nk = norm(k)
    if nk and pd.notna(v):
        diff_norm_map[nk] = int(v)

# 5) m49 붙이기
arr["m49"] = arr["country"].map(unsd_map)
arr["m49"] = arr["m49"].fillna(arr["country"].map(diff_map))

# 정규화로 한번 더
mask = arr["m49"].isna()
arr.loc[mask, "m49"] = arr.loc[mask, "country"].map(lambda x: unsd_norm_map.get(norm(x)))
mask = arr["m49"].isna()
arr.loc[mask, "m49"] = arr.loc[mask, "country"].map(lambda x: diff_norm_map.get(norm(x)))

# 6) 못 찾은 나라 출력(원하면)
missing = sorted(arr.loc[arr["m49"].isna(), "country"].dropna().unique().tolist())
print(f"Missing m49 countries: {len(missing)}")
if missing:
    print("\n".join(missing))

# 7) 컬럼 순서: m49를 맨 왼쪽으로
arr["m49"] = pd.to_numeric(arr["m49"], errors="coerce").astype("Int64")
out = arr[["m49", "country", "year", "month", "value"]]

out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
print(f"Saved: {OUT_PATH}")
