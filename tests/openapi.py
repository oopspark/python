import requests
import pandas as pd

# 1. API 요청
url = "http://211.237.50.150:7080/openapi/sample/json/Grid_20150406000000000217_1/1/5"
params = {
    "serviceKey": "bcabae00f6609258dbf869a83684b4954850a746384021f22b06b028ffeb2f01",
    "EXAMIN_DE": "20250820",
    "PRDLST_CD": "211",
    "SPCIES_CD": "02"
}

response = requests.get(url, params=params)
data_json = response.json()

# 2. JSON 안의 실제 데이터 추출
# 대부분 공공데이터 API는 { "데이터셋명": { "row": [...] } } 구조
dataset_name = "Grid_20150406000000000217_1"
rows = data_json.get(dataset_name, {}).get("row", [])

# 3. pandas DataFrame으로 변환
df = pd.DataFrame(rows)

# 4. CSV로 저장
df.to_csv("api_result.csv", index=False, encoding="utf-8-sig")  # 한글 깨짐 방지용 utf-8-sig

print("CSV 저장 완료! (파일명: api_result.csv)")
