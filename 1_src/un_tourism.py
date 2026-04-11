from playwright.sync_api import sync_playwright
import csv

URL = "https://app.powerbi.com/view?r=eyJrIjoiYWUxMzc5NWItZjA3Ny00YmM1LTkzODktMDdiMzUzNjczZmYzIiwidCI6IjRiMWJkNWRiLTY3ODItNDY2YS1hMWM1LTRlOTc1NjQ4ZjhlNSIsImMiOjl9"

YEAR = 2016
OUTPUT = f"powerbi_table_{YEAR}.csv"


def clean_number(text: str) -> float | None:
    """
    Power BI 숫자 셀 정규화
    - 콤마 제거
    - NBSP, thin space 제거
    - 정수 / 소수 모두 처리
    """
    if not text:
        return None

    cleaned = (
        text.replace(",", "")
            .replace("\u00a0", "")   # NBSP
            .replace("\u202f", "")   # thin space
            .strip()
    )

    try:
        return float(cleaned)
    except ValueError:
        return None


def main():
    rows_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto(URL, timeout=60_000)

        page.wait_for_selector('div[role="row"][row-index]', timeout=60_000)

        print("▶ 엔터를 누르면 데이터 수집을 시작합니다...")
        input()

        rows = page.query_selector_all('div[role="row"][row-index]')

        for row in rows:
            col1 = row.query_selector('[aria-colindex="1"]')
            col2 = row.query_selector('[aria-colindex="2"]')

            if not col1 or not col2:
                continue

            month = col1.inner_text().strip()
            value = clean_number(col2.inner_text())

            if month and value is not None:
                rows_data.append((YEAR, month, value))

        browser.close()

    # CSV 저장
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year", "month", "value"])
        writer.writerows(rows_data)

    print(f"✅ 저장 완료: {len(rows_data)} rows → {OUTPUT}")


if __name__ == "__main__":
    main()
