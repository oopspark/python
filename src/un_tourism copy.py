from playwright.sync_api import sync_playwright
import csv

URL = "https://app.powerbi.com/view?r=eyJrIjoiYWUxMzc5NWItZjA3Ny00YmM1LTkzODktMDdiMzUzNjczZmYzIiwidCI6IjRiMWJkNWRiLTY3ODItNDY2YS1hMWM1LTRlOTc1NjQ4ZjhlNSIsImMiOjl9"


def clean_number(text: str):
    if not text:
        return None

    cleaned = (
        text.replace(",", "")
            .replace("\u00a0", "")
            .replace("\u202f", "")
            .strip()
    )

    try:
        return float(cleaned)
    except ValueError:
        return None


def collect_table(page, year: int):
    rows_data = []

    rows = page.query_selector_all('div[role="row"][row-index]')

    for row in rows:
        col1 = row.query_selector('[aria-colindex="1"]')
        col2 = row.query_selector('[aria-colindex="2"]')

        if not col1 or not col2:
            continue

        month = col1.inner_text().strip()
        value = clean_number(col2.inner_text())

        if month and value is not None:
            rows_data.append((year, month, value))

    return rows_data


def save_csv(year: int, rows_data):
    output = f"powerbi_table_{year}.csv"

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year", "month", "value"])
        writer.writerows(rows_data)

    print(f"✅ {output} 저장 완료 ({len(rows_data)} rows)")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto(URL, timeout=60_000)
        page.wait_for_selector('div[role="row"][row-index]', timeout=60_000)

        print("\n🟢 페이지 로드 완료")
        print("👉 브라우저에서 연도를 바꾼 뒤")
        print("👉 터미널에 연도 입력 후 엔터")
        print("👉 종료하려면 q / exit 입력\n")

        while True:
            user_input = input("연도 입력 > ").strip()

            if user_input.lower() in {"q", "exit"}:
                print("👋 종료합니다")
                break

            if not user_input.isdigit():
                print("❌ 연도를 숫자로 입력하세요")
                continue

            year = int(user_input)

            input("▶ 브라우저에서 연도 설정 완료 후 엔터를 누르세요...")

            rows_data = collect_table(page, year)
            save_csv(year, rows_data)

        browser.close()


if __name__ == "__main__":
    main()
