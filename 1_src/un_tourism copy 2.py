import csv
from playwright.sync_api import sync_playwright

URL = "https://app.powerbi.com/view?r=eyJrIjoiYWUxMzc5NWItZjA3Ny00YmM1LTkzODktMDdiMzUzNjczZmYzIiwidCI6IjRiMWJkNWRiLTY3ODItNDY2YS1hMWM1LTRlOTc1NjQ4ZjhlNSIsImMiOjl9"

TIMEOUT = 60_000
WHEEL_TICK = 60
TICK_DELAY_MS = 250
MAX_SCROLLS = 3000
STOP_NO_NEW = 3
IGNORE = {"Select all", "(Blank)"}

YEAR_START = 2015
YEAR_END = 2025


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


def _wait_visual_update(page, ms: int = 500):
    page.wait_for_timeout(ms)


def wait_then_click_button_text(page, text: str, delay_sec: float = 2) -> bool:
    page.wait_for_timeout(int(delay_sec * 1000))
    btn = page.locator("div.content.text.ui-role-button-text", has_text=text).first
    btn.wait_for(state="attached", timeout=TIMEOUT)
    btn.scroll_into_view_if_needed(timeout=TIMEOUT)
    btn.click(timeout=TIMEOUT, force=True)
    return True


def close_popup(page):
    page.keyboard.press("Escape")
    page.wait_for_timeout(100)


def open_dropdown(page, aria_label):
    close_popup(page)

    combo = page.locator(
        f'div.slicer-dropdown-menu[data-testid="slicer-dropdown"][role="combobox"][aria-label="{aria_label}"]'
    ).first
    combo.wait_for(state="attached", timeout=TIMEOUT)
    combo.scroll_into_view_if_needed(timeout=TIMEOUT)
    combo.click(timeout=TIMEOUT, force=True)

    page.wait_for_timeout(200)

    popup_id = combo.get_attribute("aria-controls")
    page.locator(f"#{popup_id}").wait_for(state="attached", timeout=TIMEOUT)
    return popup_id


def scroll_to_top(page, popup_id):
    page.evaluate(
        """
        (popup_id) => {
          const root = document.getElementById(popup_id);
          const all = Array.from(root.querySelectorAll('*'));
          const scrollables = all.filter(el => el.scrollHeight > el.clientHeight + 5);
          scrollables.sort((a,b) => (b.scrollHeight-b.clientHeight) - (a.scrollHeight-a.clientHeight));
          const target = scrollables[0] || root;
          target.scrollTop = 0;
          scrollables.forEach(el => el.scrollTop = 0);
        }
        """,
        popup_id,
    )
    page.wait_for_timeout(TICK_DELAY_MS)


def scroll_down_one_tick(page, popup_id):
    page.evaluate(
        """
        ({popup_id, amount}) => {
          const root = document.getElementById(popup_id);
          const all = Array.from(root.querySelectorAll('*'));
          const scrollables = all.filter(el => el.scrollHeight > el.clientHeight + 5);
          scrollables.sort((a,b) => (b.scrollHeight-b.clientHeight) - (a.scrollHeight-a.clientHeight));
          const target = scrollables[0] || root;
          target.scrollTop = (target.scrollTop || 0) + amount;
          scrollables.forEach(el => el.scrollTop = (el.scrollTop || 0) + amount);
        }
        """,
        {"popup_id": popup_id, "amount": WHEEL_TICK},
    )
    page.wait_for_timeout(TICK_DELAY_MS)


def scan_visible_texts(page, popup_id):
    popup = page.locator(f"#{popup_id}")
    popup.locator("span.slicerText").first.wait_for(state="attached", timeout=TIMEOUT)

    items = popup.locator("span.slicerText")
    texts = []
    for i in range(items.count()):
        it = items.nth(i)
        if it.is_visible():
            t = (it.inner_text() or "").strip()
            if t:
                texts.append(t)
    return texts


def click_item_text(page, popup_id, target_text):
    popup = page.locator(f"#{popup_id}")
    items = popup.locator("span.slicerText")

    for i in range(items.count()):
        it = items.nth(i)
        if not it.is_visible():
            continue

        t = (it.inner_text() or "").strip()
        if t != target_text:
            continue

        option = it.locator("xpath=ancestor::*[@role='option'][1]")
        option.first.click(timeout=TIMEOUT, force=True)
        page.wait_for_timeout(450)
        return True

    return False


def collect_all_items(page, aria_label):
    popup_id = open_dropdown(page, aria_label)
    scroll_to_top(page, popup_id)

    seen = set()
    all_items = []
    no_new = 0

    for _ in range(MAX_SCROLLS):
        vis = scan_visible_texts(page, popup_id)

        new_count = 0
        for t in vis:
            if t in IGNORE:
                continue
            if t not in seen:
                seen.add(t)
                all_items.append(t)
                new_count += 1

        no_new = no_new + 1 if new_count == 0 else 0
        if no_new >= STOP_NO_NEW:
            break

        scroll_down_one_tick(page, popup_id)

    close_popup(page)
    return all_items


def select_country(page, country_text):
    popup_id = open_dropdown(page, "Country")
    scroll_to_top(page, popup_id)
    popup = page.locator(f"#{popup_id}")

    search = popup.locator('input[role="searchbox"]')
    if search.count() > 0 and search.first.is_visible():
        search.first.fill("")
        search.first.type(country_text, delay=10)
        page.wait_for_timeout(TICK_DELAY_MS)

        if click_item_text(page, popup_id, country_text):
            close_popup(page)
            return True

    for _ in range(MAX_SCROLLS):
        if click_item_text(page, popup_id, country_text):
            close_popup(page)
            return True
        scroll_down_one_tick(page, popup_id)

    close_popup(page)
    return False


def select_year(page, year_text):
    popup_id = open_dropdown(page, "Year")
    scroll_to_top(page, popup_id)

    for _ in range(MAX_SCROLLS):
        if click_item_text(page, popup_id, year_text):
            close_popup(page)
            return True
        scroll_down_one_tick(page, popup_id)

    close_popup(page)
    return False


def right_click_show_as_table(page) -> bool:
    chart = page.locator('svg.cartesianChart').first
    chart.wait_for(state="attached", timeout=TIMEOUT)
    chart.scroll_into_view_if_needed(timeout=TIMEOUT)

    chart.click(button="right", timeout=TIMEOUT)

    item = page.get_by_role("menuitem", name="테이블로 표시")
    item.wait_for(state="visible", timeout=TIMEOUT)
    item.click(timeout=TIMEOUT)

    _wait_visual_update(page, 800)
    return True


def collect_table_rows(page, country: str, year: int):
    page.wait_for_selector('div[role="row"][row-index]', timeout=TIMEOUT)

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
            rows_data.append((country, year, month, value))

    return rows_data


def go_back_to_report(page) -> bool:
    btn = page.locator('button[data-testid="back-to-report-button"]').first
    btn.wait_for(state="visible", timeout=TIMEOUT)
    btn.click(timeout=TIMEOUT)
    _wait_visual_update(page, 800)
    return True


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL, timeout=200_000)
        page.wait_for_timeout(1500)

        print("clicked Int. Tourist Arrivals:", wait_then_click_button_text(page, "Int. Tourist Arrivals", delay_sec=6.0))
        print("clicked Monthly:", wait_then_click_button_text(page, "Monthly", delay_sec=0.5))
        _wait_visual_update(page, 1200)

        countries = collect_all_items(page, "Country")
        print("Countries:", len(countries))

        all_rows = []

        for country in countries:
            start_idx = len(all_rows)  # ✅ 이 국가 시작 인덱스

            ok = select_country(page, country)
            if not ok:
                print("Country select fail:", country)
                continue

            for y in range(YEAR_START, YEAR_END + 1):
                y_str = str(y)

                oky = select_year(page, y_str)
                if not oky:
                    print("Year select fail:", country, y_str)
                    continue

                right_click_show_as_table(page)

                rows = collect_table_rows(page, country=country, year=y)
                all_rows.extend(rows)
                print(country, y, "rows:", len(rows))

                go_back_to_report(page)

            # ✅ 한 국가 사이클 끝났을 때, 그 나라 데이터 확인 출력
            country_rows = all_rows[start_idx:]
            print("\n==============================")
            print(f"✅ COUNTRY DONE: {country}")
            print(f"   rows collected: {len(country_rows)}")
            print(f"   first 5 rows: {country_rows[:5]}")
            print(f"   last  5 rows: {country_rows[-5:]}")
            print("==============================\n")

        out_path = "powerbi_country_year_month_value.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["country", "year", "month", "value"])
            w.writerows(all_rows)

        print(f"✅ 저장 완료: {len(all_rows)} rows → {out_path}")
        browser.close()


if __name__ == "__main__":
    main()
