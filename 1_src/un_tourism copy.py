import csv
from playwright.sync_api import sync_playwright

# ==============================
# 0) Power BI URL
# ==============================
URL = "https://app.powerbi.com/view?r=eyJrIjoiYWUxMzc5NWItZjA3Ny00YmM1LTkzODktMDdiMzUzNjczZmYzIiwidCI6IjRiMWJkNWRiLTY3ODItNDY2YS1hMWM1LTRlOTc1NjQ4ZjhlNSIsImMiOjl9"

# ==============================
# 1) 튜닝 값 (필요한 것만)
# ==============================
TIMEOUT = 60_000
WHEEL_TICK = 60
TICK_DELAY_MS = 250
MAX_SCROLLS = 3000

# ✅ "스크롤 3번 시도해도 새 항목이 안 나오면 종료"
STOP_NO_NEW = 3

IGNORE = {"Select all", "(Blank)"}

# ✅ Year는 직접 순회
YEAR_START = 2015
YEAR_END = 2025


# ==============================
# 3) 시각화 업데이트 대기
# ==============================
def _wait_visual_update(page, ms: int = 500):
    page.wait_for_timeout(ms)


# ==============================
# 4) 버튼(탭) 클릭: "Int. Tourist Arrivals", "Monthly"
# ==============================
def wait_then_click_button_text(page, text: str, delay_sec: float = 2) -> bool:
    page.wait_for_timeout(int(delay_sec * 1000))

    btn = page.locator("div.content.text.ui-role-button-text", has_text=text).first
    btn.wait_for(state="attached", timeout=TIMEOUT)
    btn.scroll_into_view_if_needed(timeout=TIMEOUT)
    btn.click(timeout=TIMEOUT, force=True)
    return True


# ==============================
# 5) ESC로 팝업 닫기
# ==============================
def close_popup(page):
    page.keyboard.press("Escape")
    page.wait_for_timeout(100)


# ==============================
# 6) 드롭다운 열기 -> popup_id 얻기
# ==============================
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


# ==============================
# 7) JS로 맨 위로 올리기 / 한틱 내리기
# ==============================
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


# ==============================
# 8) 현재 "보이는" 항목 텍스트 읽기
# ==============================
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


# ==============================
# 9) 클릭: span 말고 role=option 조상 클릭
# ==============================
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


# ==============================
# 10) (A) Country 드롭다운 전체 목록 수집
# ==============================
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

    # ✅ 조회 완료 후 드롭다운 닫기
    close_popup(page)
    return all_items


# ==============================
# 11) (B) Country 선택: 검색 있으면 검색, 없으면 스크롤
#     - 선택 성공/실패와 무관하게 마지막에 닫아줌
# ==============================
def select_country(page, country_text):
    popup_id = open_dropdown(page, "Country")
    scroll_to_top(page, popup_id)
    popup = page.locator(f"#{popup_id}")

    # (1) 검색창 있으면 먼저 검색
    search = popup.locator('input[role="searchbox"]')
    if search.count() > 0 and search.first.is_visible():
        search.first.fill("")
        search.first.type(country_text, delay=10)
        page.wait_for_timeout(TICK_DELAY_MS)

        if click_item_text(page, popup_id, country_text):
            close_popup(page)  # ✅ 선택 후 닫기
            return True

    # (2) 검색으로 못 찾으면 스크롤 탐색
    for _ in range(MAX_SCROLLS):
        if click_item_text(page, popup_id, country_text):
            close_popup(page)  # ✅ 선택 후 닫기
            return True
        scroll_down_one_tick(page, popup_id)

    close_popup(page)
    return False


# ==============================
# 12) (C) Year 선택: (검색 없음 가정) 2015~2025 직접 순회용
#     - Year는 검색이 안되니 "맨위→(필요 시)스크롤→클릭"만
# ==============================
def select_year(page, year_text):
    popup_id = open_dropdown(page, "Year")
    scroll_to_top(page, popup_id)

    # Year는 개수가 작아서, 보통 맨 위 근처에서 금방 잡힘
    for _ in range(MAX_SCROLLS):
        if click_item_text(page, popup_id, year_text):
            close_popup(page)  # ✅ 선택 후 닫기
            return True
        scroll_down_one_tick(page, popup_id)

    close_popup(page)
    return False


# ==============================
# 13) main
# ==============================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL, timeout=200_000)
        page.wait_for_timeout(1500)

        # ✅ (선택) 초기 UI 세팅
        print("clicked Int. Tourist Arrivals:", wait_then_click_button_text(page, "Int. Tourist Arrivals", delay_sec=6.0))
        print("clicked Monthly:", wait_then_click_button_text(page, "Monthly", delay_sec=0.5))
        _wait_visual_update(page, 1200)

        # ---- 1) Country 전체 수집 ----
        countries = collect_all_items(page, "Country")
        print("Countries:", len(countries))

        results = []

        # ---- 2) Country 루프 ----
        for country in countries:
            ok = select_country(page, country)
            if not ok:
                print("Country select fail:", country)
                continue

            # ---- 3) Year는 조회 없이 2015~2025 순회 ----
            clicked_years = []
            for y in range(YEAR_START, YEAR_END + 1):
                y_str = str(y)
                oky = select_year(page, y_str)
                if oky:
                    clicked_years.append(y_str)

            print(country, "clicked_years:", clicked_years)
            results.append([country, "|".join(clicked_years)])

        # ---- 4) 저장 ----
        with open("powerbi_clicked_country_years.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Country", "ClickedYears_pipe_sep"])
            w.writerows(results)

        browser.close()


if __name__ == "__main__":
    main()
