import csv
import json
from datetime import datetime
from pathlib import Path
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

OUT_DIR = Path("out_powerbi")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PROGRESS_PATH = OUT_DIR / "progress.json"
MASTER_CSV = OUT_DIR / "powerbi_master.csv"
BATCH_DIR = OUT_DIR / "batches"
BATCH_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 20


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


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


def atomic_write_json(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_progress() -> dict:
    if PROGRESS_PATH.exists():
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    return {
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "all_countries": [],
        # ✅ 완료 기준은 이것만!
        "batch_saved": [],
        # 진행중 배치(완료 아님)
        "current_batch_index": 1,
        "current_batch_countries": [],
    }


def save_progress(progress: dict):
    progress["updated_at"] = now_iso()
    atomic_write_json(PROGRESS_PATH, progress)


def ensure_master_header():
    if not MASTER_CSV.exists():
        with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["country", "year", "month", "value"])


def append_rows_to_master(rows):
    if not rows:
        return
    ensure_master_header()
    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)
        f.flush()


def write_batch_csv(batch_index: int, rows) -> Path:
    out = BATCH_DIR / f"powerbi_batch_{batch_index:03d}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["country", "year", "month", "value"])
        w.writerows(rows)
    return out


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


def completed_countries_from_batches(progress: dict) -> set:
    done = set()
    for b in progress.get("batch_saved", []):
        for c in b.get("countries", []):
            done.add(c)
    return done


def main():
    progress = load_progress()

    # ✅ 완료 판단은 batch_saved만 사용
    done_set = completed_countries_from_batches(progress)

    # 진행중 배치 로드(완료 아님)
    batch_index = int(progress.get("current_batch_index", 1))
    current_batch_countries = list(progress.get("current_batch_countries", []))

    # 배치 rows 버퍼(배치 저장 전에는 디스크에 안 씀 → 재시작해도 중복 없음)
    batch_rows_buffer = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL, timeout=200_000)
        page.wait_for_timeout(1500)

        wait_then_click_button_text(page, "Int. Tourist Arrivals", delay_sec=6.0)
        wait_then_click_button_text(page, "Monthly", delay_sec=0.5)
        _wait_visual_update(page, 1200)

        # 1) 전체 국가 목록 수집/로드
        if not progress.get("all_countries"):
            countries = collect_all_items(page, "Country")
            progress["all_countries"] = countries
            save_progress(progress)
            print("✅ Countries collected & saved:", len(countries))
        else:
            countries = progress["all_countries"]
            print("✅ Countries loaded:", len(countries))

        # 2) 배치 저장 완료된 국가(done_set) 제외하고만 진행
        remaining = [c for c in countries if c not in done_set]
        print(f"➡️ Remaining (NOT batch-saved yet): {len(remaining)} / saved: {len(done_set)}")

        # (선택) progress에 남아있던 current_batch_countries는 “완료 아님”이라 다시 돌릴 수 있음.
        # 근데 남아있는 게 있으면, 그냥 리셋하고 처음부터 remaining 기준으로 가는게 깔끔함.
        if current_batch_countries:
            print("⚠️ Found unfinished current batch in progress.json -> reset and redo (no duplication).")
            current_batch_countries = []
            progress["current_batch_countries"] = []
            save_progress(progress)

        for country in remaining:
            print(f"\n=== START COUNTRY: {country} ===")

            if not select_country(page, country):
                print("❌ Country select fail:", country)
                continue

            country_rows = []
            for y in range(YEAR_START, YEAR_END + 1):
                if not select_year(page, str(y)):
                    print("❌ Year select fail:", country, y)
                    continue

                right_click_show_as_table(page)
                rows = collect_table_rows(page, country=country, year=y)
                country_rows.extend(rows)
                go_back_to_report(page)

            # ✅ 배치 버퍼에만 쌓음 (아직 저장 X)
            batch_rows_buffer.extend(country_rows)
            current_batch_countries.append(country)

            # 진행중 배치 상태만 기록(완료 아님)
            progress["current_batch_index"] = batch_index
            progress["current_batch_countries"] = current_batch_countries
            save_progress(progress)

            print(f"✅ COUNTRY DONE (buffered): {country} | rows: {len(country_rows)}")
            print(f"   Batch progress: {len(current_batch_countries)}/{BATCH_SIZE}")

            # ✅ 배치 저장 시점(완료 인정 시점)
            if len(current_batch_countries) >= BATCH_SIZE:
                out_path = write_batch_csv(batch_index, batch_rows_buffer)
                append_rows_to_master(batch_rows_buffer)

                batch_record = {
                    "batch_index": batch_index,
                    "countries": current_batch_countries,
                    "csv": str(out_path),
                    "saved_at": now_iso(),
                    "rows": len(batch_rows_buffer),
                }
                progress["batch_saved"].append(batch_record)

                # 다음 배치로
                batch_index += 1
                current_batch_countries = []
                batch_rows_buffer = []

                progress["current_batch_index"] = batch_index
                progress["current_batch_countries"] = []
                save_progress(progress)

                print(f"✅ BATCH SAVED: {batch_record['batch_index']} → {batch_record['csv']}")

        # 남은 것도 저장(원하면 주석 처리해서 “20개 미만은 다음 실행로 넘김” 가능)
        if current_batch_countries and batch_rows_buffer:
            out_path = write_batch_csv(batch_index, batch_rows_buffer)
            append_rows_to_master(batch_rows_buffer)

            batch_record = {
                "batch_index": batch_index,
                "countries": current_batch_countries,
                "csv": str(out_path),
                "saved_at": now_iso(),
                "rows": len(batch_rows_buffer),
            }
            progress["batch_saved"].append(batch_record)

            # 리셋
            progress["current_batch_index"] = batch_index + 1
            progress["current_batch_countries"] = []
            save_progress(progress)

            print(f"✅ FINAL BATCH SAVED: {batch_record['batch_index']} → {batch_record['csv']}")

        print("\n✅ DONE")
        print("✅ MASTER CSV:", MASTER_CSV)
        print("✅ PROGRESS JSON:", PROGRESS_PATH)

        browser.close()


if __name__ == "__main__":
    main()
