import asyncio
from datetime import datetime
from playwright.async_api import async_playwright


URL = "https://example.com/calendar"  # 👉 실제 캘린더 URL로 바꿔줘
REFRESH_INTERVAL_SEC = 5 * 60        # 5분마다 새로고침 (원하면 수정 가능)


def today_str_for_data_attr() -> str:
    """YYYY-MM-DD 형식으로 오늘 날짜 문자열 만들기."""
    today = datetime.today()
    return today.strftime("%Y-%m-%d")  # 예: 2026-01-31


async def center_today_week(page):
    """캘린더에서 오늘 날짜가 포함된 주(또는 셀)를 화면 중앙에 위치시키기."""
    today = today_str_for_data_attr()
    # 여기서 data-date 속성이 있는 날짜 셀을 가정
    day_cell = page.locator(f'[data-date="{today}"]')

    # 元素可见之前先等一下
    await day_cell.wait_for(state="visible", timeout=10_000)

    # 🔥 주(week-row) 단위로 중앙에 맞추고 싶다면 .closest('.week-row')
    await day_cell.evaluate(
        """
        (el) => {
            // 필요하다면 주(week) 요소까지 올라가기
            const weekRow = el.closest('.week-row');
            const target = weekRow ?? el;  // week-row가 있으면 그걸, 없으면 el 자체

            const rect = target.getBoundingClientRect();
            const absoluteY = rect.top + window.scrollY;

            const targetCenterY = absoluteY + rect.height / 2;
            const viewportCenterY = window.innerHeight / 2;
            const scrollTop = targetCenterY - viewportCenterY;

            window.scrollTo({
                top: scrollTop,
                behavior: 'instant', // 或者 'smooth' 如果你想要动画效果
            });
        }
        """
    )

    print(f"Centered week containing {today}")


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False  # 화면 보면서 확인하고 싶으면 False 유지
        )
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        while True:
            # 1) 페이지 새로 열거나 새로고침
            await page.goto(URL, wait_until="networkidle")
            # 또는 이미 열려 있다면:
            # await page.reload(wait_until="networkidle")

            # 2) 오늘 날짜가 포함된 주를 화면 중앙에
            await center_today_week(page)

            # 3) 다음 새로고침까지 대기
            print(f"Sleeping {REFRESH_INTERVAL_SEC} seconds...")
            await asyncio.sleep(REFRESH_INTERVAL_SEC)

        # 실제로 while True라서 도달 안 하지만, 참고용:
        # await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
