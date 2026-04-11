import asyncio
from playwright.async_api import async_playwright

URL = "https://lnl.snu.ac.kr/category/board-155-rv-u08gfg8f-20231013165119/"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # 화면 보면서 디버깅하고 싶으면 False 유지
            slow_mo=200,     # 동작 하나하나 천천히 보고 싶으면 약간 딜레이
        )
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        # 1) 페이지 접속
        await page.goto(URL, wait_until="networkidle")

        # 2) "신청하기" 버튼 클릭
        # <a href="#none" class="btn_status" onclick="go_board_view('3735');">신청하기</a>
        await page.locator("a.btn_status", has_text="신청하기").click()

        # 3) 약관 체크박스 보일 때까지 기다렸다가 체크
        # <input type="checkbox" id="reserve_terms_agree" name="reserve_terms_agree">
        await page.wait_for_selector("#reserve_terms_agree")
        await page.check("#reserve_terms_agree")

        # 4) "다음" 버튼 클릭
        # <a href="#none" onclick="go_board_view('3735');" class="btn_mid btn_gray02">다음</a>
        await page.locator("a.btn_mid.btn_gray02", has_text="다음").click()

        # 5) 캘린더 로드 기다리기
        # <div id="calendar" class="calendar_area ...">
        await page.wait_for_selector("#calendar td.fc-daygrid-day")

        # 6) 오늘 날짜가 포함된 주(tr)를 화면 중앙으로 스크롤
        await page.evaluate(
            """
            () => {
                // FullCalendar에서 오늘 날짜 셀은 td.fc-day-today 로 표시됨
                const todayCell = document.querySelector('#calendar td.fc-day-today');
                if (!todayCell) {
                    console.warn('fc-day-today cell not found');
                    return;
                }

                // 오늘이 있는 주(week row) = 가장 가까운 <tr>
                const weekRow = todayCell.closest('tr');
                const target = weekRow || todayCell;

                const rect = target.getBoundingClientRect();
                const absoluteY = rect.top + window.scrollY;

                const targetCenterY = absoluteY + rect.height / 2;
                const viewportCenterY = window.innerHeight / 2;
                const scrollTop = targetCenterY - viewportCenterY;

                window.scrollTo({
                    top: scrollTop,
                    behavior: 'instant',  // 'smooth'로 바꾸면 부드럽게 스크롤됨
                });
            }
            """
        )

        print("완료: 오늘 날짜가 포함된 주를 화면 가운데에 위치시켰습니다.")

        # 결과를 눈으로 보고 싶으면 잠깐 대기
        await page.wait_for_timeout(10_000)

        # 원하면 브라우저 닫기
        # await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
