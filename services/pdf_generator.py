"""
PDF generation service using Playwright.
Persistent browser instance for fast PDF rendering with dynamic table column widths.
"""

import asyncio
import time
import atexit
from playwright.async_api import async_playwright, Browser, Playwright

_pdf_semaphore = asyncio.Semaphore(3)

# Persistent browser singleton
_playwright_instance: Playwright | None = None
_browser_instance: Browser | None = None
_browser_lock = asyncio.Lock()


async def _get_browser() -> Browser:
    """Return persistent browser instance, launching once on first call."""
    global _playwright_instance, _browser_instance

    if _browser_instance and _browser_instance.is_connected():
        return _browser_instance

    async with _browser_lock:
        # Double-check after acquiring lock
        if _browser_instance and _browser_instance.is_connected():
            return _browser_instance

        _playwright_instance = await async_playwright().start()
        _browser_instance = await _playwright_instance.chromium.launch(
            headless=True,
            args=[
                "--allow-file-access-from-files",
                "--font-render-hinting=none",
                "--disable-gpu",
                "--disable-dev-shm-usage",
            ]
        )
        return _browser_instance


async def shutdown_browser():
    """Gracefully close persistent browser. Called on server shutdown."""
    global _browser_instance, _playwright_instance
    if _browser_instance:
        await _browser_instance.close()
        _browser_instance = None
    if _playwright_instance:
        await _playwright_instance.stop()
        _playwright_instance = None


async def generate_pdf(html_content: str, margins: dict = None) -> bytes:
    """Render HTML via persistent Playwright Chromium and return PDF binary."""
    if margins is None:
        margins = {"top": "20mm", "bottom": "20mm", "left": "15mm", "right": "15mm"}

    async with _pdf_semaphore:
        browser = await _get_browser()
        page = await browser.new_page()

        try:
            # Fast content injection — no networkidle wait needed since CSS is inlined
            await page.set_content(html_content, wait_until="domcontentloaded")

            # Precision Mermaid wait — only wait if diagrams exist
            has_mermaid = await page.evaluate("() => document.querySelectorAll('.mermaid').length > 0")
            if has_mermaid:
                try:
                    await page.wait_for_selector(
                        '.mermaid[data-processed="true"], .mermaid svg',
                        timeout=8000
                    )
                    # Brief settle for SVG rendering
                    await page.wait_for_timeout(300)
                except Exception:
                    # Mermaid may not finish — proceed with whatever rendered
                    pass

            # Dynamic table column width engine
            await page.evaluate("""() => {
                document.querySelectorAll('table').forEach(table => {
                    const rows = table.querySelectorAll('tr');
                    if (rows.length === 0) return;

                    const colMaxLengths = [];
                    rows.forEach(row => {
                        const cells = row.querySelectorAll('th, td');
                        cells.forEach((cell, i) => {
                            const len = (cell.innerText || '').length;
                            colMaxLengths[i] = Math.max(colMaxLengths[i] || 0, len);
                        });
                    });

                    const totalLen = colMaxLengths.reduce((a, b) => a + b, 0) || 1;
                    const MIN_WIDTH = 8;

                    let widths = colMaxLengths.map(len => Math.max(MIN_WIDTH, (len / totalLen) * 100));
                    const widthSum = widths.reduce((a, b) => a + b, 0);
                    if (widthSum > 100) {
                        widths = widths.map(w => (w / widthSum) * 100);
                    }

                    rows.forEach(row => {
                        const cells = row.querySelectorAll('th, td');
                        cells.forEach((cell, i) => {
                            if (i < widths.length) {
                                cell.style.width = widths[i].toFixed(1) + '%';
                                cell.style.minWidth = MIN_WIDTH + '%';
                                cell.style.wordBreak = 'break-all';
                                cell.style.overflowWrap = 'break-word';
                            }
                        });
                    });
                });
            }""")

            await page.emulate_media(media="print")

            pdf_bytes = await page.pdf(
                format="A4",
                margin=margins,
                print_background=True,
                prefer_css_page_size=False
            )
        finally:
            await page.close()

    return pdf_bytes
