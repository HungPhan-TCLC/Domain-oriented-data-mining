import json
import re
import time
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


# =========================
# CONFIG
# =========================
AZ_URL = "https://tamanhhospital.vn/benh-hoc-a-z/"
OUT_JSONL = "tamanh_sections.jsonl"

HEADLESS = True
WAIT_SEC = 25
SCROLL_ROUNDS = 8
SLEEP_BETWEEN_PAGES_SEC = 0.3  # giảm tải server + ổn định

# test nhanh: đặt LIMIT_DISEASES = 5; khi chạy thật set None
LIMIT_DISEASES = None

# Bỏ qua section quá ngắn (đỡ nhiễu)
MIN_CONTENT_CHARS = 120


# =========================
# Driver
# =========================
def create_driver(headless: bool = True) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    driver = webdriver.Chrome(options=opts)
    return driver


# =========================
# Helpers
# =========================

def is_faq_section(title: str) -> bool:
    t = title.lower()
    return any(k in t for k in [
        "câu hỏi thường gặp",
        "câu hỏi hay gặp",
        "thắc mắc",
        "giải đáp thắc mắc",
        "hỏi đáp"
    ])

def clean_text(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\r\n", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] if path else ""
    return slug or "unknown"


def stable_id(url: str, anchor: str) -> str:
    # ID ổn định theo (url + anchor), không phụ thuộc index
    h = hashlib.md5(f"{url}#{anchor}".encode("utf-8")).hexdigest()[:10]
    return f"TA_{h}"


def map_category(title: str) -> str:
    t = title.lower()

    # Triệu chứng / dấu hiệu
    if any(k in t for k in ["triệu chứng", "dấu hiệu", "biến chứng", "biểu hiện"]):
        return "Triệu chứng/Dấu hiệu"

    # Thuốc / điều trị / phòng ngừa
    if any(
        k in t
        for k in [
            "điều trị",
            "thuốc",
            "phòng ngừa",
            "biện pháp",
            "chăm sóc",
            "cách phòng",
            "phòng bệnh",
        ]
    ):
        return "Thuốc"

    # Còn lại: Bệnh (bao gồm là gì, phân loại, phổ biến, nguyên nhân, chẩn đoán...)
    return "Bệnh"


def scroll_to_bottom(driver: webdriver.Chrome, rounds: int = 6, sleep_sec: float = 1.0):
    last_h = driver.execute_script("return document.body.scrollHeight")
    for i in range(rounds):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep_sec)
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            break
        last_h = new_h


# =========================
# A–Z: get disease URLs
# =========================
def get_disease_urls_from_az(driver: webdriver.Chrome) -> List[str]:
    print("[INFO] Open A–Z page:", AZ_URL)
    driver.get(AZ_URL)

    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    scroll_to_bottom(driver, rounds=SCROLL_ROUNDS, sleep_sec=1.0)

    soup = BeautifulSoup(driver.page_source, "lxml")

    urls = []
    seen = set()

    for a in soup.select("a[href*='/benh/']"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin("https://tamanhhospital.vn", href)

        # chỉ lấy dạng .../benh/<slug>/
        if not re.search(r"^https?://(www\.)?tamanhhospital\.vn/benh/[^/]+/?$", href):
            continue

        if href in seen:
            continue
        seen.add(href)
        urls.append(href)

    print(f"[INFO] Found {len(urls)} disease URLs from A–Z")
    return urls


# =========================
# TOC extraction (robust)
# =========================
def find_toc_container(soup: BeautifulSoup):
    """
    Tìm block chứa chữ 'Mục lục' và các link #anchor.
    Heuristic: tìm text 'Mục lục', rồi leo lên vài cấp đến khi thấy <a href="#...">
    """
    label = soup.find(string=lambda x: isinstance(x, str) and x.strip().lower() == "mục lục")
    if not label:
        return None

    node = label.parent
    for _ in range(6):
        if not node:
            break
        if node.find_all("a", href=re.compile(r"^#")):
            return node
        node = node.parent
    return None


def extract_toc_items_h2(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Trả về list các mục lục cấp 1 (ưu tiên h2):
      [{title, anchor}]
    Nếu TOC có mục con (1.1/1.2) -> thường tương ứng h3; ta lọc để giữ mục cấp 1.
    """
    toc = find_toc_container(soup)
    if not toc:
        return []

    # lấy tất cả link #...
    raw = []
    for a in toc.find_all("a", href=re.compile(r"^#")):
        title = a.get_text(" ", strip=True)
        href = (a.get("href") or "").strip()
        if not title or not href.startswith("#"):
            continue
        raw.append({"title": title, "anchor": href[1:]})

    # dedup
    seen = set()
    dedup = []
    for it in raw:
        key = (it["title"], it["anchor"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(it)

    # Lọc: chỉ giữ những anchor trỏ tới H2 (mục cấp 1)
    items = []
    for it in dedup:
        target = soup.find(id=it["anchor"])
        if not target:
            continue
        if target.name and target.name.lower() == "h2":
            items.append(it)

    # Nếu lọc xong mà rỗng, fallback: giữ raw (vẫn cắt theo heading id, dừng ở h2)
    return items if items else dedup


# =========================
# Section extraction: from H2 to next H2
# =========================
CONTENT_TAGS = {"p", "li", "ul", "ol", "table", "blockquote"}


def extract_section_content_from_h2(soup: BeautifulSoup, anchor_id: str) -> str:
    """
    - Start: heading id=anchor_id (thường là h2)
    - Collect: p, li, ul/ol/table/blockquote dưới section
    - Stop: gặp h2 tiếp theo (mục cấp 1 mới)
    """
    start = soup.find(id=anchor_id)
    if not start:
        return ""

    parts: List[str] = []

    # Duyệt các node sau start theo thứ tự tài liệu
    for node in start.find_all_next():
        # stop ở h2 mới
        if node is not start and node.name and node.name.lower() == "h2":
            break

        # lấy nội dung các tag giàu text
        if node.name in CONTENT_TAGS:
            txt = node.get_text(" ", strip=True)
            if txt:
                parts.append(txt)

    return clean_text("\n".join(parts))


# =========================
# Crawl one disease page -> many records
# =========================
def crawl_one_disease_sections(driver: webdriver.Chrome, url: str) -> Tuple[str, List[Dict]]:
    driver.get(url)
    WebDriverWait(driver, WAIT_SEC).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    soup = BeautifulSoup(driver.page_source, "lxml")

    h1 = soup.find("h1")
    disease_title = h1.get_text(" ", strip=True) if h1 else slug_from_url(url)

    toc_items = extract_toc_items_h2(soup)
    if not toc_items:
        print(f"[WARN] No TOC found: {url}")
        return disease_title, []

    records = []
    for it in toc_items:
        title = it["title"].strip()
        anchor = it["anchor"].strip()

        # 🚫 BỎ FAQ / THẮC MẮC
        if is_faq_section(title):
            continue

        content = extract_section_content_from_h2(soup, anchor)
        if len(content) < MIN_CONTENT_CHARS:
            continue

        rec = {
            "id": stable_id(url, title),
            "category": map_category(title),
            "title": title,
            "content": content,
            "source": "tamanhhospital",
            "url": url
        }
        records.append(rec)


    return disease_title, records


# =========================
# Main pipeline
# =========================
def main(limit_diseases: Optional[int] = None):
    driver = create_driver(headless=HEADLESS)
    total_sections = 0
    total_diseases = 0

    try:
        urls = get_disease_urls_from_az(driver)
        if limit_diseases:
            urls = urls[:limit_diseases]
            print(f"[INFO] LIMIT diseases = {limit_diseases}")

        with open(OUT_JSONL, "w", encoding="utf-8") as f:
            for i, url in enumerate(urls, start=1):
                print(f"\n[INFO] [{i}/{len(urls)}] Crawl disease: {url}")
                try:
                    disease_title, records = crawl_one_disease_sections(driver, url)
                except TimeoutException:
                    print("[ERROR] Timeout:", url)
                    continue
                except WebDriverException as e:
                    print("[ERROR] WebDriverException:", e)
                    continue
                except Exception as e:
                    print("[ERROR] Unknown error:", e)
                    continue

                total_diseases += 1
                print(f"[INFO] Disease title: {disease_title}")
                print(f"[INFO] Sections extracted: {len(records)}")

                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                total_sections += len(records)

                time.sleep(SLEEP_BETWEEN_PAGES_SEC)

        print("\n[DONE]")
        print("  Diseases processed:", total_diseases)
        print("  Sections saved    :", total_sections)
        print("  Output file       :", OUT_JSONL)

    finally:
        driver.quit()


if __name__ == "__main__":
    main(limit_diseases=LIMIT_DISEASES)
