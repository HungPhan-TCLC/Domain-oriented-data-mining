import json
import time
import re
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup


INPUT_LIST = "C:/Users/admin/Downloads/vinmec_diseases_all_letters_selenium.json"
OUTPUT_JSON = "vinmec_diseases_detail_formatted.json"


def load_disease_list(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def make_id_from_url(url: str, tab_name: str = "") -> str:
    """
    Sinh id ổn định từ slug Vinmec + tab name.
    Ví dụ:
      https://www.vinmec.com/vie/benh/addison-suy-tuyen-thuong-than-nguyen-phat-4696
      tab = "trieu-chung"
      -> id = VINMEC_ADDISON_SUY_TUYEN_THUONG_THAN_NGUYEN_PHAT_4696_TRIEU_CHUNG
    """
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] if path else "UNKNOWN"
    slug_norm = re.sub(r"[^0-9a-zA-Z]+", "_", slug).strip("_")
    
    if tab_name:
        tab_norm = re.sub(r"[^0-9a-zA-Z]+", "_", tab_name).strip("_")
        return f"VINMEC_{slug_norm.upper()}_{tab_norm.upper()}"
    
    return f"VINMEC_{slug_norm.upper()}"


def determine_category(tab_text: str) -> str:
    """
    Xác định category dựa trên tên tab
    """
    tab_lower = tab_text.lower()
    
    if any(keyword in tab_lower for keyword in ['triệu chứng', 'dấu hiệu', 'biểu hiện']):
        return "Triệu chứng/Dấu hiệu"
    elif any(keyword in tab_lower for keyword in ['biện pháp điều trị', 'điều trị', 'thuốc', 'chữa trị']):
        return "Thuốc"
    else:
        return "Bệnh"


def extract_disease_name(soup: BeautifulSoup) -> str:
    """
    Lấy tên bệnh chính từ title hoặc h1
    """
    # Thử lấy từ h1 trước
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    
    # Fallback sang title
    if soup.title and soup.title.get_text(strip=True):
        full = soup.title.get_text(strip=True)
        return full.split("|")[0].strip()
    
    return ""


def extract_tab_content(soup: BeautifulSoup, tab_url: str) -> str:
    """
    Lấy nội dung của một tab cụ thể
    """
    # Fetch tab page
    try:
        resp = requests.get(tab_url, timeout=25)
        resp.raise_for_status()
        tab_soup = BeautifulSoup(resp.text, "html.parser")
        
        # Thử các selector phổ biến cho nội dung
        candidate_selectors = [
            "div.article-content",
            "div.article-detail",
            "div.rich-text",
            "div.detail-content",
            "div.content-detail",
            "div.container_body",
            "article",
            "main",
        ]
        
        for sel in candidate_selectors:
            el = tab_soup.select_one(sel)
            if el:
                text = el.get_text("\n", strip=True)
                if len(text) > 100:  # Chỉ chấp nhận nếu đủ dài
                    return text
        
        # Fallback: lấy toàn bộ body
        if tab_soup.body:
            return tab_soup.body.get_text("\n", strip=True)
            
    except Exception as e:
        print(f"    [ERROR] Failed to fetch tab content from {tab_url}: {e}")
    
    return ""


def crawl_vinmec_with_tabs(url: str, base_disease_name: str = "") -> list:
    """
    Crawl một trang bệnh Vinmec và tách thành nhiều objects theo tabs.
    Mỗi tab sẽ trở thành một object riêng.
    
    Returns:
        List of dict records
    """
    print(f"  [DETAIL] {url}")
    
    try:
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        print(f"    [ERROR] Failed to fetch main page: {e}")
        return []
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Lấy tên bệnh chính
    disease_name = base_disease_name or extract_disease_name(soup)
    
    records = []
    
    # Tìm danh sách các tabs
    # Dựa vào HTML bạn gửi: <ul class="list_type_detail_sick over_scroll">
    tab_list = soup.find("ul", class_=lambda x: x and ("list_type_detail" in x or "over_scroll" in x))
    
    if not tab_list:
        # Fallback: thử tìm bất kỳ ul nào chứa các link tabs
        tab_list = soup.find("ul", class_="list_type_detail_sick")
    
    if not tab_list:
        print(f"    [WARN] No tabs found, creating single record")
        # Nếu không có tabs, tạo 1 record duy nhất
        content = extract_main_content_simple(soup)
        record = {
            "id": make_id_from_url(url),
            "category": "Bệnh",
            "title": disease_name,
            "content": content,
            "source": "Vinmec"
        }
        return [record]
    
    # Lấy tất cả các tab links
    tab_links = tab_list.find_all("a", href=True)
    
    print(f"    Found {len(tab_links)} tabs")
    
    for tab_link in tab_links:
        tab_text = tab_link.get_text(strip=True)
        tab_href = tab_link.get("href")
        
        # Tạo full URL
        if tab_href.startswith("http"):
            tab_url = tab_href
        else:
            tab_url = urljoin(url, tab_href)
        
        # Lấy tên tab từ href (ví dụ: #tab-11323 -> "Tổng quan")
        # Hoặc từ text của link
        tab_name = tab_text if tab_text else tab_href.split("#")[-1]
        
        # Xác định category
        category = determine_category(tab_text)
        
        # Tạo title: "Tên bệnh - Tab name"
        title = tab_text
        
        # Lấy nội dung tab
        print(f"      Processing tab: {tab_text}")
        
        # Nếu là anchor link (#tab-xxx), nội dung nằm trong cùng trang
        if tab_href.startswith("#"):
            anchor_id = tab_href.lstrip("#")
            block = soup.find("div", id=anchor_id)

            if block:
                # Title trong H2
                h2 = block.find("h2")
                title = h2.get_text(strip=True) if h2 else tab_text

                # Content trong body
                body_div = block.find("div", class_="body")
                if not body_div:
                    body_div = block.find("div", class_=lambda x: x and "collapsible-target" in x)

                content = body_div.get_text("\n", strip=True) if body_div else ""
            else:
                content = ""
        else:
            # Nếu là link khác trang, fetch riêng
            content = extract_tab_content(soup, tab_url)
        
        if not content or len(content) < 50:
            print(f"      [WARN] Tab {tab_text} has no content, skipping")
            continue
        
        # Tạo record
        record = {
            "id": make_id_from_url(url, tab_name),
            "category": category,
            "title": title,
            "content": content,
            "source": "Vinmec"
        }
        
        records.append(record)
        print(f"      ✓ Created record: {record['id']}")
        
        # Sleep ngắn giữa các tabs
        time.sleep(0.5)
    
    return records


def extract_main_content_simple(soup: BeautifulSoup) -> str:
    """
    Lấy nội dung đơn giản khi không có tabs
    """
    candidate_selectors = [
        "div.article-content",
        "div.article-detail",
        "div.rich-text",
        "div.detail-content",
        "div.content-detail",
        "article",
        "main",
    ]
    
    for sel in candidate_selectors:
        el = soup.select_one(sel)
        if el:
            text = el.get_text("\n", strip=True)
            if len(text) > 400:
                return text
    
    # Fallback
    if soup.body:
        return soup.body.get_text("\n", strip=True)
    
    return ""


def crawl_all_details(
    input_list_path: str = INPUT_LIST,
    output_path: str = OUTPUT_JSON,
    sleep_seconds: float = 1.5
):
    diseases = load_disease_list(input_list_path)
    print(f"Loaded {len(diseases)} disease URLs from {input_list_path}")

    all_records = []
    seen_ids = set()

    for idx, item in enumerate(diseases, start=1):
        url = item.get("url")
        if not url:
            continue

        print(f"\n[{idx}/{len(diseases)}] Processing: {url}")
        
        try:
            records = crawl_vinmec_with_tabs(url)
        except Exception as e:
            print(f"[ERROR] {url} -> {e}")
            continue

        # Lọc trùng lặp
        for record in records:
            if record["id"] in seen_ids:
                print(f"  [DUP] Skip duplicate id: {record['id']}")
                continue
            
            seen_ids.add(record["id"])
            all_records.append(record)

        print(f"  ✓ Added {len(records)} records from this disease")
        time.sleep(sleep_seconds)

    # Lưu ra file JSON
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"COMPLETED!")
    print(f"Saved {len(all_records)} total records to {output_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    crawl_all_details()