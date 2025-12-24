#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Crawl / parse MeSH XML -> mesh_terms.json
+ Dịch sang tiếng Việt bằng Google Gemini (batch)
"""

import xml.etree.ElementTree as ET
import json
import os
import time
import requests
from pathlib import Path
from typing import List, Dict, Optional


# ==============================
# CONFIG
# ==============================
INPUT_XML = "desc2025.xml"        # File MeSH XML
OUTPUT_JSON = "mesh_terms.json"   # Output JSON

# Anh có thể để thẳng key như dưới, hoặc dùng os.getenv("GEMINI_API_KEY")
GEMINI_API_KEY = "AIzaSyCGfJ-LkAFF1PeSraHJEd6m-I5Df0cK4xU"

if not GEMINI_API_KEY:
    raise RuntimeError("❌ Chưa set GEMINI_API_KEY")


# ==============================
# GEMINI BATCH TRANSLATE
# ==============================
def gemini_batch_translate(text_list: List[str]) -> List[str]:
    """
    Dịch batch nhiều câu bằng 1 request Gemini.
    Trả về list kết quả tương ứng từng item.
    """

    # Nếu toàn bộ là rỗng -> trả list rỗng
    if not text_list:
        return []

    # Nếu trong batch có text rỗng, xử lý cho đẹp
    # (khỏi gửi text rỗng lên API)
    non_empty_indices = [i for i, t in enumerate(text_list) if t and t.strip()]
    if not non_empty_indices:
        return ["" for _ in text_list]

    model = "gemini-2.0-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    headers = {
        "x-goog-api-key": GEMINI_API_KEY,
        "Content-Type": "application/json",
    }

    # Ghép các item không rỗng với tag [ITEM_i]
    joined_text = "\n".join(
        f"[ITEM_{i}] {text_list[i]}" for i in non_empty_indices
    )

    prompt = (
        "You are a professional medical translator. "
        "Translate ALL of the following English medical texts into natural, accurate Vietnamese.\n"
        "For each line starting with [ITEM_i], output EXACTLY one line in this format:\n"
        "[ITEM_i] <Vietnamese translation>\n"
        "Do not add extra explanations or blank lines.\n\n"
        f"{joined_text}"
    )

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ]
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=60)

        if not res.ok:
            print("⚠ Gemini batch error:", res.status_code, res.text[:200])
            res.raise_for_status()

        data = res.json()
        out_text = data["candidates"][0]["content"]["parts"][0]["text"]

        # Khởi tạo kết quả với bản gốc (fallback)
        translations = [t for t in text_list]

        # Parse từng dòng [ITEM_i] ...
        for line in out_text.split("\n"):
            line = line.strip()
            if not line.startswith("[ITEM_"):
                continue
            # dạng [ITEM_12] dịch bla bla
            try:
                prefix, translated = line.split("]", 1)
                idx_str = prefix.replace("[ITEM_", "")
                idx = int(idx_str)
                translated = translated.strip()
                translations[idx] = translated
            except Exception:
                continue

        return translations

    except Exception as e:
        print("⚠ Batch translate exception:", e)
        # fallback: trả lại bản tiếng Anh
        return text_list


# ==============================
# XML HELPERS
# ==============================
def get_text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t or None


def extract_scope_note(rec: ET.Element) -> str:
    """Lấy ScopeNote của Concept Preferred"""
    concept_list = rec.find("ConceptList")
    if concept_list is None:
        return ""

    for concept in concept_list.findall("Concept"):
        if concept.get("PreferredConceptYN") == "Y":
            scope = concept.find("ScopeNote")
            txt = get_text(scope)
            if txt:
                return txt
    return ""


def extract_tree_numbers(rec: ET.Element) -> List[str]:
    tns = []
    for tn in rec.findall("TreeNumberList/TreeNumber"):
        txt = get_text(tn)
        if txt:
            tns.append(txt)
    return tns


def guess_category(tree_numbers: List[str]) -> str:
    """Heuristic: phân loại MeSH theo treeNumber"""
    if not tree_numbers:
        return "Symptoms"
    first = tree_numbers[0]
    if first.startswith("C"):
        return "Diseases"
    if first.startswith("D"):
        return "Drugs"
    return "Symptoms"


# ==============================
# PARSER (CHỈ LẤY ENGLISH)
# ==============================
def parse_mesh(xml_path: str) -> List[Dict]:
    path = Path(xml_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file XML: {path}")

    print(f"📥 Đang đọc file MeSH: {path}")
    tree = ET.parse(str(path))
    root = tree.getroot()

    items = []
    count = 0

    for rec in root.findall("DescriptorRecord"):

        mesh_id = get_text(rec.find("DescriptorUI"))
        name_el = rec.find("DescriptorName/String")
        term_en = get_text(name_el)

        if not mesh_id or not term_en:
            continue

        definition_en = extract_scope_note(rec)
        tree_numbers = extract_tree_numbers(rec)
        category = guess_category(tree_numbers)

        items.append({
            "mesh_id": mesh_id,
            "term_en": term_en,
            "definition_en": definition_en,
            "category": category,
            "source": "MeSH"
        })

        count += 1

        if count % 2000 == 0:
            print(f"🔎 Đã parse {count} record...")

    print(f"📊 Tổng số record parse được: {count}")
    return items


# ==============================
# MAIN
# ==============================
def main():
    mesh_items = parse_mesh(INPUT_XML)

    print("🧠 Bắt đầu dịch batch bằng Gemini...")

    # Chuẩn bị list tiếng Anh
    term_en_list = [item["term_en"] for item in mesh_items]
    def_en_list = [item.get("definition_en", "") or "" for item in mesh_items]

    BATCH_SIZE = 50

    term_vi_all: List[str] = []
    def_vi_all: List[str] = []

    # Dịch term_en
    print("🔄 Đang dịch tên thuật ngữ (term_en)...")
    for i in range(0, len(term_en_list), BATCH_SIZE):
        batch = term_en_list[i:i + BATCH_SIZE]
        translated = gemini_batch_translate(batch)
        term_vi_all.extend(translated)
        print(f"   -> Đã dịch {len(term_vi_all)}/{len(term_en_list)} term")
        time.sleep(0.1)  # nhẹ nhàng tránh bị rate limit

    # Dịch definition_en
    print("🔄 Đang dịch định nghĩa (definition_en)...")
    for i in range(0, len(def_en_list), BATCH_SIZE):
        batch = def_en_list[i:i + BATCH_SIZE]
        translated = gemini_batch_translate(batch)
        def_vi_all.extend(translated)
        print(f"   -> Đã dịch {len(def_vi_all)}/{len(def_en_list)} definition")
        time.sleep(0.1)

    # Gán lại vào mesh_items
    for idx, item in enumerate(mesh_items):
        item["term_vi"] = term_vi_all[idx]
        item["definition_vi"] = def_vi_all[idx]

    out_path = Path(OUTPUT_JSON)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(mesh_items, f, ensure_ascii=False, indent=2)

    print(f"✅ Đã lưu {len(mesh_items)} record vào: {out_path}")


if __name__ == "__main__":
    main()
