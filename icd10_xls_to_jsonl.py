#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Chuyển file ma-icd-10.xlsx (ICD-10 BYT) thành JSONL.

Định dạng mỗi dòng JSON:
{
  "id": "A00.0",
  "group_id": "A00-A09",
  "disease": "Bệnh tả do Vibrio cholerae 01, typ sinh học cổ điển",
  "main_group_name": "Bệnh nhiễm trùng đường ruột",
  "type_name": "Bệnh tả"
}
"""

import json
from pathlib import Path
from typing import Any

import pandas as pd

# ==========================
# CẤU HÌNH FILE
# ==========================
INPUT_FILE = "ma-icd-10.xlsx"   # file anh đã convert
OUTPUT_FILE = "icd10.jsonl"     # file JSONL đầu ra


def safe_str(value: Any) -> str:
    """Convert giá trị sang string, xử lý NaN/None thành chuỗi rỗng."""
    if pd.isna(value):
        return ""
    return str(value).strip()


def main():
    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)

    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {input_path.resolve()}")

    print(f"📥 Đang đọc file Excel: {input_path.name}")
    df_raw = pd.read_excel(input_path)

    print(f"📊 Kích thước gốc: {df_raw.shape[0]} dòng x {df_raw.shape[1]} cột")

    # ==========================
    # 1) Đặt lại header = dòng số 2 (index = 1)
    #    và bỏ 2 dòng đầu (metadata, tên cột)
    # ==========================
    df = df_raw.copy()
    df.columns = df.iloc[1]        # dòng index=1 chứa tên cột như: MÃ BỆNH, TÊN BỆNH, ...
    df = df.iloc[2:].reset_index(drop=True)  # bỏ 2 dòng đầu

    print("📌 Các cột sau khi chuẩn hóa header:")
    print(list(df.columns))

    # ==========================
    # 2) Map cột theo đúng cấu trúc file thật
    # ==========================
    COL_ID = "MÃ BỆNH"
    COL_GROUP_ID = "MÃ NHÓM CHÍNH"
    COL_DISEASE = "TÊN BỆNH"
    COL_MAIN_GROUP_NAME = "TÊN NHÓM CHÍNH"
    COL_TYPE_NAME = "TÊN LOẠI"

    required_cols = [COL_ID, COL_DISEASE]
    for c in required_cols:
        if c not in df.columns:
            raise RuntimeError(
                f"❌ Không tìm thấy cột bắt buộc: '{c}' trong file Excel.\n"
                f"Header hiện tại: {list(df.columns)}"
            )

    print("\n🔎 Đang sử dụng mapping cột:")
    print(f"  id              ← {COL_ID}")
    print(f"  group_id        ← {COL_GROUP_ID}")
    print(f"  disease         ← {COL_DISEASE}")
    print(f"  main_group_name ← {COL_MAIN_GROUP_NAME}")
    print(f"  type_name       ← {COL_TYPE_NAME}")

    # ==========================
    # 3) Lọc bỏ các dòng không có mã bệnh (MÃ BỆNH trống)
    # ==========================
    before = len(df)
    df = df[~df[COL_ID].isna()]   # giữ những dòng có MÃ BỆNH
    after = len(df)
    print(f"\n🧹 Đã loại bỏ {before - after} dòng không có mã bệnh. Còn lại: {after} dòng.")

    # ==========================
    # 4) Ghi JSONL
    # ==========================
    print(f"\n💾 Đang ghi JSONL ra: {output_path.name}")
    count_written = 0

    with output_path.open("w", encoding="utf-8") as f_out:
        for _, row in df.iterrows():
            icd_id = safe_str(row[COL_ID])
            if not icd_id:
                continue

            obj = {
                "id": icd_id,
                "group_id": safe_str(row[COL_GROUP_ID]) if COL_GROUP_ID in df.columns else "",
                "disease": safe_str(row[COL_DISEASE]),
                "main_group_name": safe_str(row[COL_MAIN_GROUP_NAME]) if COL_MAIN_GROUP_NAME in df.columns else "",
                "type_name": safe_str(row[COL_TYPE_NAME]) if COL_TYPE_NAME in df.columns else "",
            }

            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count_written += 1

    print(f"✅ Hoàn thành! Đã ghi {count_written} dòng JSONL vào {output_path.resolve()}")


if __name__ == "__main__":
    main()
