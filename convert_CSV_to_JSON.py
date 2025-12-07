import csv
import json

input_csv = "D:/ProgramToStudy/VueJs/ViMedAQA/body-part/train-00000-of-00001.csv"
output_json = "D:/ProgramToStudy/VueJs/ViMedAQA/body-part/all_dataset_converted.json"

result = []

with open(input_csv, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        item = {
            "id": row["question_idx"],
            "category": "bệnh",
            "title": row.get("title", "").strip(),
            "content": row.get("context", "").strip(),
            "source": row.get("article_url", "").strip()
        }
        result.append(item)

# Xuất ra file JSON
with open(output_json, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Đã chuyển đổi xong! Tổng số bản ghi: {len(result)}")
