import json

# Danh sách các file cần gộp
input_files = [
    "D:/ProgramToStudy/VueJs/data/raw/diseases/Wikipedia.json",
    "D:/ProgramToStudy/VueJs/ViMedAQA/medicine/medicine_dataset_converted.json",
    "D:/ProgramToStudy/VueJs/ViMedAQA/drug/benh_dataset_converted.json",
    "D:/ProgramToStudy/VueJs/ViMedAQA/disease/benh_dataset_converted.json",
    "D:/ProgramToStudy/VueJs/data/raw/drugs/Wikipedia.json"
]

output_file = "merged.json"

merged = []

# Đọc từng file và nối vào list merged
for file in input_files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)

        # Đảm bảo data là list
        if isinstance(data, list):
            merged.extend(data)
        else:
            print(f"⚠ File {file} không phải dạng list JSON! Bỏ qua.")

# Ghi ra file gộp
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(merged, f, ensure_ascii=False, indent=2)

print("Finish!")
print("Sum object:", len(merged))
