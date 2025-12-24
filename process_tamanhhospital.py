import json

print("📖 Đang đọc merged (4).json...")
with open('merged (4).json', 'r', encoding='utf-8') as f:
    data = json.load(f)
print(f"   ✓ Loaded {len(data)} items")

# Đếm và xử lý
tamanh_count = 0
modified_count = 0

print("🔍 Tìm và xử lý items từ tamanhhospital...")
for item in data:
    if item.get('source') == 'tamanhhospital' and 'url' in item:
        tamanh_count += 1
        # Lấy URL vào source, xoá url field
        item['source'] = item['url']
        del item['url']
        modified_count += 1
        if modified_count <= 3:
            print(f"   ✓ Item {modified_count}: {item.get('id')} - source = {item.get('source')[:50]}...")

print(f"   ✓ Tìm thấy {tamanh_count} items từ tamanhhospital")
print(f"   ✓ Đã xử lý {modified_count} items")

# Lưu lại
print("💾 Saving merged (4).json...")
with open('merged (4).json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print("   ✓ Done!")

print(f"\n✅ Xử lý hoàn tất: {modified_count} Tâm Anh Hospital items - URL đã chuyển vào source field")
