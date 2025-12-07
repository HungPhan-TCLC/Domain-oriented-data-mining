import pandas as pd
import json
import re
import sys
from pathlib import Path

# Thiết lập encoding cho console Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

class MedicalParquetConverter:
    """Chuyển đổi file Parquet chứa dữ liệu y tế sang JSON"""
    
    def __init__(self):
        self.disease_counter = 0
        self.drug_counter = 0
        self.symptom_counter = 0
    
    def detect_category(self, row):
        """Phát hiện loại dữ liệu dựa vào nội dung của row"""
        # Kết hợp tất cả text trong row để phân tích
        text_content = ' '.join([str(val) for val in row.values if pd.notna(val)]).lower()
        
        # Keywords cho từng category
        disease_keywords = ['bệnh', 'hội chứng', 'rối loạn', 'viêm', 'ung thư', 
                            'nhiễm', 'suy', 'loạn', 'thoái hóa', 'u', 'nhồi máu']
        drug_keywords = ['thuốc', 'panadol', 'aspirin', 'vitamin', 'viên nén', 'liều dùng',
                        'mg', 'ml', 'tác dụng', 'chống', 'sử dụng', 'nên', 'kháng', 'chỉ định', 'điều trị', 'thành phần']
        symptom_keywords = ['triệu chứng', 'dấu hiệu', 'biểu hiện', 'đau', 'sốt', 'ho', 'khó thở', 'mệt mỏi', 'buồn nôn', 
                            'chóng mặt', 'ngứa', 'sưng', 'đỏ', 'viêm', 'chảy máu']
        
        # Đếm số lần xuất hiện của keywords
        disease_count = sum(1 for kw in disease_keywords if kw in text_content)
        drug_count = sum(1 for kw in drug_keywords if kw in text_content)
        symptom_count = sum(1 for kw in symptom_keywords if kw in text_content)
        
        # Quyết định category dựa trên count
        if disease_count > drug_count and disease_count > symptom_count:
            return 'bệnh'
        elif drug_count > symptom_count and drug_count > disease_count:
            return 'thuốc'
        elif symptom_count > disease_count and symptom_count > drug_count:
            return 'Triệu chứng/Dấu hiệu'
        else:
            # Mặc định nếu không xác định được
            return 'bệnh'
    
    def generate_id(self, category):
        """Tạo ID duy nhất theo category"""
        if category == 'Disease':
            self.disease_counter += 1
            return f"D_{self.disease_counter:03d}"
        elif category == 'Drug':
            self.drug_counter += 1
            return f"DR_{self.drug_counter:03d}"
        else:
            self.symptom_counter += 1
            return f"S_{self.symptom_counter:03d}"
    
    def extract_title(self, row):
        """Trích xuất title từ row"""
        # Thử tìm các cột có thể chứa title
        title_candidates = ['title', 'name', 'ten', 'tên', 'heading', 'subject', 'disease_name', 'drug_name']
        
        for col in row.index:
            if any(candidate in col.lower() for candidate in title_candidates):
                if pd.notna(row[col]):
                    return str(row[col]).strip()
        
        # Nếu không tìm thấy, lấy giá trị đầu tiên không null
        for val in row.values:
            if pd.notna(val) and len(str(val).strip()) > 0:
                title = str(val).strip()
                # Giới hạn độ dài title
                if len(title) > 100:
                    title = title[:100] + "..."
                return title
        
        return "Untitled"
    
    def extract_content(self, row):
        """Gộp toàn bộ nội dung từ row thành một string, chỉ giữ phần context/nội dung chính"""
        # Ưu tiên tìm cột context trước
        context_candidates = ['context', 'content', 'noi dung', 'nội dung', 'description', 'mo ta', 'mô tả']
        
        for col in row.index:
            col_lower = col.lower()
            if any(candidate in col_lower for candidate in context_candidates):
                if pd.notna(row[col]):
                    content = str(row[col]).strip()
                    if content:
                        return content
        
        # Nếu không tìm thấy context, gộp các trường quan trọng
        important_fields = ['answer', 'question', 'description']
        content_parts = []
        
        for col, val in row.items():
            if pd.notna(val):
                col_lower = col.lower()
                val_str = str(val).strip()
                
                if not val_str:
                    continue
                
                # Chỉ lấy các trường quan trọng
                if any(field in col_lower for field in important_fields):
                    content_parts.append(val_str)
        
        if content_parts:
            return ' '.join(content_parts)
        
        # Fallback: lấy giá trị text đầu tiên có độ dài > 50 ký tự
        for val in row.values:
            if pd.notna(val):
                val_str = str(val).strip()
                if len(val_str) > 50 and not val_str.startswith('http'):
                    return val_str
        
        return "No content available"
    
    def extract_source(self, row):
        """Trích xuất nguồn từ row"""
        # Ưu tiên 1: Tìm các cột có tên liên quan đến source
        source_candidates = ['source', 'nguon', 'nguồn', 'origin', 'reference', 'ref']
        
        for col in row.index:
            col_lower = col.lower()
            if any(candidate in col_lower for candidate in source_candidates):
                if pd.notna(row[col]):
                    source_val = str(row[col]).strip()
                    if source_val and source_val.lower() not in ['unknown', 'none', 'nan']:
                        return source_val
        
        # Ưu tiên 2: Trích xuất từ URL (article_url, author_url)
        url_candidates = ['article_url', 'url', 'link', 'website']
        for col in row.index:
            col_lower = col.lower()
            if any(candidate in col_lower for candidate in url_candidates):
                if pd.notna(row[col]):
                    url = str(row[col]).strip()
                    if url and url.startswith('http'):
                        # Trích xuất tên domain từ URL
                        match = re.search(r'https?://(?:www\.)?([^/]+)', url)
                        if match:
                            domain = match.group(1)
                            # Làm đẹp tên domain
                            domain = domain.replace('.vn', '').replace('.com', '')
                            # Viết hoa chữ cái đầu
                            return domain.capitalize()
        
        # Ưu tiên 3: Tìm trong author
        if 'author' in row.index and pd.notna(row['author']):
            author = str(row['author']).strip()
            if author and not author.startswith('http'):
                return f"Tác giả: {author}"
        
        # Ưu tiên 4: Tìm pattern trong content
        for val in row.values:
            if pd.notna(val):
                content = str(val)
                source_patterns = [
                    r'Nguồn:\s*(.+?)(?:\n|$)',
                    r'Source:\s*(.+?)(?:\n|$)',
                    r'Theo\s+(.+?)(?:\n|$)',
                ]
                
                for pattern in source_patterns:
                    match = re.search(pattern, content, re.IGNORECASE)
                    if match:
                        source = match.group(1).strip()
                        # Loại bỏ URL nếu có
                        source = re.sub(r'https?://\S+', '', source).strip()
                        if source:
                            return source
        
        return "Unknown"
    
    def convert_row_to_medical_item(self, row, index):
        """Chuyển đổi một row thành medical item"""
        # Phát hiện category
        category = self.detect_category(row)
        
        # Tạo ID
        item_id = self.generate_id(category)
        
        # Trích xuất các thành phần
        title = self.extract_title(row)
        content = self.extract_content(row)
        source = self.extract_source(row)
        
        return {
            "id": item_id,
            "category": category,
            "title": title,
            "content": content,
            "source": source
        }
    
    def read_parquet_file(self, file_path):
        """Đọc file Parquet"""
        self.safe_print(f"Dang doc file Parquet: {file_path}")
        df = pd.read_parquet(file_path)
        self.safe_print(f"Da doc {len(df)} dong, {len(df.columns)} cot")
        self.safe_print(f"Cac cot: {', '.join(df.columns.tolist())}")
        return df
    
    def safe_print(self, text):
        """In text an toàn, xử lý encoding"""
        try:
            print(text)
        except UnicodeEncodeError:
            # Nếu lỗi encoding, chuyển sang ASCII
            print(text.encode('ascii', 'ignore').decode('ascii'))
    
    def convert_to_json(self, parquet_file_path, json_output_path):
        """Chuyển đổi file Parquet sang JSON"""
        # Đọc file Parquet
        df = self.read_parquet_file(parquet_file_path)
        
        self.safe_print("\nDang chuyen doi du lieu...")
        
        # Chuyển đổi từng row thành medical item
        medical_items = []
        for idx, row in df.iterrows():
            try:
                item = self.convert_row_to_medical_item(row, idx)
                medical_items.append(item)
            except Exception as e:
                self.safe_print(f"Loi tai dong {idx}: {str(e)}")
                continue
        
        self.safe_print(f"Da chuyen doi {len(medical_items)} muc du lieu")
        
        # Xuất ra JSON
        with open(json_output_path, 'w', encoding='utf-8') as json_file:
            json.dump(medical_items, json_file, ensure_ascii=False, indent=2)
        
        self.safe_print(f"Da xuat ra file JSON: {json_output_path}")
        
        # Thống kê
        stats = {
            "Disease": sum(1 for item in medical_items if item['category'] == 'bệnh'),
            "Drug": sum(1 for item in medical_items if item['category'] == 'thuốc'),
            "Symptom": sum(1 for item in medical_items if item['category'] == '')
        }
        
        self.safe_print("\nThong ke:")
        for category, count in stats.items():
            self.safe_print(f"  - {category}: {count} muc")
        
        return medical_items
    
    def preview_data(self, parquet_file_path, n_rows=5):
        """Xem trước dữ liệu Parquet"""
        df = pd.read_parquet(parquet_file_path)
        self.safe_print(f"\n=== Xem truoc {n_rows} dong dau tien ===")
        self.safe_print(str(df.head(n_rows)))
        self.safe_print(f"\n=== Thong tin cac cot ===")
        df.info()
        return df


# Cách sử dụng
if __name__ == "__main__":
    # Khởi tạo converter
    converter = MedicalParquetConverter()
    
    # Đường dẫn file
    parquet_file = "D:/ProgramToStudy/VueJs/ViMedAQA/all/train-00000-of-00001.parquet"  # Thay bằng đường dẫn file của bạn
    json_file = "D:/ProgramToStudy/VueJs/ViMedAQA/all/train_data.json"
    
    # Xem trước dữ liệu (optional)
    try:
        converter.safe_print("=== XEM TRUOC DU LIEU ===")
        df = converter.preview_data(parquet_file, n_rows=3)
    except FileNotFoundError:
        converter.safe_print(f"X Khong tim thay file: {parquet_file}")
        exit(1)
    except Exception as e:
        converter.safe_print(f"X Loi khi xem truoc: {str(e)}")
    
    converter.safe_print("\n" + "="*50)
    
    # Chuyển đổi
    try:
        medical_data = converter.convert_to_json(parquet_file, json_file)
        converter.safe_print("\nV Chuyen doi thanh cong!")
        
        # Hiển thị 2 mục đầu tiên làm ví dụ
        if len(medical_data) > 0:
            converter.safe_print("\n=== Vi du du lieu da chuyen doi ===")
            converter.safe_print(json.dumps(medical_data[0], ensure_ascii=False, indent=2))
            
    except FileNotFoundError:
        converter.safe_print(f"X Khong tim thay file: {parquet_file}")
    except Exception as e:
        converter.safe_print(f"X Loi: {str(e)}")