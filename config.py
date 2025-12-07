"""
Cấu hình chung cho project
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Đường dẫn gốc
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"

# Tạo thư mục nếu chưa tồn tại
for dir_path in [RAW_DATA_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "diseases").mkdir(exist_ok=True)
    (dir_path / "symptoms").mkdir(exist_ok=True)
    (dir_path / "drugs").mkdir(exist_ok=True)

# Crawler settings
CRAWLER_DELAY = 1.0  # Delay giữa các requests (giây)
CRAWLER_TIMEOUT = 30  # Timeout cho mỗi request
MAX_RETRIES = 3

# User Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Nguồn dữ liệu
DATA_SOURCES = {
    "vinmec": {
        "name": "Vinmec",
        "base_url": "https://www.vinmec.com",
        "reliability": "high",
        "categories": {
            "diseases": "/vi/benh/",
            "symptoms": "/vi/trieu-chung/",
        }
    },
    "wikipedia": {
        "name": "Wikipedia Tiếng Việt",
        "base_url": "https://vi.wikipedia.org",
        "reliability": "medium",
        "categories": {
            "diseases": "/wiki/Thể_loại:Bệnh",
            "drugs": "/wiki/Thể_loại:Thuốc",
        }
    },
    "drugbank_vn": {
        "name": "Drugbank Vietnam",
        "base_url": "https://drugbank.vn",
        "reliability": "high",
        "categories": {
            "drugs": "/thuoc/",
        }
    },
    "hellobacsi": {
        "name": "Hello Bác Sĩ",
        "base_url": "https://hellobacsi.com",
        "reliability": "high",
        "categories": {
            "diseases": "/benh/",
            "symptoms": "/trieu-chung/",
            "drugs": "/thuoc/",
        }
    }
}

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
