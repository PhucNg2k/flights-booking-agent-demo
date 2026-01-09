### Flights Booking Agent

Trợ lý hỗ trợ tìm kiếm lịch bay theo thời gian/khu vực, tư vấn quy định hàng không và đặt vé (ghi nhận form) qua giao diện Gradio. Dữ liệu chuyến bay được crawl từ `https://vietnamairport.vn/thong-tin-lich-bay`, đẩy vào Kafka, xử lý bởi Spark Streaming và lưu vào MongoDB. Nội dung quy định được crawl từ Vietnam Airlines, chunk và lập chỉ mục vào Qdrant (vector) và Elasticsearch (từ khóa) để RAG.

### Kiến trúc tổng quan
- **Crawler**: `crawlers_producer/crawl_flights.py` (Selenium) gửi bản ghi JSON vào Kafka topic `flights`.
- **Streaming ETL**: `ingest_data_consumer/consumer-mongo.py` (Spark) đọc Kafka → chuẩn hóa → ghi `flight_db.flight_info` (MongoDB). Map IATA từ `iata_code/*.json`.
- **RAG Ingest**: `ingest_data_consumer/ingest_rag.py` đọc txt trong `crawlers_producer/crawled_data_vn_airline`, tạo embedding OpenAI, index vào Qdrant + Elasticsearch.
- **Chat Agent**: `agent/Chatbot.py` dùng LlamaIndex Workflows + OpenAI để:
  - Phân loại ý định (tìm kiếm/đặt vé/quy định).
  - Chuẩn hóa truy vấn, truy vấn MongoDB, tạo biên nhận đặt vé (ghi vào `booking_info`).
  - Truy vấn quy định bằng RAG (Qdrant + Elasticsearch).

### Thành phần dịch vụ (docker-compose)
Khởi chạy các dịch vụ phụ trợ:
- Kafka + Zookeeper
- MongoDB (27017)
- Elasticsearch (9200)
- Qdrant (6333)

### Cài đặt ChromeDriver và Chrome for Testing (cho Crawler)
Crawler sử dụng Chrome for Testing và ChromeDriver để crawl dữ liệu. Cài đặt như sau:


```

**Lưu ý:** Đảm bảo version ChromeDriver và Chrome for Testing khớp với version nhau. Cập nhật đường dẫn trong `crawlers_producer/crawl_flights.py` nếu cần.

### Yêu cầu hệ thống
- Python 3.10
- Java JDK 8+ (Spark yêu cầu)
- Apache Spark (local) 3.5.x khuyến nghị
- Google Chrome (cho Selenium) – WebDriver được quản lý tự động bởi `webdriver_manager`

### Cài đặt
1) Cài dependencies Python:
```bash
pip install -r requirements.txt
```

2) Cài Spark local và cấu hình biến môi trường:

**macOS (Khuyến nghị - Cài local):**
- Cài đặt Java JDK 8+ (nếu chưa có):
  ```bash
  # Kiểm tra Java đã cài chưa
  java -version
  
  # Nếu chưa có, cài qua Homebrew:
  brew install openjdk@11
  ```
- Tải và giải nén Spark:
  ```bash
  # Tải Spark 3.5.3 (hoặc phiên bản mới nhất)
  cd ~/Downloads
  curl -O https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
  tar -xzf spark-3.5.3-bin-hadoop3.tgz
  
  # Di chuyển vào thư mục home hoặc thư mục khác
  mv spark-3.5.3-bin-hadoop3 ~/spark-3.5.3
  ```
- Cấu hình biến môi trường trong `~/.zshrc` (hoặc `~/.bash_profile` nếu dùng bash):
  ```bash
  # Thêm vào cuối file
  export SPARK_HOME=$HOME/spark-3.5.3
  export PATH=$SPARK_HOME/bin:$PATH
  export JAVA_HOME=$(brew --prefix openjdk@11)  # Hoặc phiên bản Java bạn đã cài
  
  # Reload shell config
  source ~/.zshrc
  ```
- Kiểm tra cài đặt:
  ```bash
  spark-submit --version
  ```

**Windows:**
- Tải Spark: `https://spark.apache.org/downloads.html`
- Ví dụ đặt biến:
  - `SPARK_HOME` = `C:\\Program Files\\Spark\\spark-3.5.3-bin-hadoop3`
  - Thêm `%SPARK_HOME%\bin` vào `PATH`

1) Tạo file `.env` tại thư mục gốc chứa:
```bash
OPENAI_API_KEY=your_openai_api_key
```

1) Khởi chạy các dịch vụ:
```bash
docker compose up -d
```

### Chuẩn bị dữ liệu
1) Tạo Kafka topic `flights`:
```bash
docker exec -it kafka sh -c "\
  /usr/bin/kafka-topics --create \
  --topic flights \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1"
```

2) Chạy Spark consumer ghi MongoDB:
```bash
python ./ingest_data_consumer/consumer-mongo.py
```

3) Chạy crawler lịch bay (Selenium → Kafka):
```bash
python ./crawlers_producer/crawl_flights.py
```

4) Ingest nội dung quy định vào Qdrant + Elasticsearch (RAG):
Thư mục dữ liệu mẫu: `crawlers_producer/crawled_data_vn_airline/*.txt` (có sẵn một số file). Có thể dùng `crawlers_producer/crawl_vn_airline.py` để thu thập thêm.
```bash
python ./ingest_data_consumer/ingest_rag.py
```

### Chạy Chatbot (Gradio UI)
```bash
python ./agent/Chatbot.py
```
Mở đường dẫn Gradio hiển thị trên console, nhập câu hỏi bằng tiếng Việt.

### Luồng tương tác của Agent
- Phân loại ý định: QUERY | BOOKING | UNKNOWN (`agent/prompts.py`).
- QUERY: Chuẩn hóa thời gian/khu vực/hãng → truy vấn MongoDB (`flight_db.flight_info`) qua workflow → trả kết quả.
- BOOKING: Kiểm tra flight_id tồn tại ngày đặt → ghi `flight_db.booking_info` → trả biên nhận.
- REGULATION: Gửi truy vấn tới RAG (Qdrant + Elasticsearch) → trộn điểm semantic/keyword → trả đoạn liên quan.

### Cấu hình và hằng số
- `constant.py`
  - `EMBEDDING_MODEL = 'text-embedding-3-small'`
  - `ELASTICSEARCH_HOST = 'http://localhost:9200'`
  - `QDRANT_HOST = 'http://localhost:6333'`
  - `MONGODB_HOST = 'mongodb://localhost:27017'`

### Ghi chú thêm
- Nguồn dữ liệu lịch bay: `https://vietnamairport.vn/thong-tin-lich-bay`
- Map IATA: `iata_code/airline.json`, `iata_code/airport.json`, `iata_code/region_name.json` (được load trong Spark consumer).
- MySQL (tùy chọn): Có script mẫu `ingest_data_consumer/consumer-mysql.py` và `ingest_data_consumer/init_db_mysql.py` nếu muốn ghi MySQL thay vì MongoDB.

### Xem dữ liệu MongoDB
Database: `flight_db` chứa 2 collections:

**1. `flight_info`** - Dữ liệu chuyến bay được ingest từ Spark consumer:
- Các trường: `flight_id`, `date`, `scheduled_time`, `updated_time`, `departure_time`, `departure_airport`, `arrival_airport`, `departure_region_name`, `arrival_region_name`, `airline`, `counter`, `gate`, `status`, `is_delayed`, `processing_timestamp`

**2. `booking_info`** - Dữ liệu đặt vé từ chatbot:
- Các trường: `user_name`, `user_phone`, `user_email`, `date_book`, `flight_id`, `num_tickets`

**Lệnh xem dữ liệu:**
```bash
# Kết nối MongoDB shell trong Docker
docker exec -it mongodb mongosh

# Hoặc từ máy local (nếu đã cài mongosh)
mongosh mongodb://localhost:27017

# Trong MongoDB shell:
use flight_db

# Xem số lượng documents
db.flight_info.countDocuments()
db.booking_info.countDocuments()

# Xem 5 bản ghi đầu tiên
db.flight_info.find().limit(5).pretty()
db.booking_info.find().limit(5).pretty()

# Tìm chuyến bay theo flight_id
db.flight_info.find({flight_id: "VN123"}).pretty()

# Tìm đặt vé theo email
db.booking_info.find({user_email: "user@example.com"}).pretty()
```

### Khắc phục sự cố
- Cổng bận: đảm bảo 9092 (Kafka), 27017 (MongoDB), 9200 (Elasticsearch), 6333 (Qdrant) rảnh hoặc sửa `docker-compose.yml` tương ứng.
- Spark không tìm thấy Kafka/Mongo connector: đảm bảo `consumer-mongo.py` đã cấu hình `spark.jars.packages` như trong mã và đã cài đặt Java/Spark đúng phiên bản.
- Selenium lỗi khởi tạo Chrome: cập nhật Chrome, hoặc chạy lại để `webdriver_manager` tải đúng driver; có thể cần quyền admin trên Windows.

### Lệnh nhanh
```bash
# Khởi động dịch vụ
docker compose up -d

# Tạo topic flights
docker exec -it kafka sh -c "/usr/bin/kafka-topics --create --topic flights --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"

# Chạy consumer Spark → MongoDB
python ./ingest_data_consumer/consumer-mongo.py

# Chạy crawler đẩy dữ liệu vào Kafka
python ./crawlers_producer/crawl_flights.py

# Ingest RAG (Qdrant + Elasticsearch)
python ./ingest_data_consumer/ingest_rag.py

# Chạy Chatbot
python ./agent/Chatbot.py
```


