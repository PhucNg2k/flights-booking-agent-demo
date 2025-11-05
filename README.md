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

### Yêu cầu hệ thống
- Python 3.9+
- Java JDK 8+ (Spark yêu cầu)
- Apache Spark (local) 3.5.x khuyến nghị
- Google Chrome (cho Selenium) – WebDriver được quản lý tự động bởi `webdriver_manager`

### Cài đặt
1) Cài dependencies Python:
```bash
pip install -r requirements.txt
```

2) Cài Spark local và cấu hình biến môi trường (Windows):
- Tải Spark: `https://spark.apache.org/downloads.html`
- Ví dụ đặt biến:
  - `SPARK_HOME` = `C:\\Program Files\\Spark\\spark-3.5.3-bin-hadoop3`
  - Thêm `%SPARK_HOME%\bin` vào `PATH`

3) Tạo file `.env` tại thư mục gốc chứa:
```bash
OPENAI_API_KEY=your_openai_api_key
```

4) Khởi chạy các dịch vụ:
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


