# 📊 Big Data Labs – Báo Cáo Thực Hành

**Sinh viên:** Trần Ngọc Hân – `23520437`  
**Giảng viên:** Nguyễn Hiếu Nghĩa  
**Môn học:** Big Data Analytics

---

## 📋 Tổng Quan

| Lab | Tên bài | Công nghệ | Dữ liệu |
|-----|---------|-----------|---------|
| 1 | Movie Rating Analysis | Hadoop MapReduce | movies, ratings, users |
| 2 | Text & Sentiment Analysis | Hadoop MapReduce + Spark | Bình luận văn bản |
| 3 | Movie Analytics với RDD | Apache Spark RDD | movies, ratings, users |
| 4 | E-commerce Analytics | Spark DataFrame | Fecom Inc. dataset |
| 5 | Real-time Person Detection | YOLOv8 + TCP Pipeline + PySpark | Video/camera stream |

---

## 🗂️ Lab 1 – Phân Tích Đánh Giá Phim với Hadoop MapReduce

**Công nghệ:** Hadoop MapReduce (Python Streaming)

### Dữ liệu

| File | Schema |
|------|--------|
| `movies.txt` | MovieID, Title, Genres |
| `ratings_1.txt`, `ratings_2.txt` | UserID, MovieID, Rating, Timestamp |
| `users.txt` | UserID, Gender, Age, Occupation, Zip-code |

### Bài tập

**Bài 1 – Tính điểm đánh giá trung bình và tổng số lượt đánh giá**
- Tính điểm trung bình từng phim từ cả 2 file ratings
- Tính tổng số lượt đánh giá cho mỗi phim
- Tìm phim có điểm cao nhất (≥ 5 lượt) trong `cleanup()`
- Output: `MovieTitle AverageRating: xx (TotalRatings: xx)`

**Bài 2 – Phân tích đánh giá theo thể loại**
- Tách `Genres` theo `|`, mỗi thể loại là một key riêng
- Tính điểm trung bình và tổng lượt đánh giá cho từng thể loại
- Output: `Genre: AverageRating (TotalRatings)`

**Bài 3 – Phân tích đánh giá theo giới tính**
- Join `ratings` với `users` theo `UserID`
- Tính điểm trung bình riêng cho nam và nữ theo từng phim
- Output: `MovieTitle: Male_Avg, Female_Avg`

**Bài 4 (Tùy chọn) – Phân tích đánh giá theo nhóm tuổi**
- Phân nhóm: `0–18`, `18–35`, `35–50`, `50+`
- Tính điểm trung bình mỗi nhóm cho từng phim
- Output: `MovieTitle: [0-18: x, 18-35: x, 35-50: x, 50+: x]`

---

## 🗂️ Lab 2 – Phân Tích Văn Bản và Cảm Xúc

**Công nghệ:** Hadoop MapReduce + Apache Spark

**Bài 1 – Tiền xử lý dữ liệu**
- Lowercase toàn bộ văn bản
- Tách bình luận thành các từ theo khoảng trắng
- Loại stop words dựa trên `stopword.txt`

**Bài 2 – Thống kê tần số & phân loại**
- Thống kê tần số các từ — liệt kê từ xuất hiện > 500 lần
- Thống kê số bình luận theo `category`
- Thống kê số bình luận theo `aspect`

**Bài 3 – Xác định aspect tích cực / tiêu cực nhất**
- Aspect nhận nhiều đánh giá `negative` nhất
- Aspect nhận nhiều đánh giá `positive` nhất

**Bài 4 – Top 5 từ tích cực / tiêu cực theo category**
- Với mỗi category: tìm 5 từ tích cực nhất
- Với mỗi category: tìm 5 từ tiêu cực nhất

**Bài 5 – Top 5 từ liên quan nhất theo category**
- Với mỗi phân loại bình luận: xác định 5 từ liên quan nhất

---

## 🗂️ Lab 3 – Movie Analytics với Apache Spark RDD

**Công nghệ:** Apache Spark – RDD API (PySpark)  
> ⚠️ Toàn bộ bài tập thực hiện bằng **RDD**, không dùng DataFrame/SQL

### Dữ liệu
- `movies.txt` — MovieID, Title, Genres
- `ratings_1.txt`, `ratings_2.txt` — UserID, MovieID, Rating, Timestamp
- `users.txt` — UserID, Gender, Age, Occupation, Zip-code
- `occupation.txt` — ID, Occupation

### Bài tập

**Bài 1 – Điểm trung bình và tổng lượt đánh giá**
- Map `MovieID → (Rating, 1)`, reduce tính tổng và đếm
- Lọc phim ≥ 50 lượt, tìm phim có điểm cao nhất

**Bài 2 – Phân tích theo thể loại**
- Map `MovieID → List[Genres]`, join với ratings
- Tính điểm trung bình cho từng thể loại

**Bài 3 – Phân tích theo giới tính**
- Map `UserID → Gender`, join với ratings
- Tính điểm trung bình mỗi phim theo nam/nữ

**Bài 4 – Phân tích theo nhóm tuổi**
- Map `UserID → Age Group` (0-18, 18-35, 35-50, 50+)
- Tính điểm trung bình mỗi phim theo nhóm tuổi

**Bài 5 – Phân tích theo nghề nghiệp**
- Map `UserID → Occupation` từ `users.txt`
- Tính trung bình rating và tổng lượt đánh giá cho từng Occupation

**Bài 6 – Phân tích theo thời gian**
- Chuyển Timestamp Unix → Year
- Tính tổng lượt đánh giá và điểm trung bình cho mỗi năm

---

## 🗂️ Lab 4 – E-commerce Analytics với Spark DataFrame

**Công nghệ:** Apache Spark DataFrame API (PySpark)

### Dữ liệu – Fecom Inc.

> Công ty TMĐT tại Berlin, Đức. Dữ liệu 2022–2024.

| Thống kê | Giá trị |
|----------|---------|
| Đơn hàng | 99.441 |
| Khách hàng | 102.727 |
| Người bán | 3.095 |
| Quốc gia / Thành phố | 28 quốc gia, 338 thành phố |
| Sản phẩm | 32.951 sản phẩm, 72 danh mục |

### Bài tập bắt buộc

**Bài 1** – Đọc các file CSV với `inferSchema=True`  
**Bài 2** – Thống kê tổng số đơn hàng, khách hàng, người bán  
**Bài 3** – Số lượng đơn hàng theo quốc gia (giảm dần)  
**Bài 4** – Đơn hàng nhóm theo năm (tăng dần) và tháng (giảm dần)  
**Bài 5** – Điểm đánh giá trung bình theo mức 1–5, xử lý NULL và ngoại lệ

### Bài tập tùy chọn (chọn 3/5)

- **Bài 6** – Doanh thu 2024 nhóm theo danh mục (giá + phí vận chuyển)
- **Bài 7** – Sản phẩm bán nhiều nhất và điểm đánh giá trung bình
- **Bài 8** – Hiệu suất giao hàng: ngày giao thực tế vs ngày dự kiến
- **Bài 9** – Phân nhóm khách hàng theo số đơn, giá trị trung bình, tần suất
- **Bài 10** – Xếp hạng seller theo tổng doanh thu và số đơn hàng

---

## 🗂️ Lab 5 – Real-time Person Detection Pipeline

**Công nghệ:** YOLOv8 + TCP Streaming Pipeline + PySpark Analytics

### Kiến trúc hệ thống

```
node_capture.py  →[TCP:7001]→  node_detect.py  →[TCP:7002]→  node_writer.py  →  analytics_spark.py
   (Producer)                  (YOLOv8 Inference)             (Consumer/Render)    (PySpark)
```

### Mô tả các thành phần

| File | Vai trò | Chi tiết |
|------|---------|---------|
| `config.py` | Cấu hình chung | SEND_FPS, địa chỉ TCP, MAX_FRAMES |
| `net_utils.py` | TCP abstraction | TCPServer, TCPClient, read_msgs, write_msg |
| `node_capture.py` | Producer | Đọc frame từ video, encode JPEG, gửi qua TCP |
| `node_detect.py` | Inference | YOLOv8n: `conf=0.5`, `iou=0.4`, `classes=[0]` |
| `node_writer.py` | Consumer | Vẽ bbox, lưu output video + JSON log |
| `analytics_spark.py` | Analytics | PySpark thống kê số người theo frame |

### Tại sao dùng TCP thay Kafka?

| | Apache Kafka | TCP Socket |
|--|-------------|------------|
| Yêu cầu | JVM + Zookeeper + Broker | Không cần cài thêm |
| Tài nguyên | ~2GB RAM | Nhẹ, chạy trên Colab |
| Persistence | Có (lưu message) | Không (in-memory) |
| Phù hợp | Production scale | Demo & học thuật |

> Kiến trúc **producer → message passing → consumer** giữ nguyên đúng concept của Kafka Streaming, chỉ thay transport layer.

### Mapping TCP → Kafka concepts

| Kafka | Code |
|-------|------|
| Producer | `TCPClient` trong `node_capture.py` |
| Consumer | `TCPServer` trong `node_detect.py`, `node_writer.py` |
| Topic | Port `7001` (cam→detect), `7002` (detect→writer) |
| Message | JSON payload encode qua socket |
| Broker | `net_utils.py` |

---

*Trần Ngọc Hân – 23520437 | GVHD: Nguyễn Hiếu Nghĩa*
