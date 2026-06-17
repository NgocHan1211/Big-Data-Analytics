import argparse
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError as e:
    raise SystemExit("Chạy: pip install pyspark>=3.5") from e


def load_data(spark, input_dir: str):
    df = spark.read.json(input_dir)
    # thêm cột giờ từ processed_ts
    df = df.withColumn(
        "hour",
        F.hour(F.to_timestamp("processed_ts"))
    )
    return df


def report_by_camera(df) -> None:
    print("\nThống kê theo camera:")
    df.groupBy("camera_id").agg(
        F.count("frame_id").alias("total_frames"),
        F.sum("people_count").alias("total_detections"),
        F.round(F.avg("people_count"), 2).alias("avg_per_frame"),
        F.max("people_count").alias("peak_count"),
    ).orderBy("camera_id").show(truncate=False)


def report_by_hour(df) -> None:
    print("\nKhung giờ đông người nhất:")
    df.groupBy("camera_id", "hour").agg(
        F.round(F.avg("people_count"), 2).alias("avg_people"),
        F.sum("people_count").alias("total_people"),
    ).orderBy("camera_id", F.desc("avg_people")).show(20, truncate=False)


def report_top_frames(df, n: int = 5) -> None:
    print(f"\nTop {n} frame đông người nhất:")
    df.select("camera_id", "frame_id", "people_count", "processed_ts") \
      .orderBy(F.desc("people_count")) \
      .limit(n) \
      .show(truncate=False)


def summarize(input_dir: str, top_n: int) -> None:
    spark = (
        SparkSession.builder
        .appName("CameraAnalytics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # tắt log rác
    try:
        df = load_data(spark, input_dir)
        print(f"✓ Đọc được {df.count()} bản ghi từ {input_dir}")
        report_by_camera(df)
        report_by_hour(df)
        report_top_frames(df, top_n)
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phân tích dữ liệu đếm người bằng PySpark")
    p.add_argument("--input",  default="pipeline_out", help="Thư mục chứa JSONL")
    p.add_argument("--top",    type=int, default=5,    help="Số frame top hiển thị")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summarize(args.input, args.top)
