"""
Spark 성능 최적화 스크립트

동일 데이터에 대해 최적화 전/후를 비교하여
실무에서 활용 가능한 Spark 튜닝 기법을 검증합니다.

최적화 항목:
1. CSV vs Parquet 읽기 성능 비교
2. 파티셔닝 전략 비교 (파티션 없음 vs 날짜 파티션 vs 복합 파티션)
3. 캐싱 효과 비교
4. 브로드캐스트 조인 vs 일반 조인
5. 리파티셔닝 전략 비교
6. 종합 성능 리포트 생성

사용법:
  spark-submit scripts/performance_optimizer.py \
    --input data/card_transactions.csv \
    --output output/optimized/
"""

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    date_format,
    sum as spark_sum,
    to_timestamp,
    year,
    month,
)


def create_spark_session(storage="local"):
    """Spark 세션 생성 (로컬/S3 모두 호환)"""
    builder = SparkSession.builder \
        .appName("CardTransactionPerformanceOptimizer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # S3 스토리지 모드: LocalStack 또는 AWS S3 연동
    if storage == "s3":
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:4566")
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        print(f"  [S3 모드] 엔드포인트: {s3_endpoint}")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class PerformanceBenchmark:
    """성능 벤치마크 클래스"""

    def __init__(self):
        self.results = []

    def record(self, test_name, elapsed_seconds, detail=""):
        self.results.append({
            "test": test_name,
            "elapsed_sec": round(elapsed_seconds, 3),
            "detail": detail,
        })

    def print_report(self):
        print("\n" + "=" * 80)
        print("성능 최적화 벤치마크 리포트")
        print("=" * 80)
        print(f"{'테스트':<45} {'소요시간':>12} {'비고'}")
        print("-" * 80)
        for r in self.results:
            print(f"  {r['test']:<43} {r['elapsed_sec']:>8.3f}초   {r['detail']}")
        print("=" * 80)


def benchmark_csv_vs_parquet(spark, csv_path, output_path, bench):
    """1. CSV vs Parquet 읽기 성능 비교"""
    print("\n[1/5] CSV vs Parquet 읽기 성능 비교")

    # CSV 읽기
    start = time.time()
    df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    csv_count = df_csv.count()
    csv_elapsed = time.time() - start
    bench.record("CSV 읽기", csv_elapsed, f"{csv_count:,}건")
    print(f"  CSV 읽기: {csv_elapsed:.3f}초 ({csv_count:,}건)")

    # Parquet 변환 및 저장
    parquet_path = f"{output_path}/benchmark_parquet"
    df_csv = df_csv.withColumn(
        "transaction_ts",
        to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
    )
    df_csv.write.mode("overwrite").parquet(parquet_path)

    # Parquet 읽기
    start = time.time()
    df_parquet = spark.read.parquet(parquet_path)
    parquet_count = df_parquet.count()
    parquet_elapsed = time.time() - start
    bench.record("Parquet 읽기", parquet_elapsed, f"{parquet_count:,}건")
    print(f"  Parquet 읽기: {parquet_elapsed:.3f}초 ({parquet_count:,}건)")

    if csv_elapsed > 0:
        improvement = ((csv_elapsed - parquet_elapsed) / csv_elapsed) * 100
        print(f"  성능 개선: {improvement:.1f}%")
        bench.record("CSV->Parquet 개선률", 0, f"{improvement:.1f}%")

    return df_csv, parquet_path


def benchmark_partitioning(spark, df, output_path, bench):
    """2. 파티셔닝 전략 비교"""
    print("\n[2/5] 파티셔닝 전략 비교")

    df = df.withColumn("txn_year", year(col("transaction_ts"))) \
           .withColumn("txn_month", month(col("transaction_ts"))) \
           .withColumn("txn_date", date_format(col("transaction_ts"), "yyyy-MM-dd"))

    # 파티션 없이 저장
    no_partition_path = f"{output_path}/no_partition"
    start = time.time()
    df.write.mode("overwrite").parquet(no_partition_path)
    write_elapsed = time.time() - start
    bench.record("쓰기: 파티션 없음", write_elapsed)

    start = time.time()
    spark.read.parquet(no_partition_path) \
        .filter(col("txn_date") == "2024-06-15") \
        .count()
    no_part_query = time.time() - start
    bench.record("조회: 파티션 없음 (특정일)", no_part_query)
    print(f"  파티션 없음 - 특정일 조회: {no_part_query:.3f}초")

    # 날짜 파티셔닝
    date_partition_path = f"{output_path}/date_partition"
    start = time.time()
    df.write.mode("overwrite").partitionBy("txn_date").parquet(date_partition_path)
    write_elapsed = time.time() - start
    bench.record("쓰기: 날짜 파티션", write_elapsed)

    start = time.time()
    spark.read.parquet(date_partition_path) \
        .filter(col("txn_date") == "2024-06-15") \
        .count()
    date_part_query = time.time() - start
    bench.record("조회: 날짜 파티션 (특정일)", date_part_query)
    print(f"  날짜 파티션 - 특정일 조회: {date_part_query:.3f}초")

    # 연월 파티셔닝
    ym_partition_path = f"{output_path}/year_month_partition"
    start = time.time()
    df.write.mode("overwrite").partitionBy("txn_year", "txn_month").parquet(ym_partition_path)
    write_elapsed = time.time() - start
    bench.record("쓰기: 연월 파티션", write_elapsed)

    start = time.time()
    spark.read.parquet(ym_partition_path) \
        .filter((col("txn_year") == 2024) & (col("txn_month") == 6)) \
        .count()
    ym_part_query = time.time() - start
    bench.record("조회: 연월 파티션 (특정월)", ym_part_query)
    print(f"  연월 파티션 - 특정월 조회: {ym_part_query:.3f}초")

    if no_part_query > 0:
        improvement = ((no_part_query - date_part_query) / no_part_query) * 100
        print(f"  파티셔닝 성능 개선: {improvement:.1f}%")
        bench.record("파티셔닝 개선률", 0, f"{improvement:.1f}%")

    return df


def benchmark_caching(spark, df, bench):
    """3. 캐싱 효과 비교"""
    print("\n[3/5] 캐싱 효과 비교")

    # 캐싱 없이 동일 쿼리 2회 실행
    start = time.time()
    df.groupBy("category").agg(count("*"), spark_sum("amount")).collect()
    no_cache_1 = time.time() - start
    bench.record("캐싱 없음 - 1회차 집계", no_cache_1)

    start = time.time()
    df.groupBy("category").agg(count("*"), spark_sum("amount")).collect()
    no_cache_2 = time.time() - start
    bench.record("캐싱 없음 - 2회차 집계", no_cache_2)
    print(f"  캐싱 없음: 1회차 {no_cache_1:.3f}초, 2회차 {no_cache_2:.3f}초")

    # 캐싱 후 동일 쿼리 2회 실행
    df.cache()

    start = time.time()
    df.groupBy("category").agg(count("*"), spark_sum("amount")).collect()
    cache_1 = time.time() - start
    bench.record("캐싱 적용 - 1회차 집계 (캐싱 포함)", cache_1)

    start = time.time()
    df.groupBy("category").agg(count("*"), spark_sum("amount")).collect()
    cache_2 = time.time() - start
    bench.record("캐싱 적용 - 2회차 집계", cache_2)
    print(f"  캐싱 적용: 1회차 {cache_1:.3f}초, 2회차 {cache_2:.3f}초")

    if no_cache_2 > 0:
        improvement = ((no_cache_2 - cache_2) / no_cache_2) * 100
        print(f"  캐싱 성능 개선 (2회차 기준): {improvement:.1f}%")
        bench.record("캐싱 개선률", 0, f"{improvement:.1f}%")

    df.unpersist()


def benchmark_broadcast_join(spark, df, bench):
    """4. 브로드캐스트 조인 vs 일반 조인"""
    print("\n[4/5] 브로드캐스트 조인 vs 일반 조인")

    # 카테고리 마스터 테이블 (소규모)
    category_master_data = [
        ("식비", "생활필수", 1),
        ("교통", "생활필수", 2),
        ("쇼핑", "소비지출", 3),
        ("문화", "여가활동", 4),
        ("의료", "생활필수", 5),
        ("교육", "자기계발", 6),
        ("통신", "고정지출", 7),
        ("보험", "고정지출", 8),
    ]
    category_df = spark.createDataFrame(
        category_master_data,
        ["category", "category_group", "priority"]
    )

    # 일반 조인
    start = time.time()
    result_normal = df.join(category_df, on="category", how="left")
    normal_count = result_normal.count()
    normal_elapsed = time.time() - start
    bench.record("일반 조인", normal_elapsed, f"{normal_count:,}건")
    print(f"  일반 조인: {normal_elapsed:.3f}초")

    # 브로드캐스트 조인
    start = time.time()
    result_broadcast = df.join(broadcast(category_df), on="category", how="left")
    broadcast_count = result_broadcast.count()
    broadcast_elapsed = time.time() - start
    bench.record("브로드캐스트 조인", broadcast_elapsed, f"{broadcast_count:,}건")
    print(f"  브로드캐스트 조인: {broadcast_elapsed:.3f}초")

    if normal_elapsed > 0:
        improvement = ((normal_elapsed - broadcast_elapsed) / normal_elapsed) * 100
        print(f"  브로드캐스트 조인 개선률: {improvement:.1f}%")
        bench.record("브로드캐스트 조인 개선률", 0, f"{improvement:.1f}%")


def benchmark_repartition(spark, df, output_path, bench):
    """5. 리파티셔닝 전략 비교"""
    print("\n[5/5] 리파티셔닝 전략 비교")

    current_partitions = df.rdd.getNumPartitions()
    print(f"  현재 파티션 수: {current_partitions}")

    # 다양한 파티션 수로 집계 성능 측정
    partition_tests = [1, 4, 8, 16, 50, 200]

    for num_partitions in partition_tests:
        df_repartitioned = df.repartition(num_partitions)

        start = time.time()
        df_repartitioned.groupBy("category") \
            .agg(count("*"), spark_sum("amount"), avg("amount")) \
            .collect()
        elapsed = time.time() - start

        bench.record(f"리파티션 {num_partitions}개 집계", elapsed)
        print(f"  파티션 {num_partitions:>3}개: {elapsed:.3f}초")


def save_benchmark_report(spark, bench, output_path):
    """벤치마크 결과 저장"""
    rows = [(r["test"], float(r["elapsed_sec"]), r["detail"]) for r in bench.results]
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("test_name", StringType(), True),
        StructField("elapsed_sec", DoubleType(), True),
        StructField("detail", StringType(), True),
    ])
    report_df = spark.createDataFrame(rows, schema)

    report_output = f"{output_path}/benchmark_report"
    report_df.write.mode("overwrite").parquet(report_output)
    print(f"\n벤치마크 리포트 저장: {report_output}")


def main():
    parser = argparse.ArgumentParser(
        description="Spark 성능 최적화 벤치마크"
    )
    parser.add_argument("--input", type=str, required=True, help="입력 데이터 경로")
    parser.add_argument("--output", type=str, required=True, help="출력 경로")
    args = parser.parse_args()

    total_start = time.time()

    print("=" * 80)
    print("Spark 성능 최적화 벤치마크 시작")
    print("=" * 80)

    # 입력 경로 기반 S3 자동 감지
    storage = "s3" if args.input.startswith("s3") else "local"
    spark = create_spark_session(storage=storage)
    bench = PerformanceBenchmark()

    try:
        # 1. CSV vs Parquet
        df, parquet_path = benchmark_csv_vs_parquet(
            spark, args.input, args.output, bench
        )

        # 2. 파티셔닝 전략
        df = benchmark_partitioning(spark, df, args.output, bench)

        # 3. 캐싱 효과
        benchmark_caching(spark, df, bench)

        # 4. 브로드캐스트 조인
        benchmark_broadcast_join(spark, df, bench)

        # 5. 리파티셔닝
        benchmark_repartition(spark, df, args.output, bench)

        # 종합 리포트
        bench.print_report()
        save_benchmark_report(spark, bench, args.output)

        total_elapsed = time.time() - total_start
        print(f"\n전체 벤치마크 소요 시간: {total_elapsed:.2f}초")

    except Exception as e:
        print(f"\n벤치마크 오류: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
