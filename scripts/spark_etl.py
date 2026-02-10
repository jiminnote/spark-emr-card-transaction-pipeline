"""
카드 거래 데이터 Spark ETL 파이프라인

메인 배치 처리 스크립트:
1. CSV 데이터 로드 및 스키마 적용
2. 데이터 정제 (타입 변환, 필터링)
3. 일별/카테고리별 집계
4. 가맹점별 매출 분석
5. 시간대별 거래 패턴 분석
6. Parquet 포맷으로 결과 저장

사용법:
  로컬: spark-submit scripts/spark_etl.py --input data/card_transactions.csv --output output/
  EMR:  spark-submit --deploy-mode cluster --master yarn scripts/spark_etl.py \
        --input s3://bucket/input/card_transactions.csv --output s3://bucket/output/
"""

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    date_format,
    dayofweek,
    desc,
    hour,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_spark_session(app_name="CardTransactionETL", storage="local"):
    """Spark 세션 생성 (로컬/S3/EMR 모두 호환)"""
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

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


def define_schema():
    """카드 거래 데이터 스키마 정의"""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", StringType(), False),
        StructField("card_no", StringType(), False),
        StructField("card_type", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("installment_months", IntegerType(), True),
        StructField("approval_status", StringType(), True),
        StructField("region", StringType(), True),
    ])


def load_data(spark, input_path):
    """데이터 로드 및 기본 변환"""
    print(f"\n[1/6] 데이터 로드: {input_path}")
    start_time = time.time()

    schema = define_schema()

    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(schema) \
        .csv(input_path)

    # 타임스탬프 변환
    df = df.withColumn(
        "transaction_ts",
        to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
    )

    # 날짜 파생 컬럼 추가
    df = df.withColumn("txn_year", year(col("transaction_ts"))) \
           .withColumn("txn_month", month(col("transaction_ts"))) \
           .withColumn("txn_date", date_format(col("transaction_ts"), "yyyy-MM-dd")) \
           .withColumn("txn_hour", hour(col("transaction_ts"))) \
           .withColumn("txn_day_of_week", dayofweek(col("transaction_ts")))

    # 승인 건만 필터 (분석 대상)
    df_approved = df.filter(col("approval_status") == "approved")

    record_count = df.count()
    approved_count = df_approved.count()
    elapsed = time.time() - start_time

    print(f"  전체 레코드: {record_count:,}")
    print(f"  승인 건수: {approved_count:,}")
    print(f"  소요 시간: {elapsed:.2f}초")

    return df, df_approved


def daily_category_summary(df, output_path):
    """일별 카테고리별 거래 집계"""
    print("\n[2/6] 일별 카테고리별 집계 처리 중...")
    start_time = time.time()

    daily_summary = df.groupBy("txn_date", "category") \
        .agg(
            count("transaction_id").alias("txn_count"),
            countDistinct("card_no").alias("unique_cards"),
            spark_sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 0).alias("avg_amount"),
            spark_max("amount").alias("max_amount"),
            spark_min("amount").alias("min_amount"),
        ) \
        .orderBy("txn_date", "category")

    output = f"{output_path}/daily_category_summary"
    daily_summary.write \
        .mode("overwrite") \
        .partitionBy("txn_date") \
        .parquet(output)

    row_count = daily_summary.count()
    elapsed = time.time() - start_time

    print(f"  결과 행수: {row_count:,}")
    print(f"  저장 경로: {output}")
    print(f"  소요 시간: {elapsed:.2f}초")

    # 미리보기
    print("\n  -- 일별 카테고리별 집계 미리보기 (상위 10건) --")
    daily_summary.show(10, truncate=False)

    return daily_summary


def merchant_ranking(df, output_path):
    """가맹점별 매출 랭킹"""
    print("\n[3/6] 가맹점별 매출 랭킹 처리 중...")
    start_time = time.time()

    merchant_stats = df.groupBy("merchant", "category", "sub_category") \
        .agg(
            count("transaction_id").alias("txn_count"),
            countDistinct("card_no").alias("unique_customers"),
            spark_sum("amount").alias("total_revenue"),
            spark_round(avg("amount"), 0).alias("avg_transaction"),
        ) \
        .withColumn(
            "revenue_rank",
            spark_round(col("total_revenue") / 10000, 1)
        ) \
        .orderBy(desc("total_revenue"))

    output = f"{output_path}/merchant_ranking"
    merchant_stats.write \
        .mode("overwrite") \
        .parquet(output)

    elapsed = time.time() - start_time

    print(f"  소요 시간: {elapsed:.2f}초")
    print("\n  -- 가맹점 매출 TOP 15 --")
    merchant_stats.show(15, truncate=False)

    return merchant_stats


def hourly_pattern_analysis(df, output_path):
    """시간대별 거래 패턴 분석"""
    print("\n[4/6] 시간대별 거래 패턴 분석 중...")
    start_time = time.time()

    hourly_stats = df.groupBy("txn_hour") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 0).alias("avg_amount"),
            countDistinct("card_no").alias("unique_cards"),
        ) \
        .withColumn(
            "time_slot",
            when(col("txn_hour").between(6, 11), "오전")
            .when(col("txn_hour").between(12, 17), "오후")
            .when(col("txn_hour").between(18, 23), "저녁")
            .otherwise("심야")
        ) \
        .orderBy("txn_hour")

    output = f"{output_path}/hourly_pattern"
    hourly_stats.write \
        .mode("overwrite") \
        .parquet(output)

    elapsed = time.time() - start_time

    print(f"  소요 시간: {elapsed:.2f}초")
    print("\n  -- 시간대별 거래 패턴 --")
    hourly_stats.show(24, truncate=False)

    return hourly_stats


def regional_analysis(df, output_path):
    """지역별 거래 분석"""
    print("\n[5/6] 지역별 거래 분석 중...")
    start_time = time.time()

    regional_stats = df.groupBy("region", "category") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 0).alias("avg_amount"),
            countDistinct("card_no").alias("unique_cards"),
        ) \
        .orderBy("region", desc("total_amount"))

    output = f"{output_path}/regional_analysis"
    regional_stats.write \
        .mode("overwrite") \
        .partitionBy("region") \
        .parquet(output)

    elapsed = time.time() - start_time

    print(f"  소요 시간: {elapsed:.2f}초")
    print("\n  -- 지역별 거래 분석 (상위 15건) --")
    regional_stats.show(15, truncate=False)

    return regional_stats


def monthly_trend(df, output_path):
    """월별 트렌드 분석"""
    print("\n[6/6] 월별 트렌드 분석 중...")
    start_time = time.time()

    monthly_stats = df.groupBy("txn_year", "txn_month") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 0).alias("avg_amount"),
            countDistinct("card_no").alias("unique_cards"),
            countDistinct("merchant").alias("unique_merchants"),
        ) \
        .orderBy("txn_year", "txn_month")

    output = f"{output_path}/monthly_trend"
    monthly_stats.write \
        .mode("overwrite") \
        .parquet(output)

    elapsed = time.time() - start_time

    print(f"  소요 시간: {elapsed:.2f}초")
    print("\n  -- 월별 트렌드 --")
    monthly_stats.show(12, truncate=False)

    return monthly_stats


def generate_etl_report(df, output_path):
    """ETL 실행 결과 요약 리포트"""
    print("\n" + "=" * 70)
    print("ETL 처리 결과 요약")
    print("=" * 70)

    total_count = df.count()
    total_amount = df.agg(spark_sum("amount")).collect()[0][0]
    unique_cards = df.select("card_no").distinct().count()
    unique_merchants = df.select("merchant").distinct().count()
    date_range = df.agg(
        spark_min("txn_date").alias("min_date"),
        spark_max("txn_date").alias("max_date")
    ).collect()[0]

    print(f"  총 처리 건수     : {total_count:,}")
    print(f"  총 거래 금액     : {total_amount:,}원")
    print(f"  고유 카드 수     : {unique_cards:,}")
    print(f"  고유 가맹점 수   : {unique_merchants:,}")
    print(f"  거래 기간        : {date_range['min_date']} ~ {date_range['max_date']}")
    print(f"  결과 저장 경로   : {output_path}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="카드 거래 데이터 Spark ETL 파이프라인"
    )
    parser.add_argument(
        "--input", type=str, required=True,
        help="입력 데이터 경로 (CSV 파일 또는 S3 경로)"
    )
    parser.add_argument(
        "--output", type=str, required=True,
        help="출력 경로 (로컬 디렉토리 또는 S3 경로)"
    )
    args = parser.parse_args()

    total_start = time.time()

    print("=" * 70)
    print("카드 거래 데이터 배치 처리 파이프라인")
    print("=" * 70)

    # 1. Spark 세션 생성 (입력 경로 기반 S3 자동 감지)
    storage = "s3" if args.input.startswith("s3") else "local"
    spark = create_spark_session(storage=storage)

    try:
        # 2. 데이터 로드
        df_all, df_approved = load_data(spark, args.input)

        # 3. 배치 처리 단계 실행
        daily_category_summary(df_approved, args.output)
        merchant_ranking(df_approved, args.output)
        hourly_pattern_analysis(df_approved, args.output)
        regional_analysis(df_approved, args.output)
        monthly_trend(df_approved, args.output)

        # 4. 전체 데이터 Parquet 변환 저장 (파티셔닝 적용)
        print("\n파티셔닝된 Parquet 파일 저장 중...")
        df_approved.write \
            .mode("overwrite") \
            .partitionBy("txn_year", "txn_month") \
            .parquet(f"{args.output}/partitioned_transactions")
        print(f"  저장 완료: {args.output}/partitioned_transactions")

        # 5. ETL 요약 리포트
        generate_etl_report(df_approved, args.output)

        total_elapsed = time.time() - total_start
        print(f"\n전체 ETL 소요 시간: {total_elapsed:.2f}초")

    except Exception as e:
        print(f"\nETL 실행 오류: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark 세션 종료")


if __name__ == "__main__":
    main()
