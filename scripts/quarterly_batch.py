"""
분기별 배치 처리 스크립트

현대카드 실무의 분기별 정기 배치 프로세스를 시뮬레이션합니다.
- 1월, 4월, 7월, 10월 분기 기준 배치 처리
- 분기별 카테고리/가맹점 매출 리포트 자동 생성
- 전분기 대비 증감률 분석
- 분기별 고객 소비 패턴 변화 분석

사용법:
  spark-submit scripts/quarterly_batch.py \
    --input data/card_transactions.csv \
    --output output/quarterly/ \
    [--year 2024]
"""

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    dense_rank,
    desc,
    lag,
    lit,
    month,
    quarter,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    when,
    year,
)


def create_spark_session(storage="local"):
    """Spark 세션 생성 (로컬/S3 모두 호환)"""
    builder = SparkSession.builder \
        .appName("CardTransactionQuarterlyBatch") \
        .config("spark.sql.parquet.compression.codec", "snappy")

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


def load_and_prepare(spark, input_path):
    """데이터 로드 및 분기 컬럼 추가"""
    print(f"\n데이터 로드: {input_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)

    df = df.withColumn(
        "transaction_ts",
        to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
    )
    df = df.withColumn("txn_year", year(col("transaction_ts")))
    df = df.withColumn("txn_month", month(col("transaction_ts")))
    df = df.withColumn("txn_quarter", quarter(col("transaction_ts")))

    # 분기 라벨 (Q1, Q2, Q3, Q4)
    df = df.withColumn(
        "quarter_label",
        when(col("txn_quarter") == 1, "Q1(1-3월)")
        .when(col("txn_quarter") == 2, "Q2(4-6월)")
        .when(col("txn_quarter") == 3, "Q3(7-9월)")
        .otherwise("Q4(10-12월)")
    )

    # 승인 건만 필터
    df = df.filter(col("approval_status") == "approved")

    print(f"  승인 거래 건수: {df.count():,}")
    return df


def quarterly_category_summary(df, output_path, target_year):
    """분기별 카테고리 매출 집계"""
    print(f"\n[1/4] {target_year}년 분기별 카테고리 매출 집계")
    start_time = time.time()

    df_year = df.filter(col("txn_year") == target_year)

    quarterly_stats = df_year.groupBy("txn_quarter", "quarter_label", "category") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 0).alias("avg_amount"),
            countDistinct("card_no").alias("unique_cards"),
        ) \
        .orderBy("txn_quarter", desc("total_amount"))

    output = f"{output_path}/{target_year}/quarterly_category_summary"
    quarterly_stats.write \
        .mode("overwrite") \
        .partitionBy("txn_quarter") \
        .parquet(output)

    elapsed = time.time() - start_time
    print(f"  소요 시간: {elapsed:.2f}초")
    print(f"  저장: {output}")
    print("\n  -- 분기별 카테고리 매출 --")
    quarterly_stats.show(30, truncate=False)

    return quarterly_stats


def quarterly_merchant_ranking(df, output_path, target_year):
    """분기별 가맹점 매출 TOP 랭킹"""
    print(f"\n[2/4] {target_year}년 분기별 가맹점 TOP 랭킹")
    start_time = time.time()

    df_year = df.filter(col("txn_year") == target_year)

    merchant_quarterly = df_year.groupBy("txn_quarter", "quarter_label", "merchant", "category") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_revenue"),
            countDistinct("card_no").alias("unique_customers"),
        )

    # 분기 내 매출 순위
    window_spec = Window.partitionBy("txn_quarter").orderBy(desc("total_revenue"))
    merchant_ranked = merchant_quarterly.withColumn(
        "rank", dense_rank().over(window_spec)
    ).filter(col("rank") <= 10)

    output = f"{output_path}/{target_year}/quarterly_merchant_top10"
    merchant_ranked.write \
        .mode("overwrite") \
        .partitionBy("txn_quarter") \
        .parquet(output)

    elapsed = time.time() - start_time
    print(f"  소요 시간: {elapsed:.2f}초")

    for q in range(1, 5):
        print(f"\n  -- Q{q} 가맹점 매출 TOP 10 --")
        merchant_ranked.filter(col("txn_quarter") == q).show(10, truncate=False)

    return merchant_ranked


def quarter_over_quarter_growth(df, output_path, target_year):
    """전분기 대비 증감률 분석 (QoQ Growth)"""
    print(f"\n[3/4] {target_year}년 전분기 대비 증감률 분석")
    start_time = time.time()

    df_year = df.filter(col("txn_year") == target_year)

    # 분기별 총 매출
    quarterly_total = df_year.groupBy("txn_quarter", "quarter_label") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            countDistinct("card_no").alias("unique_cards"),
        ) \
        .orderBy("txn_quarter")

    # 전분기 대비 증감률 계산
    window_spec = Window.orderBy("txn_quarter")

    qoq_analysis = quarterly_total \
        .withColumn("prev_amount", lag("total_amount").over(window_spec)) \
        .withColumn("prev_count", lag("txn_count").over(window_spec)) \
        .withColumn(
            "amount_growth_rate",
            when(
                col("prev_amount").isNotNull(),
                spark_round(
                    (col("total_amount") - col("prev_amount")) / col("prev_amount") * 100,
                    2
                )
            ).otherwise(lit(None))
        ) \
        .withColumn(
            "count_growth_rate",
            when(
                col("prev_count").isNotNull(),
                spark_round(
                    (col("txn_count") - col("prev_count")) / col("prev_count") * 100,
                    2
                )
            ).otherwise(lit(None))
        )

    output = f"{output_path}/{target_year}/qoq_growth"
    qoq_analysis.write.mode("overwrite").parquet(output)

    elapsed = time.time() - start_time
    print(f"  소요 시간: {elapsed:.2f}초")
    print("\n  -- 전분기 대비 증감률 --")
    qoq_analysis.show(truncate=False)

    return qoq_analysis


def quarterly_customer_pattern(df, output_path, target_year):
    """분기별 고객 소비 패턴 변화 분석"""
    print(f"\n[4/4] {target_year}년 분기별 고객 소비 패턴 분석")
    start_time = time.time()

    df_year = df.filter(col("txn_year") == target_year)

    # 분기별 카드타입별 분석
    card_type_quarterly = df_year.groupBy("txn_quarter", "quarter_label", "card_type") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 0).alias("avg_amount"),
        ) \
        .orderBy("txn_quarter", "card_type")

    # 분기별 지역별 분석
    region_quarterly = df_year.groupBy("txn_quarter", "quarter_label", "region") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
        ) \
        .orderBy("txn_quarter", desc("total_amount"))

    # 분기별 할부 분석
    installment_quarterly = df_year \
        .filter(col("installment_months") > 0) \
        .groupBy("txn_quarter", "quarter_label", "installment_months") \
        .agg(
            count("transaction_id").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
        ) \
        .orderBy("txn_quarter", "installment_months")

    # 저장
    card_type_quarterly.write.mode("overwrite") \
        .parquet(f"{output_path}/{target_year}/quarterly_card_type")
    region_quarterly.write.mode("overwrite") \
        .parquet(f"{output_path}/{target_year}/quarterly_region")
    installment_quarterly.write.mode("overwrite") \
        .parquet(f"{output_path}/{target_year}/quarterly_installment")

    elapsed = time.time() - start_time
    print(f"  소요 시간: {elapsed:.2f}초")

    print("\n  -- 분기별 카드타입 분석 --")
    card_type_quarterly.show(truncate=False)

    print("\n  -- 분기별 지역 매출 (상위) --")
    region_quarterly.show(20, truncate=False)

    print("\n  -- 분기별 할부 이용 현황 --")
    installment_quarterly.show(truncate=False)


def generate_quarterly_report(df, target_year):
    """분기별 배치 종합 리포트"""
    print("\n" + "=" * 70)
    print(f"{target_year}년 분기별 배치 처리 종합 리포트")
    print("=" * 70)

    df_year = df.filter(col("txn_year") == target_year)

    for q in range(1, 5):
        df_q = df_year.filter(col("txn_quarter") == q)
        q_count = df_q.count()

        if q_count == 0:
            print(f"\n  Q{q}: 데이터 없음")
            continue

        q_amount = df_q.agg(spark_sum("amount")).collect()[0][0]
        q_cards = df_q.select("card_no").distinct().count()
        q_merchants = df_q.select("merchant").distinct().count()

        print(f"\n  Q{q} ({q*3-2}~{q*3}월)")
        print(f"    거래 건수    : {q_count:,}")
        print(f"    거래 총액    : {q_amount:,}원")
        print(f"    고유 카드 수 : {q_cards:,}")
        print(f"    고유 가맹점  : {q_merchants:,}")
        print(f"    건당 평균    : {q_amount // q_count:,}원")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="카드 거래 분기별 배치 처리"
    )
    parser.add_argument("--input", type=str, required=True, help="입력 데이터 경로")
    parser.add_argument("--output", type=str, required=True, help="출력 경로")
    parser.add_argument("--year", type=int, default=2024, help="대상 연도 (기본: 2024)")
    args = parser.parse_args()

    total_start = time.time()

    print("=" * 70)
    print(f"분기별 배치 처리 시작 - {args.year}년")
    print("=" * 70)

    # 입력 경로 기반 S3 자동 감지
    storage = "s3" if args.input.startswith("s3") else "local"
    spark = create_spark_session(storage=storage)

    try:
        df = load_and_prepare(spark, args.input)

        quarterly_category_summary(df, args.output, args.year)
        quarterly_merchant_ranking(df, args.output, args.year)
        quarter_over_quarter_growth(df, args.output, args.year)
        quarterly_customer_pattern(df, args.output, args.year)

        generate_quarterly_report(df, args.year)

        total_elapsed = time.time() - total_start
        print(f"\n전체 분기 배치 소요 시간: {total_elapsed:.2f}초")

    except Exception as e:
        print(f"\n분기 배치 오류: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
