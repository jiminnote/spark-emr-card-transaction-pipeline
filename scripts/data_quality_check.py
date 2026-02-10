"""
카드 거래 데이터 품질 검증 스크립트

실무에서 배치 처리 전 필수적으로 수행하는 데이터 품질 검증을 자동화합니다.

검증 항목:
1. Null/결측값 검사
2. 중복 거래 탐지
3. 금액 범위 이상치 검출
4. 날짜 유효성 검증
5. 카테고리 유효성 검증
6. 카드번호 포맷 검증
7. 승인 상태 분포 이상 탐지
8. 종합 품질 리포트 생성

사용법:
  spark-submit scripts/data_quality_check.py --input data/card_transactions.csv [--output output/quality_report/]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    isnan,
    isnull,
    length,
    lit,
    regexp_extract,
    sum as spark_sum,
    to_timestamp,
    when,
)


# -- 검증 기준값 --
VALID_CATEGORIES = ["식비", "교통", "쇼핑", "문화", "의료", "교육", "통신", "보험"]
VALID_STATUSES = ["approved", "declined", "cancelled"]
VALID_CARD_TYPES = ["신용", "체크"]
VALID_REGIONS = ["서울", "경기", "인천", "부산", "대구", "대전", "광주", "제주"]
AMOUNT_MIN = 0
AMOUNT_MAX = 10000000  # 1000만원
CARD_NO_PATTERN = r"^\*{4}-\*{4}-\*{4}-\d{4}$"


def create_spark_session(storage="local"):
    """Spark 세션 생성 (로컬/S3 모두 호환)"""
    builder = SparkSession.builder \
        .appName("CardTransactionDataQuality")

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


class DataQualityChecker:
    """데이터 품질 검증 클래스"""

    def __init__(self, spark, df):
        self.spark = spark
        self.df = df
        self.total_records = df.count()
        self.issues = []
        self.summary = {
            "check_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_records": self.total_records,
            "checks": [],
            "overall_status": "PASS",
        }

    def _add_result(self, check_name, status, issue_count, detail=""):
        """검증 결과 기록"""
        ratio = (issue_count / self.total_records * 100) if self.total_records > 0 else 0
        result = {
            "check": check_name,
            "status": status,
            "issue_count": issue_count,
            "issue_ratio": f"{ratio:.4f}%",
            "detail": detail,
        }
        self.summary["checks"].append(result)

        status_symbol = "PASS" if status == "PASS" else "FAIL" if status == "FAIL" else "WARN"
        print(f"  [{status_symbol}] {check_name}: {issue_count:,}건 ({ratio:.4f}%) {detail}")

        if status == "FAIL":
            self.summary["overall_status"] = "FAIL"
        elif status == "WARN" and self.summary["overall_status"] != "FAIL":
            self.summary["overall_status"] = "WARN"

    def check_null_values(self):
        """1. Null/결측값 검사"""
        print("\n--- 1. Null/결측값 검사 ---")

        for column in self.df.columns:
            col_type = str(dict(self.df.dtypes).get(column, "string")).lower()
            if col_type in ("string",):
                null_count = self.df.filter(
                    isnull(col(column)) | (col(column) == "") | (col(column) == "null")
                ).count()
            else:
                null_count = self.df.filter(isnull(col(column))).count()

            if null_count > 0:
                threshold = self.total_records * 0.01  # 1% 이상이면 FAIL
                status = "FAIL" if null_count > threshold else "WARN"
                self._add_result(
                    f"Null 검사 [{column}]",
                    status,
                    null_count,
                    f"컬럼 '{column}'에 Null 값 발견"
                )
            else:
                self._add_result(f"Null 검사 [{column}]", "PASS", 0)

    def check_duplicates(self):
        """2. 중복 거래 탐지"""
        print("\n--- 2. 중복 거래 탐지 ---")

        # transaction_id 기준 중복
        dup_by_id = self.df.groupBy("transaction_id") \
            .count() \
            .filter("count > 1")
        dup_id_count = dup_by_id.count()

        status = "FAIL" if dup_id_count > 0 else "PASS"
        self._add_result(
            "거래ID 중복 검사",
            status,
            dup_id_count,
            "transaction_id 중복 발견" if dup_id_count > 0 else ""
        )

        if dup_id_count > 0:
            print("  -- 중복 거래 ID 샘플 (상위 5건) --")
            dup_by_id.orderBy(col("count").desc()).show(5, truncate=False)

        # 동일 시각/카드/금액 중복 (실제 이중 결제 의심)
        dup_suspicious = self.df.groupBy(
            "transaction_date", "card_no", "amount", "merchant"
        ).count().filter("count > 1")
        dup_sus_count = dup_suspicious.count()

        status = "WARN" if dup_sus_count > 0 else "PASS"
        self._add_result(
            "이중결제 의심 검사",
            status,
            dup_sus_count,
            "동일 시각/카드/금액/가맹점 거래 발견" if dup_sus_count > 0 else ""
        )

    def check_amount_range(self):
        """3. 금액 범위 이상치 검출"""
        print("\n--- 3. 금액 범위 이상치 검출 ---")

        # 음수 금액
        negative_count = self.df.filter(col("amount") < AMOUNT_MIN).count()
        status = "FAIL" if negative_count > 0 else "PASS"
        self._add_result("음수 금액 검사", status, negative_count)

        # 과도한 금액 (1000만원 초과)
        excessive_count = self.df.filter(col("amount") > AMOUNT_MAX).count()
        status = "WARN" if excessive_count > 0 else "PASS"
        self._add_result(
            "과도 금액 검사",
            status,
            excessive_count,
            f"기준: {AMOUNT_MAX:,}원 초과"
        )

        # 0원 거래
        zero_count = self.df.filter(col("amount") == 0).count()
        status = "WARN" if zero_count > 0 else "PASS"
        self._add_result("0원 거래 검사", status, zero_count)

        # 금액 통계
        stats = self.df.agg(
            avg("amount").alias("avg"),
            spark_sum(when(col("amount") < 0, 1).otherwise(0)).alias("neg"),
        ).collect()[0]
        print(f"  평균 거래금액: {stats['avg']:,.0f}원")

    def check_date_validity(self):
        """4. 날짜 유효성 검증"""
        print("\n--- 4. 날짜 유효성 검증 ---")

        from pyspark.sql.functions import try_to_timestamp, current_timestamp

        df_with_ts = self.df.withColumn(
            "parsed_ts",
            try_to_timestamp(col("transaction_date"), lit("yyyy-MM-dd HH:mm:ss"))
        )

        # 파싱 실패 (잘못된 날짜 형식)
        invalid_date_count = df_with_ts.filter(isnull(col("parsed_ts"))).count()
        status = "FAIL" if invalid_date_count > 0 else "PASS"
        self._add_result(
            "날짜 형식 검사",
            status,
            invalid_date_count,
            "yyyy-MM-dd HH:mm:ss 형식 불일치"
        )

        # 미래 날짜
        future_count = df_with_ts.filter(
            col("parsed_ts") > current_timestamp()
        ).count()
        status = "WARN" if future_count > 0 else "PASS"
        self._add_result("미래 날짜 검사", status, future_count)

    def check_category_validity(self):
        """5. 카테고리 유효성 검증"""
        print("\n--- 5. 카테고리 유효성 검증 ---")

        invalid_cat_count = self.df.filter(
            ~col("category").isin(VALID_CATEGORIES)
        ).count()
        status = "FAIL" if invalid_cat_count > 0 else "PASS"
        self._add_result(
            "카테고리 유효성 검사",
            status,
            invalid_cat_count,
            f"유효 카테고리: {VALID_CATEGORIES}"
        )

        # 카드유형 유효성
        invalid_type_count = self.df.filter(
            ~col("card_type").isin(VALID_CARD_TYPES)
        ).count()
        status = "FAIL" if invalid_type_count > 0 else "PASS"
        self._add_result("카드유형 유효성 검사", status, invalid_type_count)

        # 승인상태 유효성
        invalid_status_count = self.df.filter(
            ~col("approval_status").isin(VALID_STATUSES)
        ).count()
        status = "FAIL" if invalid_status_count > 0 else "PASS"
        self._add_result("승인상태 유효성 검사", status, invalid_status_count)

        # 지역 유효성
        invalid_region_count = self.df.filter(
            ~col("region").isin(VALID_REGIONS)
        ).count()
        status = "FAIL" if invalid_region_count > 0 else "PASS"
        self._add_result("지역 유효성 검사", status, invalid_region_count)

    def check_card_number_format(self):
        """6. 카드번호 포맷 검증"""
        print("\n--- 6. 카드번호 포맷 검증 ---")

        # 마스킹 패턴 확인: ****-****-****-1234
        invalid_format = self.df.filter(
            ~col("card_no").rlike(r"^\*{4}-\*{4}-\*{4}-\d{4}$")
        ).count()

        status = "FAIL" if invalid_format > 0 else "PASS"
        self._add_result(
            "카드번호 마스킹 검사",
            status,
            invalid_format,
            "****-****-****-NNNN 패턴 불일치"
        )

    def check_approval_distribution(self):
        """7. 승인 상태 분포 이상 탐지"""
        print("\n--- 7. 승인 상태 분포 분석 ---")

        status_dist = self.df.groupBy("approval_status") \
            .agg(
                count("*").alias("cnt")
            ).collect()

        for row in status_dist:
            status_name = row["approval_status"]
            cnt = row["cnt"]
            ratio = cnt / self.total_records * 100
            print(f"  {status_name}: {cnt:,}건 ({ratio:.2f}%)")

            # 거절률 10% 초과 시 경고
            if status_name == "declined" and ratio > 10:
                self._add_result(
                    "거절률 이상 탐지",
                    "WARN",
                    cnt,
                    f"거절률 {ratio:.2f}% (기준: 10% 이하)"
                )

    def generate_report(self, output_path=None):
        """종합 품질 리포트 생성"""
        print("\n" + "=" * 70)
        print("데이터 품질 검증 종합 리포트")
        print("=" * 70)
        print(f"  검증 시각       : {self.summary['check_timestamp']}")
        print(f"  총 레코드 수    : {self.total_records:,}")
        print(f"  총 검증 항목    : {len(self.summary['checks'])}")

        pass_count = sum(1 for c in self.summary["checks"] if c["status"] == "PASS")
        warn_count = sum(1 for c in self.summary["checks"] if c["status"] == "WARN")
        fail_count = sum(1 for c in self.summary["checks"] if c["status"] == "FAIL")

        print(f"  PASS: {pass_count}  |  WARN: {warn_count}  |  FAIL: {fail_count}")
        print(f"  종합 판정       : {self.summary['overall_status']}")
        print("=" * 70)

        if output_path:
            # JSON 리포트 저장
            report_path = f"{output_path}/quality_report.json"
            report_json = json.dumps(self.summary, ensure_ascii=False, indent=2)

            # Spark를 사용해 저장 (S3 호환)
            rdd = self.spark.sparkContext.parallelize([report_json])
            rdd.coalesce(1).saveAsTextFile(f"{output_path}/quality_report")
            print(f"\n리포트 저장: {output_path}/quality_report")

        return self.summary


def main():
    parser = argparse.ArgumentParser(
        description="카드 거래 데이터 품질 검증"
    )
    parser.add_argument(
        "--input", type=str, required=True,
        help="검증 대상 데이터 경로"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="품질 리포트 저장 경로 (선택)"
    )
    args = parser.parse_args()

    total_start = time.time()

    print("=" * 70)
    print("카드 거래 데이터 품질 검증 시작")
    print("=" * 70)

    # 입력 경로 기반 S3 자동 감지
    storage = "s3" if args.input.startswith("s3") else "local"
    spark = create_spark_session(storage=storage)

    try:
        # 데이터 로드 (문자열로 읽어서 타입 캐스팅 에러 방지)
        df = spark.read \
            .option("header", "true") \
            .csv(args.input)

        # amount를 정수로 변환
        from pyspark.sql.functions import trim
        df = df.withColumn("amount", col("amount").cast("int")) \
               .withColumn("installment_months", col("installment_months").cast("int"))

        print(f"데이터 로드 완료: {args.input}")
        print(f"스키마:")
        df.printSchema()

        # 품질 검증 실행
        checker = DataQualityChecker(spark, df)

        checker.check_null_values()
        checker.check_duplicates()
        checker.check_amount_range()
        checker.check_date_validity()
        checker.check_category_validity()
        checker.check_card_number_format()
        checker.check_approval_distribution()

        # 리포트 생성
        report = checker.generate_report(output_path=args.output)

        total_elapsed = time.time() - total_start
        print(f"\n전체 검증 소요 시간: {total_elapsed:.2f}초")

        # 종합 판정이 FAIL이면 exit code 1 반환 (파이프라인 연동용)
        if report["overall_status"] == "FAIL":
            print("\n데이터 품질 검증 실패. 배치 처리를 중단합니다.")
            sys.exit(1)

    except Exception as e:
        print(f"\n품질 검증 오류: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
