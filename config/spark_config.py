"""
Spark 및 애플리케이션 설정 관리

로컬 실행과 EMR 실행 모두 호환되는 설정을 관리합니다.
환경 변수 또는 설정 파일로 오버라이드 가능.
"""

import os


class SparkConfig:
    """Spark 설정"""

    # 실행 환경 감지
    ENV = os.getenv("SPARK_ENV", "local")  # local | emr

    # 공통 설정
    APP_NAME = "CardTransactionETL"
    LOG_LEVEL = "WARN"

    # Spark SQL 설정
    SHUFFLE_PARTITIONS = 200
    PARQUET_COMPRESSION = "snappy"

    # 직렬화
    SERIALIZER = "org.apache.spark.serializer.KryoSerializer"

    # Adaptive Query Execution (Spark 3.x)
    AQE_ENABLED = True
    AQE_COALESCE_PARTITIONS = True

    @classmethod
    def get_spark_config(cls):
        """환경별 Spark 설정 딕셔너리 반환"""
        config = {
            "spark.app.name": cls.APP_NAME,
            "spark.sql.shuffle.partitions": str(cls.SHUFFLE_PARTITIONS),
            "spark.sql.parquet.compression.codec": cls.PARQUET_COMPRESSION,
            "spark.serializer": cls.SERIALIZER,
            "spark.sql.adaptive.enabled": str(cls.AQE_ENABLED).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(cls.AQE_COALESCE_PARTITIONS).lower(),
        }

        if cls.ENV == "local":
            config.update({
                "spark.master": "local[*]",
                "spark.driver.memory": "2g",
            })
        elif cls.ENV == "emr":
            config.update({
                "spark.master": "yarn",
                "spark.submit.deployMode": "cluster",
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "4g",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": "1",
                "spark.dynamicAllocation.maxExecutors": "10",
            })

        return config


class AppConfig:
    """애플리케이션 설정"""

    ENV = os.getenv("SPARK_ENV", "local")

    # S3 설정 (EMR 환경)
    S3_BUCKET = os.getenv("S3_BUCKET", "your-card-etl-project")
    S3_INPUT_PREFIX = "input/"
    S3_OUTPUT_PREFIX = "output/"
    S3_SCRIPTS_PREFIX = "scripts/"
    S3_LOGS_PREFIX = "logs/"

    # 데이터 설정
    DEFAULT_RECORDS = 1_000_000
    DEFAULT_YEAR = 2024

    # 데이터 품질 임계값
    NULL_THRESHOLD_PERCENT = 1.0        # Null 허용 비율 (%)
    DUPLICATE_THRESHOLD = 0             # 중복 허용 건수
    AMOUNT_MAX = 10_000_000             # 최대 거래금액 (원)

    # 유효값
    VALID_CATEGORIES = ["식비", "교통", "쇼핑", "문화", "의료", "교육", "통신", "보험"]
    VALID_STATUSES = ["approved", "declined", "cancelled"]
    VALID_REGIONS = ["서울", "경기", "인천", "부산", "대구", "대전", "광주", "제주"]

    @classmethod
    def get_input_path(cls, filename="card_transactions.csv"):
        if cls.ENV == "emr":
            return f"s3://{cls.S3_BUCKET}/{cls.S3_INPUT_PREFIX}{filename}"
        return f"data/{filename}"

    @classmethod
    def get_output_path(cls, sub_path=""):
        if cls.ENV == "emr":
            return f"s3://{cls.S3_BUCKET}/{cls.S3_OUTPUT_PREFIX}{sub_path}"
        return f"output/{sub_path}"
