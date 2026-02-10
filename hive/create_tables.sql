-- ============================================================
-- 카드 거래 데이터 Hive 테이블 정의서 (DDL)
--
-- 목적:  Spark ETL의 Parquet 출력 → Hive Metastore 등록
-- 용도:  Hive, Impala, Presto, Athena 에서 SQL 조회 가능
-- 환경:  EMR(HDFS/S3), LocalStack(S3), 로컬(HDFS)
-- ============================================================

-- 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS card_analytics
    COMMENT '카드사 거래 데이터 분석 데이터베이스'
    LOCATION 's3://card-pipeline-output/';

USE card_analytics;

-- ============================================================
-- 1. 원본 거래 데이터 (파티션 테이블)
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS partitioned_transactions (
    transaction_id      STRING      COMMENT '거래 고유 ID',
    card_number         STRING      COMMENT '카드 번호 (마스킹됨)',
    amount              BIGINT      COMMENT '거래 금액 (원)',
    merchant            STRING      COMMENT '가맹점 이름',
    category            STRING      COMMENT '업종 카테고리',
    card_type           STRING      COMMENT '카드 유형 (신용/체크)',
    status              STRING      COMMENT '거래 상태 (승인/취소)',
    region              STRING      COMMENT '지역',
    timestamp           TIMESTAMP   COMMENT '거래 일시',
    installment_months  INT         COMMENT '할부 개월 수 (0=일시불)'
)
PARTITIONED BY (
    txn_year    INT     COMMENT '거래 연도',
    txn_month   INT     COMMENT '거래 월'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/partitioned_transactions/'
TBLPROPERTIES (
    'parquet.compress'          = 'SNAPPY',
    'classification'            = 'parquet',
    'spark.sql.sources.schema'  = 'auto'
);

-- Hive: 파티션 자동 인식
MSCK REPAIR TABLE partitioned_transactions;

-- ============================================================
-- 2. 일별 카테고리 매출 요약
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS daily_category_summary (
    txn_date                STRING      COMMENT '거래 일자 (yyyy-MM-dd)',
    category                STRING      COMMENT '업종 카테고리',
    total_amount            BIGINT      COMMENT '총 거래 금액',
    transaction_count       BIGINT      COMMENT '거래 건수',
    avg_amount              DOUBLE      COMMENT '평균 거래 금액',
    approved_count          BIGINT      COMMENT '승인 건수',
    cancelled_count         BIGINT      COMMENT '취소 건수',
    approval_rate           DOUBLE      COMMENT '승인율 (%)',
    unique_cards            BIGINT      COMMENT '고유 카드 수'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/daily_category_summary/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- 3. 가맹점 랭킹
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS merchant_ranking (
    merchant                STRING      COMMENT '가맹점 이름',
    category                STRING      COMMENT '업종 카테고리',
    total_amount            BIGINT      COMMENT '총 매출 금액',
    transaction_count       BIGINT      COMMENT '총 거래 건수',
    avg_amount              DOUBLE      COMMENT '평균 거래 금액',
    unique_cards            BIGINT      COMMENT '고유 카드 수',
    amount_rank             INT         COMMENT '매출 순위'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/merchant_ranking/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- 4. 시간대별 패턴 분석
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS hourly_pattern (
    hour                    INT         COMMENT '거래 시간 (0-23)',
    time_slot               STRING      COMMENT '시간대 (새벽/오전/오후/저녁/심야)',
    transaction_count       BIGINT      COMMENT '거래 건수',
    total_amount            BIGINT      COMMENT '총 거래 금액',
    avg_amount              DOUBLE      COMMENT '평균 거래 금액',
    credit_ratio            DOUBLE      COMMENT '신용카드 비율 (%)',
    cancel_rate             DOUBLE      COMMENT '취소율 (%)'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/hourly_pattern/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- 5. 지역별 분석
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS regional_analysis (
    region                  STRING      COMMENT '지역',
    total_amount            BIGINT      COMMENT '총 거래 금액',
    transaction_count       BIGINT      COMMENT '거래 건수',
    avg_amount              DOUBLE      COMMENT '평균 거래 금액',
    unique_merchants        BIGINT      COMMENT '고유 가맹점 수',
    top_category            STRING      COMMENT '최다 거래 카테고리',
    credit_ratio            DOUBLE      COMMENT '신용카드 비율 (%)'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/regional_analysis/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- 6. 월별 트렌드
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS monthly_trend (
    txn_month               STRING      COMMENT '거래 월 (yyyy-MM)',
    total_amount            BIGINT      COMMENT '총 거래 금액',
    transaction_count       BIGINT      COMMENT '거래 건수',
    avg_amount              DOUBLE      COMMENT '평균 거래 금액',
    mom_growth_rate         DOUBLE      COMMENT '전월 대비 성장률 (%)',
    unique_cards            BIGINT      COMMENT '고유 카드 수',
    unique_merchants        BIGINT      COMMENT '고유 가맹점 수'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/monthly_trend/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- 7. 분기별 카테고리 요약
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS quarterly_category_summary (
    quarter                 STRING      COMMENT '분기 (Q1~Q4)',
    category                STRING      COMMENT '업종 카테고리',
    total_amount            BIGINT      COMMENT '총 거래 금액',
    transaction_count       BIGINT      COMMENT '거래 건수',
    avg_amount              DOUBLE      COMMENT '평균 거래 금액',
    qoq_growth_rate         DOUBLE      COMMENT '전분기 대비 성장률 (%)'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/quarterly/category_summary/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ============================================================
-- 8. 데이터 품질 리포트
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS quality_report (
    check_category          STRING      COMMENT '검증 카테고리',
    check_name              STRING      COMMENT '검증 항목 이름',
    status                  STRING      COMMENT '검증 결과 (PASS/FAIL/WARNING)',
    detail                  STRING      COMMENT '검증 상세 메시지',
    checked_at              TIMESTAMP   COMMENT '검증 수행 시각'
)
STORED AS PARQUET
LOCATION 's3://card-pipeline-output/quality_report/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');


-- ============================================================
--  Impala 용 메타데이터 동기화
-- ============================================================
-- Hive Metastore에 등록한 뒤 Impala에서 즉시 조회하려면:
--   INVALIDATE METADATA card_analytics.partitioned_transactions;
--   INVALIDATE METADATA card_analytics.daily_category_summary;
--   ...


-- ============================================================
--  Athena 참고
-- ============================================================
-- AWS Athena에서는 Glue Data Catalog이 Hive Metastore 역할을 합니다.
-- 위 DDL을 Athena 콘솔에서 그대로 실행하면 테이블이 등록됩니다.
-- LOCATION만 실제 S3 경로로 변경하면 됩니다.
-- 예: LOCATION 's3://my-prod-bucket/card-pipeline-output/daily_category_summary/'


-- ============================================================
--  유용한 분석 쿼리 예시 (Hive / Impala / Athena 공통)
-- ============================================================

-- 1) 일별 매출 TOP 5 카테고리
-- SELECT txn_date, category, total_amount,
--        ROW_NUMBER() OVER (PARTITION BY txn_date ORDER BY total_amount DESC) AS rn
-- FROM   daily_category_summary
-- WHERE  rn <= 5;

-- 2) 시간대별 거래 패턴 (심야 거래 비중)
-- SELECT time_slot,
--        SUM(transaction_count) AS total_txns,
--        ROUND(SUM(transaction_count) * 100.0
--              / SUM(SUM(transaction_count)) OVER (), 2) AS pct
-- FROM   hourly_pattern
-- GROUP BY time_slot;

-- 3) 지역별 월간 매출 추이
-- SELECT t.txn_month, r.region, r.total_amount
-- FROM   regional_analysis r
-- JOIN   monthly_trend t ON 1=1
-- ORDER BY t.txn_month, r.total_amount DESC;
