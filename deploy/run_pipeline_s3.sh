#!/bin/bash
# ================================================================
#  S3 연동 전체 파이프라인 실행 스크립트
#  LocalStack을 S3 에뮬레이터로 사용하여
#  실제 AWS EMR 환경과 동일한 S3 I/O 흐름을 재현합니다.
#
#  사용법:
#    bash deploy/run_pipeline_s3.sh
# ================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# -- S3 환경변수 (LocalStack) --
export S3_ENDPOINT="http://localhost:4566"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="ap-northeast-2"

# -- Python / Spark 경로 --
export PYSPARK_PYTHON="${PROJECT_DIR}/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="${PROJECT_DIR}/.venv/bin/python"

# spark-submit 자동 감지
SPARK_SUBMIT=$(find "${PROJECT_DIR}/.venv" -name "spark-submit" -type f 2>/dev/null | head -1)
if [ -z "$SPARK_SUBMIT" ]; then
    echo "❌ spark-submit을 찾을 수 없습니다."
    exit 1
fi

# Hadoop-AWS 패키지 (PySpark Hadoop 3.4.2에 맞춤)
PACKAGES="org.apache.hadoop:hadoop-aws:3.4.2"

# S3 경로
INPUT="s3a://card-pipeline-input/card_transactions.csv"
OUTPUT="s3a://card-pipeline-output"

echo "================================================================"
echo "  🚀 S3 연동 카드 거래 배치 파이프라인"
echo "================================================================"
echo "  Storage  : S3 (LocalStack @ ${S3_ENDPOINT})"
echo "  Input    : ${INPUT}"
echo "  Output   : ${OUTPUT}"
echo "  Packages : hadoop-aws:3.4.2"
echo "================================================================"

TOTAL_START=$(date +%s)

# ============================================
#  Step 1: 데이터 품질 검증
# ============================================
echo ""
echo "============================================"
echo "  [Step 1/4] 데이터 품질 검증"
echo "============================================"
$SPARK_SUBMIT \
    --packages "$PACKAGES" \
    scripts/data_quality_check.py \
        --input "$INPUT" \
        --output "${OUTPUT}/quality_report"

# ============================================
#  Step 2: 메인 Spark ETL
# ============================================
echo ""
echo "============================================"
echo "  [Step 2/4] 메인 Spark ETL"
echo "============================================"
$SPARK_SUBMIT \
    --packages "$PACKAGES" \
    scripts/spark_etl.py \
        --input "$INPUT" \
        --output "${OUTPUT}"

# ============================================
#  Step 3: 분기별 배치 처리
# ============================================
echo ""
echo "============================================"
echo "  [Step 3/4] 분기별 배치 처리"
echo "============================================"
$SPARK_SUBMIT \
    --packages "$PACKAGES" \
    scripts/quarterly_batch.py \
        --input "$INPUT" \
        --output "${OUTPUT}/quarterly"

# ============================================
#  Step 4: 성능 벤치마크
# ============================================
echo ""
echo "============================================"
echo "  [Step 4/4] 성능 벤치마크"
echo "============================================"
$SPARK_SUBMIT \
    --packages "$PACKAGES" \
    scripts/performance_optimizer.py \
        --input "$INPUT" \
        --output "${OUTPUT}/optimized"

TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$((TOTAL_END - TOTAL_START))

# ============================================
#  결과 확인: S3 출력 목록
# ============================================
echo ""
echo "================================================================"
echo "  ✅ 전체 파이프라인 완료! (${TOTAL_ELAPSED}초)"
echo "================================================================"
echo ""
echo "  S3 출력 결과:"

"${PROJECT_DIR}/.venv/bin/python" << 'PYEOF'
import boto3, os
s3 = boto3.client("s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id="test", aws_secret_access_key="test",
    region_name="ap-northeast-2")

paginator = s3.get_paginator("list_objects_v2")
total_size = 0
dirs = set()
file_count = 0

for page in paginator.paginate(Bucket="card-pipeline-output"):
    for obj in page.get("Contents", []):
        parts = obj["Key"].split("/")
        if len(parts) > 1:
            dirs.add(parts[0])
        total_size += obj["Size"]
        file_count += 1

for d in sorted(dirs):
    print(f"    📁 s3://card-pipeline-output/{d}/")
print(f"\n    총 {file_count}개 파일, {total_size / 1024 / 1024:.1f} MB")
PYEOF

echo ""
echo "================================================================"
