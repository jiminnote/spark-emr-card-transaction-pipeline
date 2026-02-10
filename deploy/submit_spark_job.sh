#!/bin/bash
# =============================================================================
# Spark Job 제출 스크립트
#
# EMR 클러스터에 Spark Job을 Step으로 제출합니다.
# 전체 ETL 파이프라인을 순서대로 실행:
#   1. 데이터 품질 검증
#   2. 메인 ETL 처리
#   3. 분기별 배치 처리
#   4. 성능 최적화 벤치마크
#
# 사용법:
#   bash deploy/submit_spark_job.sh [cluster-id] [bucket-name]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# 클러스터 ID (.cluster_id 파일 또는 인자)
if [ -f "${PROJECT_DIR}/.cluster_id" ]; then
    CLUSTER_ID="${1:-$(cat "${PROJECT_DIR}/.cluster_id")}"
else
    CLUSTER_ID="${1:?오류: 클러스터 ID를 지정하세요. 사용법: $0 <cluster-id> [bucket-name]}"
fi

S3_BUCKET="${2:-your-card-etl-project}"

echo "=============================================="
echo "Spark Job 제출"
echo "  Cluster: ${CLUSTER_ID}"
echo "  Bucket:  s3://${S3_BUCKET}"
echo "=============================================="

# Step 1: 데이터 품질 검증
echo ""
echo "[Step 1/4] 데이터 품질 검증 제출..."
STEP1_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "Type=Spark,Name=DataQualityCheck,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3://${S3_BUCKET}/scripts/data_quality_check.py,--input,s3://${S3_BUCKET}/input/card_transactions.csv,--output,s3://${S3_BUCKET}/output/quality_report]" \
    --query "StepIds[0]" \
    --output text)
echo "  Step ID: ${STEP1_ID}"

# Step 1 완료 대기
echo "  품질 검증 실행 대기 중..."
aws emr wait step-complete \
    --cluster-id "${CLUSTER_ID}" \
    --step-id "${STEP1_ID}"
echo "  품질 검증 완료"

# Step 2: 메인 ETL 처리
echo ""
echo "[Step 2/4] 메인 ETL 처리 제출..."
STEP2_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "Type=Spark,Name=MainETL,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3://${S3_BUCKET}/scripts/spark_etl.py,--input,s3://${S3_BUCKET}/input/card_transactions.csv,--output,s3://${S3_BUCKET}/output]" \
    --query "StepIds[0]" \
    --output text)
echo "  Step ID: ${STEP2_ID}"

echo "  메인 ETL 실행 대기 중..."
aws emr wait step-complete \
    --cluster-id "${CLUSTER_ID}" \
    --step-id "${STEP2_ID}"
echo "  메인 ETL 완료"

# Step 3: 분기별 배치 처리
echo ""
echo "[Step 3/4] 분기별 배치 처리 제출..."
STEP3_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "Type=Spark,Name=QuarterlyBatch,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://${S3_BUCKET}/scripts/quarterly_batch.py,--input,s3://${S3_BUCKET}/input/card_transactions.csv,--output,s3://${S3_BUCKET}/output/quarterly,--year,2024]" \
    --query "StepIds[0]" \
    --output text)
echo "  Step ID: ${STEP3_ID}"

echo "  분기 배치 실행 대기 중..."
aws emr wait step-complete \
    --cluster-id "${CLUSTER_ID}" \
    --step-id "${STEP3_ID}"
echo "  분기 배치 완료"

# Step 4: 성능 최적화 벤치마크
echo ""
echo "[Step 4/4] 성능 벤치마크 제출..."
STEP4_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "Type=Spark,Name=PerformanceBenchmark,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://${S3_BUCKET}/scripts/performance_optimizer.py,--input,s3://${S3_BUCKET}/input/card_transactions.csv,--output,s3://${S3_BUCKET}/output/optimized]" \
    --query "StepIds[0]" \
    --output text)
echo "  Step ID: ${STEP4_ID}"

echo "  벤치마크 실행 대기 중..."
aws emr wait step-complete \
    --cluster-id "${CLUSTER_ID}" \
    --step-id "${STEP4_ID}"
echo "  벤치마크 완료"

# 결과 확인
echo ""
echo "=============================================="
echo "전체 Spark Job 완료"
echo "=============================================="
echo ""
echo "S3 출력 결과:"
aws s3 ls "s3://${S3_BUCKET}/output/" --recursive | head -30

echo ""
echo "Step 상태 확인:"
aws emr list-steps \
    --cluster-id "${CLUSTER_ID}" \
    --query "Steps[*].{Name:Name,Status:Status.State}" \
    --output table
