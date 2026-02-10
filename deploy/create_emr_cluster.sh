#!/bin/bash
# =============================================================================
# EMR 클러스터 생성 스크립트
#
# 사전 요구사항:
#   1. AWS CLI 설치 및 설정 (aws configure)
#   2. IAM 역할 생성 완료 (EMR_DefaultRole, EMR_EC2_DefaultRole)
#   3. EC2 Key Pair 생성 완료
#   4. config/emr_cluster.json 설정 완료
#
# 사용법:
#   bash deploy/create_emr_cluster.sh
# =============================================================================

set -euo pipefail

# 설정
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="${PROJECT_DIR}/config/emr_cluster.json"

echo "=============================================="
echo "EMR 클러스터 생성"
echo "=============================================="

# AWS CLI 확인
if ! command -v aws &> /dev/null; then
    echo "오류: AWS CLI가 설치되어 있지 않습니다."
    echo "설치: brew install awscli  또는  pip install awscli"
    exit 1
fi

# AWS 인증 확인
echo "AWS 계정 확인 중..."
AWS_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
if [ -z "$AWS_ACCOUNT" ]; then
    echo "오류: AWS 인증이 설정되지 않았습니다."
    echo "실행: aws configure"
    exit 1
fi
echo "  AWS 계정: ${AWS_ACCOUNT}"

# 설정 파일 확인
if [ ! -f "$CONFIG_FILE" ]; then
    echo "오류: 설정 파일을 찾을 수 없습니다: ${CONFIG_FILE}"
    exit 1
fi

# IAM 역할 생성 (없는 경우)
echo ""
echo "IAM 역할 확인 중..."
if ! aws iam get-role --role-name EMR_DefaultRole &>/dev/null; then
    echo "  EMR 기본 역할 생성 중..."
    aws emr create-default-roles
else
    echo "  EMR 기본 역할 확인 완료"
fi

# 클러스터 생성
echo ""
echo "EMR 클러스터 생성 중..."
CLUSTER_ID=$(aws emr create-cluster \
    --cli-input-json "file://${CONFIG_FILE}" \
    --query "ClusterId" \
    --output text)

echo ""
echo "=============================================="
echo "클러스터 생성 완료"
echo "  Cluster ID: ${CLUSTER_ID}"
echo "=============================================="

# 클러스터 상태 대기
echo ""
echo "클러스터 준비 대기 중... (약 5-10분 소요)"
aws emr wait cluster-running --cluster-id "${CLUSTER_ID}"
echo "클러스터 준비 완료"

# 클러스터 정보 출력
echo ""
echo "클러스터 정보:"
aws emr describe-cluster \
    --cluster-id "${CLUSTER_ID}" \
    --query "Cluster.{Name:Name,Status:Status.State,MasterDns:MasterPublicDnsName}" \
    --output table

# 클러스터 ID 저장 (다른 스크립트에서 사용)
echo "${CLUSTER_ID}" > "${PROJECT_DIR}/.cluster_id"
echo ""
echo "클러스터 ID가 .cluster_id 파일에 저장되었습니다."
echo "Spark Job 제출: bash deploy/submit_spark_job.sh"
