#!/bin/bash
# =============================================================================
# EMR 클러스터 종료 스크립트
#
# 비용 절감을 위해 작업 완료 후 반드시 클러스터를 종료합니다.
#
# 사용법:
#   bash deploy/teardown_cluster.sh [cluster-id]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# 클러스터 ID
if [ -f "${PROJECT_DIR}/.cluster_id" ]; then
    CLUSTER_ID="${1:-$(cat "${PROJECT_DIR}/.cluster_id")}"
else
    CLUSTER_ID="${1:?오류: 클러스터 ID를 지정하세요.}"
fi

echo "=============================================="
echo "EMR 클러스터 종료"
echo "  Cluster: ${CLUSTER_ID}"
echo "=============================================="

# 실행 중인 Step 확인
RUNNING_STEPS=$(aws emr list-steps \
    --cluster-id "${CLUSTER_ID}" \
    --step-states RUNNING PENDING \
    --query "Steps[*].Name" \
    --output text 2>/dev/null || true)

if [ -n "$RUNNING_STEPS" ]; then
    echo ""
    echo "경고: 실행 중인 Step이 있습니다:"
    echo "  ${RUNNING_STEPS}"
    echo ""
    read -p "정말 클러스터를 종료하시겠습니까? (y/N): " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        echo "종료 취소"
        exit 0
    fi
fi

# 클러스터 종료
echo ""
echo "클러스터 종료 중..."
aws emr terminate-clusters --cluster-ids "${CLUSTER_ID}"

echo "클러스터 종료 요청 완료"

# 종료 대기
echo "종료 완료 대기 중..."
aws emr wait cluster-terminated --cluster-id "${CLUSTER_ID}"

echo ""
echo "=============================================="
echo "클러스터 종료 완료: ${CLUSTER_ID}"
echo "=============================================="

# .cluster_id 파일 삭제
if [ -f "${PROJECT_DIR}/.cluster_id" ]; then
    rm "${PROJECT_DIR}/.cluster_id"
    echo ".cluster_id 파일 삭제 완료"
fi
