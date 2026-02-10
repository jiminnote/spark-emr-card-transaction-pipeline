#!/bin/bash
# =============================================================================
# S3 업로드 스크립트
#
# 데이터 파일, Spark 스크립트, 부트스트랩 스크립트를 S3에 업로드합니다.
#
# 사용법:
#   bash deploy/upload_to_s3.sh [bucket-name]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# S3 버킷 이름 (인자 또는 기본값)
S3_BUCKET="${1:-your-card-etl-project}"

echo "=============================================="
echo "S3 업로드"
echo "  버킷: s3://${S3_BUCKET}"
echo "=============================================="

# S3 버킷 존재 확인, 없으면 생성
if ! aws s3 ls "s3://${S3_BUCKET}" &>/dev/null; then
    echo "S3 버킷 생성 중: ${S3_BUCKET}"
    aws s3 mb "s3://${S3_BUCKET}" --region ap-northeast-2
fi

# 1. 데이터 파일 업로드
echo ""
echo "[1/3] 데이터 파일 업로드..."
if [ -f "${PROJECT_DIR}/data/card_transactions.csv" ]; then
    aws s3 cp \
        "${PROJECT_DIR}/data/card_transactions.csv" \
        "s3://${S3_BUCKET}/input/card_transactions.csv" \
        --storage-class STANDARD
    echo "  업로드 완료: input/card_transactions.csv"
else
    echo "  경고: data/card_transactions.csv 파일이 없습니다."
    echo "  먼저 데이터를 생성하세요: python data/generate_data.py"
fi

# 2. Spark 스크립트 업로드
echo ""
echo "[2/3] Spark 스크립트 업로드..."
for script in "${PROJECT_DIR}"/scripts/*.py; do
    if [ -f "$script" ]; then
        filename=$(basename "$script")
        aws s3 cp "$script" "s3://${S3_BUCKET}/scripts/${filename}"
        echo "  업로드: scripts/${filename}"
    fi
done

# 3. 부트스트랩 스크립트 업로드
echo ""
echo "[3/3] 부트스트랩 스크립트 업로드..."

# 부트스트랩 스크립트 생성 및 업로드
cat > /tmp/install_dependencies.sh << 'EOF'
#!/bin/bash
sudo pip3 install pyyaml boto3
EOF

aws s3 cp /tmp/install_dependencies.sh "s3://${S3_BUCKET}/bootstrap/install_dependencies.sh"
echo "  업로드: bootstrap/install_dependencies.sh"
rm /tmp/install_dependencies.sh

# 업로드 확인
echo ""
echo "=============================================="
echo "S3 업로드 완료. 파일 목록:"
echo "=============================================="
aws s3 ls "s3://${S3_BUCKET}/" --recursive --human-readable | head -20
echo ""
echo "총 파일 수:"
aws s3 ls "s3://${S3_BUCKET}/" --recursive | wc -l
