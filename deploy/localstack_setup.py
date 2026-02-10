"""
LocalStack S3 초기화 스크립트

S3 버킷 생성 및 카드 거래 데이터 업로드를 수행합니다.
AWS CLI 없이 boto3만으로 동작합니다.

사용법:
  python deploy/localstack_setup.py
"""

import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

# -- 설정 --
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
BUCKET_INPUT = "card-pipeline-input"
BUCKET_OUTPUT = "card-pipeline-output"


def wait_for_localstack(endpoint, max_retries=30):
    """LocalStack 준비 대기"""
    import urllib.request
    import urllib.error

    print(f"LocalStack 준비 대기 중 ({endpoint})...")
    for i in range(max_retries):
        try:
            req = urllib.request.Request(f"{endpoint}/_localstack/health")
            resp = urllib.request.urlopen(req, timeout=2)
            if resp.status == 200:
                print(f"  ✅ LocalStack 준비 완료 ({i + 1}회 시도)")
                return True
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(1)
        if (i + 1) % 5 == 0:
            print(f"  ... {i + 1}초 경과")

    print("  ❌ LocalStack 연결 실패")
    return False


def create_s3_client():
    """LocalStack S3 클라이언트 생성"""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )


def create_buckets(s3):
    """S3 버킷 생성"""
    print("\n[1/3] S3 버킷 생성")
    for bucket in [BUCKET_INPUT, BUCKET_OUTPUT]:
        try:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
            print(f"  ✅ 버킷 생성 완료: s3://{bucket}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
                print(f"  ℹ️  버킷 이미 존재: s3://{bucket}")
            else:
                raise


def upload_data(s3):
    """카드 거래 데이터 업로드"""
    print("\n[2/3] 데이터 업로드")

    data_file = "data/card_transactions.csv"
    if not os.path.exists(data_file):
        print(f"  ❌ {data_file} 파일이 없습니다.")
        print("     먼저 데이터를 생성하세요: python data/generate_data.py")
        sys.exit(1)

    file_size = os.path.getsize(data_file) / 1024 / 1024
    print(f"  📤 업로드: {data_file} ({file_size:.1f} MB)")
    s3.upload_file(data_file, BUCKET_INPUT, "card_transactions.csv")
    print(f"  ✅ s3://{BUCKET_INPUT}/card_transactions.csv 업로드 완료")

    # 스크립트 업로드 (EMR 배포 시뮬레이션)
    scripts_dir = "scripts"
    for script in sorted(os.listdir(scripts_dir)):
        if script.endswith(".py"):
            s3.upload_file(
                f"{scripts_dir}/{script}",
                BUCKET_INPUT,
                f"scripts/{script}",
            )
            print(f"  📤 s3://{BUCKET_INPUT}/scripts/{script}")


def verify_uploads(s3):
    """업로드 확인"""
    print("\n[3/3] S3 버킷 내용 확인")

    for bucket in [BUCKET_INPUT, BUCKET_OUTPUT]:
        print(f"\n  --- s3://{bucket}/ ---")
        try:
            response = s3.list_objects_v2(Bucket=bucket)
            if "Contents" in response:
                total_size = 0
                for obj in response["Contents"]:
                    size_str = f"{obj['Size'] / 1024 / 1024:.2f} MB" if obj["Size"] > 1024 * 1024 else f"{obj['Size'] / 1024:.1f} KB"
                    print(f"    {obj['Key']:45s} {size_str}")
                    total_size += obj["Size"]
                print(f"    {'합계':45s} {total_size / 1024 / 1024:.2f} MB")
            else:
                print("    (비어있음)")
        except ClientError:
            print("    (버킷 없음)")


def main():
    print("=" * 60)
    print("  LocalStack S3 초기화")
    print("=" * 60)

    # LocalStack 준비 대기
    if not wait_for_localstack(S3_ENDPOINT):
        sys.exit(1)

    s3 = create_s3_client()

    create_buckets(s3)
    upload_data(s3)
    verify_uploads(s3)

    print(f"\n{'=' * 60}")
    print("  ✅ LocalStack S3 초기화 완료!")
    print(f"{'=' * 60}")
    print(f"  Input  : s3a://{BUCKET_INPUT}/card_transactions.csv")
    print(f"  Output : s3a://{BUCKET_OUTPUT}/")
    print(f"  Endpoint: {S3_ENDPOINT}")
    print()


if __name__ == "__main__":
    main()
