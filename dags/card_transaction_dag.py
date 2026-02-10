"""
카드 거래 배치 파이프라인 Airflow DAG

4개의 Spark Job을 순서대로 스케줄링합니다:
  1. 데이터 품질 검증 → PASS 시에만 다음 단계 진행
  2. 메인 ETL 파이프라인 (일별/카테고리/가맹점/시간대/지역/월별 분석)
  3. 분기별 배치 처리 (Q1~Q4 집계 + 전분기 대비 증감률)
  4. 성능 벤치마크 (CSV vs Parquet, 조인/캐싱/파티셔닝 비교)

스케줄: 매일 새벽 2시 (KST) 실행
재시도: 실패 시 5분 간격 2회 재시도
알림:  실패 시 담당자에게 알림

EMR 환경:
  - EmrAddStepsOperator로 Step을 EMR 클러스터에 제출
  - EmrStepSensor로 완료 대기

로컬 환경:
  - SparkSubmitOperator로 로컬 spark-submit 실행
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

# ============================================================
#  EMR 사용 시 아래 import를 활성화
# ============================================================
# from airflow.providers.amazon.aws.operators.emr import (
#     EmrAddStepsOperator,
#     EmrCreateJobFlowOperator,
#     EmrTerminateJobFlowOperator,
# )
# from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


# -- 기본 설정 --
S3_BUCKET = "card-pipeline"
S3_INPUT = f"s3a://{S3_BUCKET}-input/card_transactions.csv"
S3_OUTPUT = f"s3a://{S3_BUCKET}-output"
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.2"

# EMR 클러스터 ID (이미 실행 중인 클러스터 사용 시)
# EMR_CLUSTER_ID = "j-XXXXXXXXXX"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-team@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ============================================================
#  품질 검증 결과 판단 함수
# ============================================================
def check_quality_result(**context):
    """
    데이터 품질 검증 결과를 확인하여 분기 처리.
    exit code 0 = PASS → ETL 진행
    exit code 1 = FAIL → 파이프라인 중단
    """
    ti = context["ti"]
    return_value = ti.xcom_pull(task_ids="data_quality_check")

    # BashOperator는 exit code != 0이면 이미 실패하므로
    # 여기까지 왔으면 PASS
    return "spark_etl"


# ============================================================
#  DAG 정의
# ============================================================
with DAG(
    dag_id="card_transaction_batch_pipeline",
    default_args=default_args,
    description="카드 거래 데이터 일일 배치 파이프라인 (품질검증 → ETL → 분기배치 → 벤치마크)",
    schedule_interval="0 17 * * *",  # UTC 17:00 = KST 02:00 매일 새벽 2시
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["card-transaction", "spark", "etl", "batch"],
) as dag:

    # ========================================
    #  시작
    # ========================================
    start = DummyOperator(task_id="start")

    # ========================================
    #  Step 1: 데이터 품질 검증
    # ========================================
    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=f"""
            spark-submit \
                --packages {SPARK_PACKAGES} \
                scripts/data_quality_check.py \
                    --input {S3_INPUT} \
                    --output {S3_OUTPUT}/quality_report
        """,
        doc="데이터 품질 23개 항목 검증. FAIL 시 exit code 1 반환하여 파이프라인 중단.",
    )

    # 품질 검증 결과에 따라 분기
    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=check_quality_result,
        provide_context=True,
    )

    quality_failed = DummyOperator(task_id="quality_failed")

    # ========================================
    #  Step 2: 메인 Spark ETL
    # ========================================
    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command=f"""
            spark-submit \
                --packages {SPARK_PACKAGES} \
                scripts/spark_etl.py \
                    --input {S3_INPUT} \
                    --output {S3_OUTPUT}
        """,
        doc="6개 분석 테이블 생성: 일별카테고리/가맹점랭킹/시간대패턴/지역분석/월별트렌드/파티션저장",
    )

    # ========================================
    #  Step 3: 분기별 배치 처리
    # ========================================
    quarterly_batch = BashOperator(
        task_id="quarterly_batch",
        bash_command=f"""
            spark-submit \
                --packages {SPARK_PACKAGES} \
                scripts/quarterly_batch.py \
                    --input {S3_INPUT} \
                    --output {S3_OUTPUT}/quarterly \
                    --year {{{{ execution_date.year }}}}
        """,
        doc="분기별 카테고리/가맹점 매출, 전분기 대비 증감률, 카드유형/지역/할부 분석",
    )

    # ========================================
    #  Step 4: 성능 벤치마크 (선택적)
    # ========================================
    performance_benchmark = BashOperator(
        task_id="performance_benchmark",
        bash_command=f"""
            spark-submit \
                --packages {SPARK_PACKAGES} \
                scripts/performance_optimizer.py \
                    --input {S3_INPUT} \
                    --output {S3_OUTPUT}/optimized
        """,
        doc="CSV vs Parquet, 브로드캐스트 조인, 캐싱, 파티셔닝 전략 벤치마크",
    )

    # ========================================
    #  완료 알림
    # ========================================
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "[SUCCESS] 카드 거래 배치 파이프라인 완료: {{ ds }}"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ========================================
    #  의존성 정의 (실행 순서)
    # ========================================
    #
    #  start → quality_check → quality_gate ─┬─→ spark_etl → quarterly → benchmark → notify → end
    #                                        └─→ quality_failed → end
    #
    start >> data_quality_check >> quality_gate
    quality_gate >> spark_etl >> quarterly_batch >> performance_benchmark >> notify_success >> end
    quality_gate >> quality_failed >> end


# ============================================================
#  EMR Step 정의 (참고용)
#  실제 EMR 배포 시 BashOperator → EmrAddStepsOperator로 교체
# ============================================================
EMR_SPARK_STEPS = [
    {
        "Name": "Step 1 - Data Quality Check",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.sql.adaptive.enabled=true",
                f"s3://{S3_BUCKET}-input/scripts/data_quality_check.py",
                "--input", f"s3://{S3_BUCKET}-input/card_transactions.csv",
                "--output", f"s3://{S3_BUCKET}-output/quality_report",
            ],
        },
    },
    {
        "Name": "Step 2 - Spark ETL Pipeline",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.sql.shuffle.partitions=200",
                f"s3://{S3_BUCKET}-input/scripts/spark_etl.py",
                "--input", f"s3://{S3_BUCKET}-input/card_transactions.csv",
                "--output", f"s3://{S3_BUCKET}-output",
            ],
        },
    },
    {
        "Name": "Step 3 - Quarterly Batch",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                f"s3://{S3_BUCKET}-input/scripts/quarterly_batch.py",
                "--input", f"s3://{S3_BUCKET}-input/card_transactions.csv",
                "--output", f"s3://{S3_BUCKET}-output/quarterly",
            ],
        },
    },
    {
        "Name": "Step 4 - Performance Benchmark",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                f"s3://{S3_BUCKET}-input/scripts/performance_optimizer.py",
                "--input", f"s3://{S3_BUCKET}-input/card_transactions.csv",
                "--output", f"s3://{S3_BUCKET}-output/optimized",
            ],
        },
    },
]
