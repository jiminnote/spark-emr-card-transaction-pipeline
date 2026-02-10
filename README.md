# 카드 거래 데이터 배치 처리 파이프라인 (Spark + AWS EMR)

## 프로젝트 개요

현대카드 분기별 정기 배치 운영 경험을 바탕으로,
AWS EMR과 Apache Spark를 활용한 대용량 카드 거래 데이터
배치 처리 파이프라인을 구현한 프로젝트입니다.

기존 Oracle/MySQL 기반 배치 처리에서 느꼈던 성능 한계를
Spark 분산 처리로 극복하는 과정을 담았습니다.

---

## 기술 스택

| 구분 | 기술 |
|------|------|
| 분산 처리 | Apache Spark 4.1 (PySpark) |
| 클라우드 | AWS EMR 6.15, S3 |
| S3 에뮬레이션 | LocalStack (Docker) - AWS 계정 불필요 |
| 언어 | Python 3.9+ |
| 데이터 포맷 | CSV (원본), Parquet (변환 후) |
| S3 연동 | hadoop-aws 3.4.2 (s3a:// 프로토콜) |
| 워크플로우 | Apache Airflow (DAG 스케줄링) |
| 데이터 카탈로그 | Hive Metastore (DDL), Impala/Athena 호환 |
| 인프라 | EMR 클러스터 (m5.xlarge x 3) |

---

## 아키텍처

```
┌─────────────────────┐     ┌───────────────────────────┐     ┌─────────────────────┐
│  S3 Input Bucket    │     │  Spark (Local / EMR)      │     │  S3 Output Bucket   │
│  (LocalStack/AWS)   │     │                           │     │  (LocalStack/AWS)   │
│                     │     │  ┌─────────────────────┐  │     │                     │
│  card_transactions  │────▶│  │ Data Quality Check  │  │     │  quality_report/    │
│  .csv               │     │  │ (data_quality_check) │──┼────▶│  daily_summary/     │
│                     │     │  └──────────┬──────────┘  │     │  merchant_ranking/  │
│  scripts/           │     │             │             │     │  hourly_pattern/    │
│  ├── spark_etl.py   │     │  ┌──────────▼──────────┐  │     │  regional_analysis/ │
│  ├── quarterly_     │     │  │ Spark ETL Pipeline  │  │     │  monthly_trend/     │
│  │   batch.py       │     │  │ (spark_etl.py)      │──┼────▶│  partitioned_txn/   │
│  └── ...            │     │  └──────────┬──────────┘  │     │  quarterly/         │
│                     │     │             │             │     │  optimized/         │
└─────────────────────┘     │  ┌──────────▼──────────┐  │     │  benchmark_report/  │
                            │  │ Quarterly Batch     │  │     │                     │
  ┌─────────────────────┐   │  │ (quarterly_batch.py)│──┼────▶│                     │
  │  Docker (LocalStack)│   │  └──────────┬──────────┘  │     └─────────────────────┘
  │  S3 API :4566       │   │             │             │
  │  aws s3 호환         │   │  ┌──────────▼──────────┐  │
  └─────────────────────┘   │  │ Performance Bench   │  │
                            │  │ (performance_opt.)  │──┘
                            │  └─────────────────────┘  │
                            └───────────────────────────┘
```

- **LocalStack**: AWS 계정 없이 S3를 로컬에서 에뮬레이션 (Docker)
- **Spark**: `s3a://` 프로토콜로 S3 버킷에서 직접 읽기/쓰기
- **hadoop-aws**: Spark ↔ S3 연동을 위한 Hadoop AWS 커넥터
- **Airflow**: 4개 Spark Job을 DAG로 스케줄링 (품질검증 → ETL → 분기배치 → 벤치마크)
- **Hive**: Parquet 출력을 External Table로 등록 → Impala / Athena에서 SQL 조회

상세 아키텍처는 [docs/architecture.md](docs/architecture.md) 참고

---

## 프로젝트 목적

> 실무에서 Oracle/MySQL 기반으로 운영하던 카드 거래 배치 처리를  
> **Spark + S3 + Airflow + Hive** 기반 클라우드 아키텍처로 전환하는 과정을 구현한 프로젝트입니다.  
> 배치 로직 자체는 실무 경험에서 이미 다루고 있으며, 이 프로젝트는 **기술 전환 역량**에 초점을 맞춥니다.

## 핵심 기술 포인트

### 1. Spark 분산 처리 + S3 연동 파이프라인
- PySpark로 CSV → Parquet 변환, Snappy 압축 적용
- `s3a://` 프로토콜로 S3 직접 읽기/쓰기 (hadoop-aws 3.4.2)
- LocalStack으로 AWS 계정 없이 S3 연동 검증 → 동일 코드 EMR에서 그대로 동작
- 입력 경로 기반 로컬/S3 모드 자동 감지 (`create_spark_session`)

### 2. Spark 성능 최적화 전략 비교
- CSV vs Parquet 읽기 성능 벤치마크 (95.3% 단축)
- 브로드캐스트 조인 vs 일반 조인 (86.9% 단축)
- 캐싱 적용 전후 비교, 리파티셔닝 전략
- 파티셔닝 키 설계 (txn_year/txn_month, region)

### 3. Airflow DAG 워크플로우 오케스트레이션
- 4개 Spark Job 의존성 기반 순차 실행 (품질검증 → ETL → 분기배치 → 벤치마크)
- `BranchPythonOperator`로 품질 검증 실패 시 파이프라인 자동 중단
- 매일 KST 02:00 스케줄, 실패 시 5분 간격 2회 재시도
- EMR `EmrAddStepsOperator` 전환 가능한 구조로 설계

### 4. Hive 데이터 카탈로그 + Impala / Athena 호환
- Spark ETL 출력 Parquet → Hive External Table 8개 등록
- 파티션 테이블 설계 (`PARTITIONED BY txn_year, txn_month` + `MSCK REPAIR`)
- Impala `INVALIDATE METADATA`, Athena Glue Catalog 호환 DDL
- 분석 쿼리 예시 포함 (일별 TOP 5, 시간대별 비중, 지역별 추이)

### 5. EMR 클러스터 배포 설정
- EMR 6.15 클러스터 구성 (m5.xlarge Master 1 + Core 2 Spot)
- spark-submit `--deploy-mode cluster --master yarn` 실행 설정
- 클러스터 생성 → Job 제출 → 종료까지 자동화 스크립트 구성

---

## 프로젝트 구조

```
spark-emr-card-transaction-pipeline/
├── README.md
├── requirements.txt
├── docker-compose.yml                 # LocalStack (로컬 S3 에뮬레이션)
├── data/
│   └── generate_data.py               # 100만 건 카드 거래 데이터 생성
├── scripts/
│   ├── spark_etl.py                   # 메인 Spark ETL (로컬/S3 자동 감지)
│   ├── data_quality_check.py          # 데이터 품질 검증
│   ├── quarterly_batch.py             # 분기별 배치 처리
│   └── performance_optimizer.py       # 성능 최적화 스크립트
├── config/
│   ├── emr_cluster.json               # EMR 클러스터 설정
│   ├── spark_config.py                # Spark 설정 관리
│   └── app_config.yaml                # 애플리케이션 설정
├── dags/
│   └── card_transaction_dag.py         # Airflow DAG (4개 Spark Job 스케줄링)
├── hive/
│   └── create_tables.sql              # Hive DDL (8개 External Table 정의)
├── deploy/
│   ├── localstack_setup.py            # LocalStack S3 초기화 (버킷 생성/업로드)
│   ├── run_pipeline_s3.sh             # S3 연동 전체 파이프라인 실행
│   ├── verify_s3_output.py            # S3 출력 결과 확인
│   ├── create_emr_cluster.sh          # EMR 클러스터 생성
│   ├── submit_spark_job.sh            # Spark Job 제출
│   ├── upload_to_s3.sh                # S3 업로드
│   └── teardown_cluster.sh            # 클러스터 종료
├── notebooks/
│   └── analysis.ipynb                 # 분석 노트북
└── docs/
    ├── architecture.md                # 아키텍처 문서
    └── performance_report.md          # 성능 리포트
```

---

## 실행 방법

### 1. 사전 준비

```bash
# 프로젝트 클론
git clone https://github.com/your-username/spark-emr-card-transaction-pipeline.git
cd spark-emr-card-transaction-pipeline

# 의존성 설치
pip install -r requirements.txt

# Spark 설치 (Mac)
brew install apache-spark
```

### 2. 데이터 생성

```bash
python data/generate_data.py --records 1000000 --output data/card_transactions.csv
```

### 3. 로컬 Spark 실행

```bash
# 메인 ETL
spark-submit scripts/spark_etl.py --input data/card_transactions.csv --output output/

# 데이터 품질 검증
spark-submit scripts/data_quality_check.py --input data/card_transactions.csv

# 분기별 배치
spark-submit scripts/quarterly_batch.py --input data/card_transactions.csv --output output/quarterly/

# 성능 최적화 테스트
spark-submit scripts/performance_optimizer.py --input data/card_transactions.csv --output output/optimized/
```

### 4. S3 연동 실행 (LocalStack - AWS 계정 불필요)

```bash
# LocalStack 시작 (Docker)
docker compose up -d

# S3 버킷 생성 및 데이터 업로드
python deploy/localstack_setup.py

# S3 연동 전체 파이프라인 실행
bash deploy/run_pipeline_s3.sh

# S3 출력 결과 확인
python deploy/verify_s3_output.py

# 종료
docker compose down
```

Spark가 `s3a://` 프로토콜로 LocalStack S3에서 직접 읽기/쓰기합니다.  
동일한 코드가 실제 AWS S3/EMR에서도 그대로 동작합니다.

### 5. AWS EMR 실행 (실제 AWS 배포)

```bash
# S3에 데이터 및 스크립트 업로드
bash deploy/upload_to_s3.sh

# EMR 클러스터 생성
bash deploy/create_emr_cluster.sh

# Spark Job 제출
bash deploy/submit_spark_job.sh

# 작업 완료 후 클러스터 종료 (비용 절감)
bash deploy/teardown_cluster.sh
```

---

## 성능 결과

### 로컬 실행 (S3 연동, 10만 건)

| 항목 | 결과 |
|------|------|
| 데이터 규모 | 10만 건 카드 거래 (10.9 MB) |
| 스토리지 | S3 (LocalStack) → s3a:// 프로토콜 |
| CSV → Parquet 읽기 개선 | **95.3% 단축** |
| 브로드캐스트 조인 개선 | **86.9% 단축** |
| 캐싱 효과 (2회차) | **55.8% 단축** |
| S3 출력 | 1,107 파일, 19.8 MB (Parquet) |
| 전체 파이프라인 | 약 97초 (4개 Job) |

### EMR 예상 (100만 건)

| 항목 | 결과 |
|------|------|
| 데이터 규모 | 100만 건 카드 거래 |
| EMR 클러스터 | m5.xlarge 3대 (Master 1, Core 2) |
| 처리 시간 (전체 ETL) | 약 2분 |
| Parquet 파티셔닝 적용 | 쿼리 시간 70% 단축 |
| 데이터 압축률 | CSV 대비 75% 절감 |

상세 성능 리포트는 [docs/performance_report.md](docs/performance_report.md) 참고

---

## AWS 비용

```
EMR 클러스터 (m5.xlarge x 3대 x 2시간) = 약 $1.8
S3 스토리지 (1GB)                       = 거의 무료
총 예상 비용: ~$2-3
```

비용 절약 적용사항:
- 작업 후 즉시 클러스터 종료
- Spot Instance 사용 (50% 절감 가능)
- Free Tier S3 활용

---

## 배운 점

- 현대카드 실무에서 Oracle/MySQL 기반 배치를 운영하며 느꼈던 성능 한계를
  Spark의 분산 처리로 극복할 수 있었습니다.
- **LocalStack으로 S3를 로컬에서 에뮬레이션**하여, AWS 계정 없이도
  `s3a://` 프로토콜 기반 Spark ↔ S3 연동을 실제로 구현하고 검증했습니다.
- EMR 클러스터 구성부터 Job 제출, 모니터링까지 클라우드 기반 데이터 처리
  전체 라이프사이클을 경험했습니다.
- Parquet 포맷과 파티셔닝 전략이 실무에서 얼마나 큰 성능 차이를 만드는지
  직접 확인할 수 있었습니다.
- Spark 4.x의 ANSI 모드, 타입 추론 변경 등 버전 간 호환성 이슈를
  해결하며 실무 트러블슈팅 역량을 키웠습니다.
- **Airflow DAG**로 Spark Job 간 의존성과 재시도 정책을 정의하여,
  운영 환경에서의 워크플로우 관리 방법을 학습했습니다.
- **Hive External Table**로 Parquet 데이터를 등록하고,
  Impala/Athena에서 SQL로 즉시 조회 가능한 데이터 카탈로그를 구성했습니다.

---

## 라이선스

MIT License
