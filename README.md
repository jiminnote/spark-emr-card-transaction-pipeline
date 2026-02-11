# 카드 거래 데이터 배치 처리 파이프라인 (Spark + AWS EMR)

## 프로젝트 개요
금융권 카드사의 분기별 정기 배치 처리를 Spark + AWS EMR으로 재구현한 학습 프로젝트입니다

## 역량 강화 목표 
본 프로젝트는 기존 RDBMS 중심의 분석 경험을 넘어, 대용량 분산 처리 아키텍처(Hadoop/Spark Eco)에 대한 기술적 숙련도를 확보하기 위해 기획되었습니다.

---

## 핵심 기능

- **PySpark ETL**: 100만 건 카드 거래 데이터 처리 (CSV → Parquet)
- **AWS EMR**: 클러스터 구성 및 Spark Job 제출
- **Airflow DAG**: 4개 Spark Job 스케줄링 (품질검증 → ETL → 분기배치 → 벤치마크)
- **Hive 카탈로그**: Parquet → External Table, Impala/Athena 호환
- **LocalStack**: AWS 계정 없이 S3 로컬 테스트

## 기술 스택

| 구분 | 기술 |
|-----|------|
| 분산 처리 | Apache Spark 4.1 (PySpark) |
| 클라우드 | AWS EMR 6.15, S3 |
| 워크플로우 | Apache Airflow |
| 데이터 카탈로그 | Hive, Impala |
| 로컬 테스트 | LocalStack (Docker) |

---

## 아키텍처

<img width="2368" height="1792" alt="Gemini_Generated_Image_p8k4nhp8k4nhp8k4" src="https://github.com/user-attachments/assets/57ab4dbe-2fa9-4777-b250-d293c835c431" />

- **LocalStack**: AWS 계정 없이 S3를 로컬에서 에뮬레이션 (Docker)
- **Spark**: `s3a://` 프로토콜로 S3 버킷에서 직접 읽기/쓰기
- **hadoop-aws**: Spark ↔ S3 연동을 위한 Hadoop AWS 커넥터
- **Airflow**: 4개 Spark Job을 DAG로 스케줄링 (품질검증 → ETL → 분기배치 → 벤치마크)
- **Hive**: Parquet 출력을 External Table로 등록 → Impala / Athena에서 SQL 조회

---
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

## 실행 방법

### 1. 데이터 생성

```bash
python data/generate_data.py --records 1000000 --output data/card_transactions.csv
```

### 2. 로컬 Spark 실행

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

### 3. S3 연동 실행 (LocalStack - AWS 계정 불필요)

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

---

## 배운 점

- Oracle/MySQL 기반 배치를 운영하며 느꼈던 성능 한계를 Spark의 분산 처리로 극복할 수 있었습니다.
- EMR 클러스터 구성부터 Job 제출, 모니터링까지 클라우드 기반 데이터 처리 전체 라이프사이클을 경험했습니다.
- Parquet 포맷과 파티셔닝 전략이 실무에서 얼마나 큰 성능 차이를 만드는지 직접 확인할 수 있었습니다.
- Spark 4.x의 ANSI 모드, 타입 추론 변경 등 버전 간 호환성 이슈를 해결해봤습니다.
- **Airflow DAG**로 Spark Job 간 의존성과 재시도 정책을 정의하여, 운영 환경에서의 워크플로우 관리 방법을 학습했습니다.
- **Hive External Table**로 Parquet 데이터를 등록하고, Impala/Athena에서 SQL로 즉시 조회 가능한 데이터 카탈로그를 구성했습니다.

