# 시스템 아키텍처

## 전체 아키텍처 다이어그램

```
+------------------+     +-------------------+     +--------------------+
|   Data Source    |     |   AWS EMR Cluster  |     |   Output Storage   |
|                  |     |                    |     |                    |
|  S3 Input        |---->|  Master (m5.xl)    |---->|  S3 Output         |
|  - CSV 원본 데이터|     |    Spark Driver    |     |  - Parquet 집계    |
|  - 100만+ 건     |     |                    |     |  - 품질 리포트      |
|                  |     |  Core 1 (m5.xl)    |     |  - 벤치마크 결과    |
+------------------+     |    Spark Executor  |     |                    |
                         |                    |     +--------------------+
                         |  Core 2 (m5.xl)    |
                         |    Spark Executor  |
                         +-------------------+
```

## 데이터 처리 흐름

```
[1. 데이터 생성]
    |
    v
generate_data.py
- 100만 건 카드 거래 데이터 생성
- 실제 카드 사용 패턴 시뮬레이션 (시간대 가중치, 카테고리 비중)
- CSV 포맷으로 저장
    |
    v
[2. S3 업로드]
    |
    v
upload_to_s3.sh
- 데이터 파일 -> s3://bucket/input/
- 스크립트 파일 -> s3://bucket/scripts/
    |
    v
[3. EMR 클러스터 생성]
    |
    v
create_emr_cluster.sh
- EMR 6.15 (Spark 3.5, Hadoop 3.3)
- Master 1대 + Core 2대 (m5.xlarge)
- Spot Instance로 비용 절감
    |
    v
[4. Spark Job 실행]
    |
    +---> data_quality_check.py    (품질 검증)
    |         |
    |         v
    |     [품질 PASS?] --- FAIL ---> 중단 및 알림
    |         |
    |        PASS
    |         |
    +---> spark_etl.py             (메인 ETL)
    |     - 일별 카테고리 집계
    |     - 가맹점 매출 랭킹
    |     - 시간대별 패턴 분석
    |     - 지역별 분석
    |     - 월별 트렌드
    |         |
    +---> quarterly_batch.py       (분기 배치)
    |     - Q1~Q4 분기별 집계
    |     - 전분기 대비 증감률 (QoQ)
    |     - 가맹점 분기 TOP 랭킹
    |         |
    +---> performance_optimizer.py (성능 벤치마크)
          - CSV vs Parquet 비교
          - 파티셔닝 전략 비교
          - 캐싱 효과 측정
          - 브로드캐스트 조인 테스트
    |
    v
[5. 결과 저장]
    |
    v
S3 Output Bucket
├── daily_category_summary/     (일별 카테고리 집계, 날짜 파티션)
├── merchant_ranking/           (가맹점 매출 랭킹)
├── hourly_pattern/             (시간대별 패턴)
├── regional_analysis/          (지역별 분석, 지역 파티션)
├── monthly_trend/              (월별 트렌드)
├── partitioned_transactions/   (전체 데이터 연/월 파티션)
├── quarterly/                  (분기별 배치 결과)
├── quality_report/             (품질 검증 리포트)
└── optimized/                  (벤치마크 결과)
    |
    v
[6. 클러스터 종료]
    |
    v
teardown_cluster.sh
- 비용 절감을 위한 즉시 종료
```

## EMR 클러스터 구성

| 노드 | 인스턴스 타입 | 수량 | 역할 |
|------|-------------|------|------|
| Master | m5.xlarge | 1 | Spark Driver, YARN ResourceManager |
| Core | m5.xlarge | 2 | Spark Executor, HDFS DataNode |

### m5.xlarge 스펙
- vCPU: 4
- 메모리: 16 GB
- 스토리지: EBS Only
- 네트워크: Up to 10 Gbps

### Spark 설정
- Executor Memory: 4GB
- Executor Cores: 2
- Driver Memory: 4GB
- Dynamic Allocation: 활성화 (1~10 Executors)
- AQE (Adaptive Query Execution): 활성화
- Shuffle Partitions: 200
- Compression: Snappy

## 데이터 모델

### 입력 데이터 (card_transactions.csv)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| transaction_id | String | 거래 고유 ID (TXN0000000001) |
| transaction_date | String | 거래 일시 (yyyy-MM-dd HH:mm:ss) |
| card_no | String | 카드 번호 마스킹 (****-****-****-1234) |
| card_type | String | 카드 유형 (신용/체크) |
| merchant | String | 가맹점명 |
| sub_category | String | 세부 카테고리 |
| category | String | 대분류 카테고리 |
| amount | Integer | 거래 금액 (원) |
| installment_months | Integer | 할부 개월 (0=일시불) |
| approval_status | String | 승인 상태 (approved/declined/cancelled) |
| region | String | 거래 지역 |

### 출력 데이터 (Parquet)
- 일별 카테고리 집계: txn_date, category, txn_count, total_amount, avg_amount
- 가맹점 랭킹: merchant, category, txn_count, total_revenue, unique_customers
- 시간대별 패턴: txn_hour, txn_count, total_amount, time_slot
- 지역별 분석: region, category, txn_count, total_amount
- 월별 트렌드: txn_year, txn_month, txn_count, total_amount

## 비용 구조

```
EMR 클러스터 (m5.xlarge x 3대 x 2시간)
  - On-Demand: $0.192/시간 x 3대 x 2시간 = $1.15
  - Master: On-Demand = $0.384
  - Core(Spot): ~$0.096/시간 x 2대 x 2시간 = $0.384

S3 스토리지 (~1GB)
  - 저장: $0.023/GB = ~$0.02
  - PUT 요청: ~$0.005

총 예상 비용: ~$1-2 (Spot Instance 활용 시)
```
