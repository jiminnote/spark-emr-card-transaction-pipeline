"""
카드 거래 데이터 생성기

현대카드 실무 거래 패턴을 시뮬레이션하여 대용량 테스트 데이터를 생성합니다.
- 거래일시, 카드번호(마스킹), 가맹점, 금액, 카테고리, 할부, 승인상태 등
- 기본 100만 건, 최대 500만 건까지 생성 가능
"""

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timedelta


# -- 가맹점 및 카테고리 매핑 (실제 카드사 분류 체계 반영) --

MERCHANTS_BY_CATEGORY = {
    "식비": [
        ("GS25", "편의점"),
        ("CU", "편의점"),
        ("세븐일레븐", "편의점"),
        ("스타벅스", "카페"),
        ("이디야커피", "카페"),
        ("투썸플레이스", "카페"),
        ("맥도날드", "패스트푸드"),
        ("BBQ치킨", "배달"),
        ("배달의민족", "배달"),
        ("요기요", "배달"),
        ("한솥도시락", "식당"),
        ("김밥천국", "식당"),
    ],
    "교통": [
        ("카카오택시", "택시"),
        ("서울교통공사", "지하철"),
        ("GS칼텍스", "주유"),
        ("SK에너지", "주유"),
        ("현대오일뱅크", "주유"),
        ("하이패스", "톨게이트"),
        ("티머니", "대중교통"),
        ("쏘카", "카셰어링"),
    ],
    "쇼핑": [
        ("쿠팡", "온라인"),
        ("11번가", "온라인"),
        ("네이버쇼핑", "온라인"),
        ("SSG닷컴", "온라인"),
        ("이마트", "대형마트"),
        ("홈플러스", "대형마트"),
        ("롯데마트", "대형마트"),
        ("다이소", "생활용품"),
        ("올리브영", "뷰티"),
        ("무신사", "패션"),
    ],
    "문화": [
        ("CGV", "영화"),
        ("메가박스", "영화"),
        ("롯데시네마", "영화"),
        ("교보문고", "서적"),
        ("YES24", "서적"),
        ("멜론", "음악"),
        ("넷플릭스", "OTT"),
        ("왓챠", "OTT"),
        ("디즈니플러스", "OTT"),
    ],
    "의료": [
        ("서울대병원", "종합병원"),
        ("세브란스병원", "종합병원"),
        ("연세의원", "의원"),
        ("올리브약국", "약국"),
        ("온누리약국", "약국"),
    ],
    "교육": [
        ("메가스터디", "학원"),
        ("해커스", "학원"),
        ("클래스101", "온라인강의"),
        ("인프런", "온라인강의"),
        ("유데미", "온라인강의"),
    ],
    "통신": [
        ("SKT", "통신비"),
        ("KT", "통신비"),
        ("LGU+", "통신비"),
        ("네이버클라우드", "클라우드"),
    ],
    "보험": [
        ("삼성화재", "보험료"),
        ("현대해상", "보험료"),
        ("DB손해보험", "보험료"),
    ],
}

# 카테고리별 금액 범위 (원)
AMOUNT_RANGES = {
    "식비": (1500, 80000),
    "교통": (1250, 150000),
    "쇼핑": (3000, 500000),
    "문화": (5000, 50000),
    "의료": (5000, 300000),
    "교육": (10000, 500000),
    "통신": (10000, 100000),
    "보험": (30000, 300000),
}

# 카테고리별 거래 비중 (가중치)
CATEGORY_WEIGHTS = {
    "식비": 35,
    "교통": 15,
    "쇼핑": 25,
    "문화": 8,
    "의료": 5,
    "교육": 5,
    "통신": 4,
    "보험": 3,
}

# 승인 상태
APPROVAL_STATUSES = [
    ("approved", 95),   # 승인 95%
    ("declined", 3),    # 거절 3%
    ("cancelled", 2),   # 취소 2%
]

# 할부 개월 (0=일시불)
INSTALLMENT_OPTIONS = [0, 0, 0, 0, 0, 0, 2, 3, 6, 12]

# 카드 종류
CARD_TYPES = ["신용", "체크"]
CARD_TYPE_WEIGHTS = [60, 40]


def generate_card_number():
    """마스킹된 카드 번호 생성"""
    last_four = random.randint(1000, 9999)
    return f"****-****-****-{last_four}"


def generate_transaction_date(start_date, end_date):
    """거래 일시 생성 (업무 시간대 가중치 적용)"""
    delta = end_date - start_date
    random_day = start_date + timedelta(days=random.randint(0, delta.days))

    # 시간대별 가중치 (실제 카드 사용 패턴)
    hour_weights = {
        0: 2, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1,
        6: 3, 7: 5, 8: 8, 9: 7, 10: 6, 11: 8,
        12: 15, 13: 12, 14: 8, 15: 7, 16: 6, 17: 8,
        18: 12, 19: 15, 20: 12, 21: 10, 22: 8, 23: 5,
    }
    hours = list(hour_weights.keys())
    weights = list(hour_weights.values())
    hour = random.choices(hours, weights=weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return random_day.replace(hour=hour, minute=minute, second=second)


def generate_amount(category):
    """카테고리별 현실적인 금액 생성 (정규분포 기반)"""
    min_amount, max_amount = AMOUNT_RANGES[category]
    mean = (min_amount + max_amount) / 3  # 평균은 낮은 쪽으로 치우침
    std = (max_amount - min_amount) / 4

    amount = int(random.gauss(mean, std))
    amount = max(min_amount, min(max_amount, amount))

    # 100원 단위로 반올림
    amount = round(amount / 100) * 100
    return amount


def get_approval_status():
    """승인 상태 결정"""
    statuses = [s[0] for s in APPROVAL_STATUSES]
    weights = [s[1] for s in APPROVAL_STATUSES]
    return random.choices(statuses, weights=weights, k=1)[0]


def generate_transactions(num_records, start_date, end_date, num_customers=10000):
    """카드 거래 데이터 생성"""
    # 고객별 카드 번호 사전 생성
    customer_cards = {}
    for i in range(num_customers):
        card_no = generate_card_number()
        card_type = random.choices(CARD_TYPES, weights=CARD_TYPE_WEIGHTS, k=1)[0]
        customer_cards[i] = (card_no, card_type)

    # 카테고리 가중치 준비
    categories = list(CATEGORY_WEIGHTS.keys())
    weights = list(CATEGORY_WEIGHTS.values())

    transactions = []

    for txn_id in range(1, num_records + 1):
        # 고객 선택
        customer_id = random.randint(0, num_customers - 1)
        card_no, card_type = customer_cards[customer_id]

        # 카테고리 선택 (가중치 기반)
        category = random.choices(categories, weights=weights, k=1)[0]

        # 가맹점 선택
        merchant_info = random.choice(MERCHANTS_BY_CATEGORY[category])
        merchant_name = merchant_info[0]
        sub_category = merchant_info[1]

        # 거래 정보 생성
        txn_date = generate_transaction_date(start_date, end_date)
        amount = generate_amount(category)
        status = get_approval_status()

        # 할부 (5만원 이상 신용카드만)
        if card_type == "신용" and amount >= 50000:
            installment = random.choice(INSTALLMENT_OPTIONS)
        else:
            installment = 0

        # 지역 (주요 도시)
        regions = ["서울", "경기", "인천", "부산", "대구", "대전", "광주", "제주"]
        region_weights = [35, 25, 10, 8, 6, 5, 4, 2]
        region = random.choices(regions, weights=region_weights, k=1)[0]

        transaction = {
            "transaction_id": f"TXN{txn_id:010d}",
            "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "card_no": card_no,
            "card_type": card_type,
            "merchant": merchant_name,
            "sub_category": sub_category,
            "category": category,
            "amount": amount,
            "installment_months": installment,
            "approval_status": status,
            "region": region,
        }
        transactions.append(transaction)

        # 진행률 표시
        if txn_id % 100000 == 0:
            progress = (txn_id / num_records) * 100
            print(f"  생성 진행: {txn_id:,} / {num_records:,} ({progress:.1f}%)")

    return transactions


def save_to_csv(transactions, output_path):
    """CSV 파일로 저장"""
    if not transactions:
        print("저장할 데이터가 없습니다.")
        return

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    fieldnames = transactions[0].keys()
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)

    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\n저장 완료: {output_path}")
    print(f"  레코드 수: {len(transactions):,}")
    print(f"  파일 크기: {file_size:.1f} MB")


def print_summary(transactions):
    """데이터 요약 출력"""
    print("\n=== 생성 데이터 요약 ===")
    print(f"총 거래 건수: {len(transactions):,}")

    # 카테고리별 집계
    category_counts = {}
    category_amounts = {}
    for txn in transactions:
        cat = txn["category"]
        category_counts[cat] = category_counts.get(cat, 0) + 1
        category_amounts[cat] = category_amounts.get(cat, 0) + txn["amount"]

    print("\n카테고리별 분포:")
    print(f"  {'카테고리':<8} {'건수':>10} {'비율':>8} {'총액':>15}")
    print(f"  {'-'*45}")
    for cat in sorted(category_counts, key=category_counts.get, reverse=True):
        count = category_counts[cat]
        ratio = count / len(transactions) * 100
        total = category_amounts[cat]
        print(f"  {cat:<8} {count:>10,} {ratio:>7.1f}% {total:>14,}원")

    # 승인 상태별 집계
    status_counts = {}
    for txn in transactions:
        st = txn["approval_status"]
        status_counts[st] = status_counts.get(st, 0) + 1

    print("\n승인 상태 분포:")
    for st, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        ratio = cnt / len(transactions) * 100
        print(f"  {st:<12} {cnt:>10,} ({ratio:.1f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="카드 거래 데이터 생성기 - 현대카드 실무 패턴 시뮬레이션"
    )
    parser.add_argument(
        "--records", type=int, default=1000000,
        help="생성할 거래 건수 (기본: 1,000,000)"
    )
    parser.add_argument(
        "--output", type=str, default="data/card_transactions.csv",
        help="출력 파일 경로 (기본: data/card_transactions.csv)"
    )
    parser.add_argument(
        "--start-date", type=str, default="2024-01-01",
        help="거래 시작일 (기본: 2024-01-01)"
    )
    parser.add_argument(
        "--end-date", type=str, default="2024-12-31",
        help="거래 종료일 (기본: 2024-12-31)"
    )
    parser.add_argument(
        "--customers", type=int, default=10000,
        help="고객 수 (기본: 10,000)"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="랜덤 시드 (기본: 42)"
    )
    args = parser.parse_args()

    random.seed(args.seed)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    print("=" * 60)
    print("카드 거래 데이터 생성기")
    print("=" * 60)
    print(f"  생성 건수: {args.records:,}")
    print(f"  기간: {args.start_date} ~ {args.end_date}")
    print(f"  고객 수: {args.customers:,}")
    print(f"  출력 파일: {args.output}")
    print(f"  랜덤 시드: {args.seed}")
    print("=" * 60)
    print()

    print("데이터 생성 중...")
    transactions = generate_transactions(
        num_records=args.records,
        start_date=start_date,
        end_date=end_date,
        num_customers=args.customers,
    )

    print_summary(transactions)
    save_to_csv(transactions, args.output)


if __name__ == "__main__":
    main()
