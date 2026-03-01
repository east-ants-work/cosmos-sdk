# Cosmos SDK

Python SDK for Cosmos Runtime — Object Graph 쿼리 및 Dataset CRUD를 단일 인터페이스로 제공합니다.

## Installation

```bash
pip install cosmos-sdk

# Polars DataFrame 지원 포함
pip install "cosmos-sdk[polars]"
```

## Authentication

Connection string 형식으로 인증합니다. 이메일/패스워드에 특수문자(`@`, `#` 등)가 있으면 URL 인코딩합니다.

```
cosmos://email:password@host:port

# 예시: admin@cosmos.local / admin123@
cosmos://admin%40cosmos.local:admin123%40@localhost:3001
```

환경변수로도 설정할 수 있습니다.

```bash
export COSMOS_CONNECTION_STRING="cosmos://admin%40cosmos.local:admin123%40@localhost:3001"
export COSMOS_GRAPH_KEY="my_graph"
```

## Dataset CRUD

Internal Dataset을 외부 스크립트(ETL, cron job 등)에서 직접 읽고 쓸 수 있습니다.

`graph=` 파라미터를 주면 dataset key 대신 **name**으로도 접근할 수 있습니다.

```python
import polars as pl
from cosmos_sdk import CosmosClient

client = CosmosClient(
    "cosmos://admin%40cosmos.local:admin123%40@localhost:3001",
    graph="my_graph",
)

# dataset key 또는 name 모두 사용 가능
df = await client.datasets.get_dataframe("customer_orders")       # name
df = await client.datasets.get_dataframe("dataset_3be80ce71bb7")  # key

await client.close()
```

### 데이터 읽기

```python
# 전체 로드 (페이지네이션 자동 처리)
df = await client.datasets.get_dataframe("customer_orders")

# 필터 조건 지정
df = await client.datasets.get_dataframe(
    "customer_orders",
    filters=[{"field": "status", "op": "eq", "value": "active"}],
    select=["id", "customer_name", "total_amount"],
)
```

### 데이터 쓰기

```python
new_df = pl.DataFrame({
    "id": [1, 2, 3],
    "customer_name": ["Alice", "Bob", "Charlie"],
    "total_amount": [1000.0, 2000.0, 3000.0],
})

# 전체 덮어쓰기 (기존 데이터 삭제 후 새 데이터로 대체)
result = await client.datasets.overwrite_table("customer_orders", new_df)
print(f"inserted: {result.rows_inserted}")

# 행 추가
result = await client.datasets.append_to_table("customer_orders", extra_df)

# 조건부 업데이트
result = await client.datasets.update_rows(
    "customer_orders",
    filters=[{"field": "status", "op": "eq", "value": "pending"}],
    updates={"status": "processed"},
)
print(f"updated: {result.rows_updated}")

# 조건부 삭제
result = await client.datasets.delete_rows(
    "customer_orders",
    filters=[{"field": "status", "op": "eq", "value": "cancelled"}],
)
```

### Filter 연산자

| op | 설명 |
|----|------|
| `eq` | 같음 |
| `ne` | 다름 |
| `gt` / `gte` | 초과 / 이상 |
| `lt` / `lte` | 미만 / 이하 |
| `in` / `not_in` | 목록 포함 / 미포함 |
| `is_null` / `is_not_null` | null 여부 |

## Object Graph 쿼리

```python
from cosmos_sdk import CosmosClient

client = CosmosClient(
    "cosmos://admin%40cosmos.local:admin123%40@localhost:3001",
    graph="my_graph",
)

# 필터 쿼리
customers = await client.objects.Customer.where(
    Customer.status == "active"
).list()

# DataFrame 변환
df = await client.objects.Customer.where(
    Customer.tier == "Gold"
).to_dataframe()

# 관계 탐색
orders = await client.objects.Order.where(
    Order.status == "completed"
).search_around("customer").list()
```

## codegen

Object/Link 타입 정의에서 Python 클래스를 자동 생성합니다.

```bash
# 설치
pip install "cosmos-sdk[polars]"

# Object 클래스 생성
cosmos-codegen generate \
    --connection "cosmos://admin%40cosmos.local:admin123%40@localhost:3001" \
    --graph my_graph \
    --output ./cosmos_objects

# 타입 목록 확인
cosmos-codegen list \
    --connection "cosmos://..." \
    --graph my_graph
```

생성된 클래스는 `CosmosClient` 싱글톤을 통해 동작합니다. analytics 코드와 동일한 패턴으로 클래스를 직접 임포트해서 사용합니다.

```python
import sys
sys.path.insert(0, ".")
from cosmos_objects import Customer, Order

from cosmos_sdk import CosmosClient

# 싱글톤 초기화
CosmosClient("cosmos://admin%40cosmos.local:admin123%40@localhost:3001", graph="my_graph")

# 클래스 메서드 직접 사용
customers = await Customer.where(Customer.tier == "Gold").list()
df = await Order.where(Order.status == "pending").to_dataframe()

# 관계 탐색
vip_orders = await Order.where(
    Order.status == "completed"
).search_around("customer").list()
```
