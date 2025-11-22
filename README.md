# Databricks Sink Connector

Kafka Connect를 통해 Kafka 토픽의 데이터를 Databricks Delta Lake로 전송하는 Sink Connector입니다.

## 주요 기능

- **Debezium CDC 지원**: PostgreSQL Debezium Source Connector와 연동하여 CDC 데이터 수신
- **자동 테이블 생성**: 첫 메시지의 스키마를 기반으로 staging 테이블 자동 생성
- **배치 INSERT**: 토픽별로 레코드를 그룹화하여 단일 INSERT 문으로 처리
- **독립적 트랜잭션**: 각 테이블별로 독립적인 트랜잭션 처리 (일부 실패 시에도 다른 테이블은 성공적으로 커밋)
- **선택적 Offset Commit**: 성공한 레코드의 Kafka offset만 commit하여 재처리 가능

## 구조

```
databricks/
├── pom.xml                  # Maven 설정
├── src/
│   ├── main/java/
│   │   ├── DeltaLakeSinkConnector.java  # Connector 메인 클래스
│   │   ├── DeltaLakeSinkTask.java        # 실제 데이터 처리 로직
│   │   └── DeltaLakeSinkConfig.java      # 설정 관리
│   └── test/java/          # 테스트 코드
└── target/                  # 빌드 결과물
```

## 빌드

```bash
# 컴파일
mvn compile

# 테스트
mvn test

# 패키지 (JAR 생성)
mvn clean package

# 테스트 생략하고 빌드
mvn clean package -DskipTests
```

## 설정

### 필수 설정

| 설정 | 설명 | 예시 |
|------|------|------|
| `topics` | 처리할 Kafka 토픽 목록 (쉼표로 구분) | `cdc.public.users,cdc.public.orders` |
| `databricks.jdbc.url` | Databricks JDBC URL | `jdbc:databricks://<workspace-url>:443/default;transportMode=http;ssl=1` |
| `databricks.token` | Databricks Personal Access Token | `dapi...` |
| `databricks.catalog` | Databricks Catalog 이름 | `development` |
| `databricks.schema` | Databricks Schema 이름 | `default` |

### 설정 예시

```json
{
  "name": "databricks-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.DeltaLakeSinkConnector",
    "tasks.max": "1",
    "topics": "cdc.public.users,cdc.public.orders",
    "databricks.jdbc.url": "jdbc:databricks://<workspace-url>:443/default;transportMode=http;ssl=1",
    "databricks.token": "dapi...",
    "databricks.catalog": "development",
    "databricks.schema": "default"
  }
}
```

## 동작 방식

1. **초기화**: Connector 시작 시 각 토픽에 대한 테이블 정보 초기화
2. **테이블 생성**: 첫 메시지 수신 시 스키마를 분석하여 staging 테이블 자동 생성
3. **데이터 처리**: 
   - 토픽별로 레코드 그룹화
   - 각 토픽에 대해 단일 INSERT 문 생성 (여러 VALUES 절 포함)
   - 독립적인 JDBC 트랜잭션으로 처리
4. **Offset 관리**: 
   - 성공한 레코드의 offset만 추적
   - `flush()` 호출 시 성공한 offset만 commit

## 테이블 구조

각 토픽은 Databricks에서 다음 형식의 테이블로 매핑됩니다:

```
{catalog}.{schema}.{table_name}
```

예: `cdc.public.users` → `development.default.users`

### Staging 테이블 컬럼

- **데이터 컬럼**: Debezium CDC 메시지의 `after` 필드에서 추출
- **`_operation`**: CDC 작업 타입 (`c`=create, `u`=update, `d`=delete)
- **`_timestamp`**: CDC 이벤트 타임스탬프

## 개발

### 코드 수정 후 재배포

```bash
# 1. 코드 수정
vim src/main/java/org/apache/kafka/connect/file/DeltaLakeSinkTask.java

# 2. 재빌드
mvn clean package -DskipTests

# 3. Kafka Connect 재시작
docker-compose restart kafka-connect
```

### 의존성

- Kafka Connect API 3.6.1
- Databricks JDBC Driver 3.0.3
- SLF4J 2.0.9
- JUnit 5.10.1 (테스트)
- Mockito 5.8.0 (테스트)

## 주의사항

- 각 테이블의 INSERT는 독립적인 트랜잭션으로 처리됩니다
- 하나의 테이블이 실패해도 다른 테이블은 정상적으로 처리됩니다
- 실패한 테이블의 레코드는 Kafka offset이 commit되지 않아 재처리됩니다
- 테이블 이름은 토픽 이름의 마지막 부분을 사용합니다 (예: `cdc.public.users` → `users`)
