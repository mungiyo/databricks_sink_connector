# Kafka Connector 모듈

Delta Lake Sink Connector 소스 코드 및 빌드를 관리하는 모듈입니다.

## 구조

```
kafka-connect/connectors/databricks/
├── pom.xml                  # Maven 설정
├── src/
│   ├── main/java/          # 소스 코드
│   │   ├── DeltaLakeSinkConnector.java
│   │   ├── DeltaLakeSinkTask.java
│   │   └── DeltaLakeSinkConfig.java
│   └── test/java/          # 테스트 코드
│       ├── DeltaLakeSinkConnectorTest.java
│       ├── DeltaLakeSinkTaskTest.java
│       └── DeltaLakeSinkConfigTest.java
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

## 테스트

```bash
# 단위 테스트 실행
mvn test

# 특정 테스트만 실행
mvn test -Dtest=DeltaLakeSinkTaskTest
```

테스트 결과: **15개 테스트 모두 통과** ✅

## Connector 설정

### 필수 설정

| 설정 | 설명 | 예시 |
|------|------|------|
| `delta.table.path` | Delta Lake 테이블 경로 | `/tmp/delta-table` |

### 선택 설정

| 설정 | 설명 | 기본값 |
|------|------|--------|
| `flush.size` | Flush할 레코드 수 | 1000 |
| `partition.column` | 파티션 컬럼 이름 | "" |

## 개발

### 코드 수정 후 재배포

```bash
# 1. 코드 수정
vim src/main/java/org/apache/kafka/connect/file/DeltaLakeSinkTask.java

# 2. 재빌드
mvn clean package -DskipTests

# 3. Kafka Connect 재시작
cd ../../..
docker-compose -f docker-compose.yml restart kafka-connect
```

### 의존성

- Kafka Connect API 3.6.1
- Delta Lake Core 2.4.0
- SLF4J 2.0.9
- JUnit 5.10.1 (테스트)
- Mockito 5.8.0 (테스트)

## 다음 개발 단계

- [ ] Delta Lake 실제 쓰기 구현
- [ ] 스키마 변환 로직
- [ ] 에러 핸들링 강화
- [ ] 파티셔닝 지원
- [ ] 배치 쓰기 최적화
- [ ] 메트릭 및 모니터링

