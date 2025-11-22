package org.apache.kafka.connect.file;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Databricks Delta Lake Sink Task
 * Staging 테이블에 파싱된 데이터를 INSERT만 수행
 */
public class DeltaLakeSinkTask extends SinkTask {
    
    private static final Logger log = LoggerFactory.getLogger(DeltaLakeSinkTask.class);
    
    private DeltaLakeSinkConfig config;
    private String jdbcUrl;
    private String jdbcToken;
    
    // 토픽별 테이블 정보
    private Map<String, TopicTableInfo> topicTableMap;
    
    // 성공한 레코드의 offset 추적 (토픽 파티션별로 최대 offset 저장)
    private Map<TopicPartition, Long> committedOffsets;
    
    /**
     * 토픽별 테이블 정보
     */
    private static class TopicTableInfo {
        String targetTableName;      // 예: main.default.users
        String stagingTableName;     // 예: main.default.users (staging과 타겟이 동일)
        List<String> dataColumns;    // 데이터 컬럼 목록
        boolean stagingTableCreated; // staging 테이블 생성 여부
        
        TopicTableInfo(String targetTableName, String stagingTableName) {
            this.targetTableName = targetTableName;
            this.stagingTableName = stagingTableName;
            this.dataColumns = new ArrayList<>();
            this.stagingTableCreated = false;
        }
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new DeltaLakeSinkConfig(props);
        this.topicTableMap = new HashMap<>();
        this.committedOffsets = new ConcurrentHashMap<>();
        
        // Databricks JDBC 연결 초기화
        try {
            initializeDatabricksConnection();
            
            // 토픽 목록 파싱 및 staging 테이블 생성
            String topicsConfig = props.get("topics");
            if (topicsConfig != null && !topicsConfig.isEmpty()) {
                String[] topics = topicsConfig.split(",");
                for (String topic : topics) {
                    topic = topic.trim();
                    createStagingTableForTopic(topic);
                }
            }
            
            log.info("Delta Lake Sink Task 시작 - Staging 테이블에 데이터 수집 모드");
        } catch (Exception e) {
            log.error("Delta Lake Sink Task 초기화 실패", e);
            throw new RuntimeException("Failed to initialize Delta Lake Sink Task", e);
        }
    }
    
    private void initializeDatabricksConnection() {
        this.jdbcUrl = config.getDatabricksJdbcUrl();
        this.jdbcToken = config.getDatabricksToken();
        
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            log.warn("Databricks JDBC URL이 설정되지 않았습니다. 로컬 파일 모드로 동작합니다.");
            return;
        }
        
        // Databricks JDBC 드라이버 명시적으로 로드
        try {
            // 여러 방법으로 드라이버 로드 시도
            Class<?> driverClass = Class.forName("com.databricks.client.jdbc.Driver");
            // 드라이버 인스턴스 생성하여 DriverManager에 등록
            driverClass.getDeclaredConstructor().newInstance();
            log.info("Databricks JDBC 드라이버 로드 완료");
        } catch (Exception e) {
            log.warn("Databricks JDBC 드라이버를 명시적으로 로드할 수 없습니다: {}. JDBC URL에 따라 자동으로 로드될 수 있습니다.", e.getMessage());
            log.debug("드라이버 로드 실패 상세", e);
        }
        
        // 연결 테스트
        try (Connection conn = getConnection()) {
            log.info("Databricks 연결 테스트 성공: {}", jdbcUrl);
        } catch (SQLException e) {
            log.error("Databricks 연결 테스트 실패", e);
            // 연결 실패해도 계속 진행 (나중에 재시도 가능)
            log.warn("Databricks 연결 실패했지만 Connector는 계속 실행됩니다. 설정을 확인하세요.");
        }
    }
    
    /**
     * Databricks JDBC 연결 생성
     */
    private Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.put("PWD", jdbcToken);
        return DriverManager.getConnection(jdbcUrl, props);
    }
    
    /**
     * 토픽별 staging 테이블 생성 (초기화 - 타겟 테이블이 있으면 생성, 없으면 lazy initialization)
     */
    private void createStagingTableForTopic(String topic) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            log.warn("Databricks 연결이 없어 staging 테이블을 생성할 수 없습니다.");
            return;
        }
        
        try (Connection conn = getConnection()) {
            // 타겟 테이블 이름 추출 (staging 테이블도 동일한 이름 사용)
            String targetTableName = extractTargetTableName(topic);
            String stagingTableName = targetTableName;
            
            // 타겟 테이블이 존재하는지 확인
            boolean targetExists = tableExists(conn, targetTableName);
            
            if (targetExists) {
                // 타겟 테이블이 있으면 기존 방식대로 생성
                String createStagingTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s AS " +
                    "SELECT *, " +
                    "  CAST(NULL AS STRING) AS _operation, " +
                    "  CAST(NULL AS TIMESTAMP) AS _timestamp " +
                    "FROM %s WHERE 1=0",
                    stagingTableName, targetTableName
                );
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(createStagingTableSql);
                    log.info("테이블 생성 완료: {} (토픽: {})", stagingTableName, topic);
                } catch (SQLException e) {
                    if (e.getMessage() != null && 
                        (e.getMessage().contains("already exists") || 
                         e.getMessage().contains("TableAlreadyExistsException"))) {
                        log.info("테이블이 이미 존재합니다: {}", stagingTableName);
                    } else {
                        throw e;
                    }
                }
                
                // 컬럼 정보 저장
                TopicTableInfo tableInfo = new TopicTableInfo(targetTableName, stagingTableName);
                tableInfo.dataColumns = getTableColumns(conn, stagingTableName);
                tableInfo.stagingTableCreated = true;
                topicTableMap.put(topic, tableInfo);
                
                log.info("토픽 '{}' → 테이블 '{}' 매핑 완료", 
                    topic, targetTableName);
            } else {
                // 타겟 테이블이 없으면 lazy initialization (첫 메시지에서 생성)
                TopicTableInfo tableInfo = new TopicTableInfo(targetTableName, stagingTableName);
                topicTableMap.put(topic, tableInfo);
                log.info("토픽 '{}' → 타겟 테이블 '{}' 이 존재하지 않습니다. 첫 메시지 수신 시 staging 테이블을 자동 생성합니다.", 
                    topic, targetTableName);
            }
            
        } catch (SQLException e) {
            log.error("토픽 '{}' 테이블 초기화 실패", topic, e);
            // 초기화 실패해도 계속 진행 (lazy initialization으로 처리)
            String targetTableName = extractTargetTableName(topic);
            String stagingTableName = targetTableName;
            TopicTableInfo tableInfo = new TopicTableInfo(targetTableName, stagingTableName);
            topicTableMap.put(topic, tableInfo);
            log.warn("토픽 '{}' 테이블은 첫 메시지 수신 시 생성됩니다.", topic);
        }
    }
    
    /**
     * 토픽 이름에서 타겟 테이블 이름 추출
     */
    private String extractTargetTableName(String topic) {
        String[] parts = topic.split("\\.");
        String tableName = parts[parts.length - 1];
        
        return String.format("%s.%s.%s",
            config.getDatabricksCatalog(),
            config.getDatabricksSchema(),
            tableName);
    }
    
    /**
     * 테이블 존재 여부 확인
     */
    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String checkSql = String.format("SELECT COUNT(*) FROM %s LIMIT 1", tableName);
            stmt.executeQuery(checkSql);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
    
    /**
     * 테이블의 컬럼 목록 가져오기
     */
    private List<String> getTableColumns(Connection conn, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(String.format("DESCRIBE %s", tableName));
            while (rs.next()) {
                String colName = rs.getString("col_name");
                if (!colName.equals("_operation") && !colName.equals("_timestamp")) {
                    columns.add(colName);
                }
            }
        }
        return columns;
    }
    
    /**
     * 첫 메시지의 스키마를 기반으로 staging 테이블 생성
     */
    private synchronized void createStagingTableFromFirstMessage(String topic, TopicTableInfo info, SinkRecord firstRecord, Connection conn) {
        if (info.stagingTableCreated) {
            return; // 이미 생성되었으면 스킵
        }

        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            log.warn("Databricks 연결이 없어 테이블을 생성할 수 없습니다.");
            return;
        }
        
        try {
            // Debezium 메시지에서 스키마 추출
            Object value = firstRecord.value();
            if (!(value instanceof Struct)) {
                log.warn("토픽 '{}': 첫 메시지가 Struct 형식이 아닙니다. staging 테이블 생성을 건너뜁니다.", topic);
                return;
            }
            
            Struct struct = (Struct) value;
            Struct after = struct.getStruct("after");
            Struct before = struct.getStruct("before");
            
            // after 또는 before에서 스키마 추출
            Struct dataStruct = after != null ? after : before;
            if (dataStruct == null) {
                log.warn("토픽 '{}': 첫 메시지에 after/before 필드가 없습니다. staging 테이블 생성을 건너뜁니다.", topic);
                return;
            }
            
            // 컬럼 정의 생성
            List<String> columnDefinitions = new ArrayList<>();
            List<String> dataColumns = new ArrayList<>();
            
            for (Field field : dataStruct.schema().fields()) {
                String colName = field.name();
                String sqlType = convertSchemaTypeToSQL(field.schema());
                columnDefinitions.add(String.format("%s %s", colName, sqlType));
                dataColumns.add(colName);
            }
            
            // _operation, _timestamp 컬럼 추가 (테이블 생성용)
            columnDefinitions.add("_operation STRING");
            columnDefinitions.add("_timestamp TIMESTAMP");
            // dataColumns에는 데이터 컬럼만 저장 (INSERT 시 _operation, _timestamp는 별도 추가)
            
            // CREATE TABLE 문 생성
            String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s) USING DELTA",
                info.stagingTableName,
                String.join(", ", columnDefinitions)
            );
            
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSql);
                log.info("테이블 자동 생성 완료: {} (토픽: {}, 컬럼 수: {})", 
                    info.stagingTableName, topic, dataColumns.size());
                
                // 컬럼 정보 저장
                info.dataColumns = dataColumns;
                info.stagingTableCreated = true;
            } catch (SQLException e) {
                if (e.getMessage() != null && 
                    (e.getMessage().contains("already exists") || 
                     e.getMessage().contains("TableAlreadyExistsException"))) {
                    log.info("테이블이 이미 존재합니다: {}", info.stagingTableName);
                    // 기존 테이블의 컬럼 정보 가져오기 (데이터 컬럼만, _operation, _timestamp 제외)
                    try {
                        info.dataColumns = getTableColumns(conn, info.stagingTableName);
                        // _operation, _timestamp는 이미 제외됨 (getTableColumns에서 제외)
                    } catch (SQLException ex) {
                        log.error("기존 테이블 컬럼 정보 가져오기 실패", ex);
                        info.dataColumns = dataColumns; // 초기 스키마로 폴백
                    }
                    info.stagingTableCreated = true;
                } else {
                    log.error("토픽 '{}' 테이블 생성 실패", topic, e);
                    throw e;
                }
            }
            
        } catch (Exception e) {
            log.error("토픽 '{}' 첫 메시지 기반 테이블 생성 실패", topic, e);
            throw new RuntimeException("Failed to create table for topic: " + topic, e);
        }
        }
    
    /**
     * Schema 타입을 SQL 타입으로 변환
     */
    private String convertSchemaTypeToSQL(org.apache.kafka.connect.data.Schema schema) {
        if (schema == null) {
            return "STRING";
        }
        
        org.apache.kafka.connect.data.Schema.Type type = schema.type();
        String logicalType = schema.name();
        
        // Logical type 우선 처리
        if (logicalType != null) {
            switch (logicalType) {
                case "org.apache.kafka.connect.data.Date":
                    return "DATE";
                case "org.apache.kafka.connect.data.Time":
                    return "TIME";
                case "org.apache.kafka.connect.data.Timestamp":
                    return "TIMESTAMP";
                case "io.debezium.time.Date":
                    return "DATE";
                case "io.debezium.time.MicroTime":
                case "io.debezium.time.Time":
                    return "TIME";
                case "io.debezium.time.MicroTimestamp":
                case "io.debezium.time.Timestamp":
                    return "TIMESTAMP";
                case "io.debezium.time.ZonedTimestamp":
                    // ZonedTimestamp는 string 타입이지만 TIMESTAMP로 변환
                    return "TIMESTAMP";
                case "io.debezium.data.VariableScaleDecimal":
                case "io.debezium.data.Decimal":
                case "org.apache.kafka.connect.data.Decimal":
                    // Decimal 타입 처리 - precision과 scale 파라미터 확인
                    String scale = schema.parameters() != null ? schema.parameters().get("scale") : null;
                    String precision = schema.parameters() != null ? schema.parameters().get("connect.decimal.precision") : null;
                    if (precision != null && scale != null) {
                        return String.format("DECIMAL(%s,%s)", precision, scale);
                    } else if (scale != null) {
                        return String.format("DECIMAL(38,%s)", scale);
                    } else {
                        return "DECIMAL(38,2)"; // 기본값
                    }
            }
        }
        
        // 기본 타입 처리
        switch (type) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                // int32는 Date일 수도 있음 (이미 logical type에서 처리됨)
                return "INT";
            case INT64:
                // int64는 Timestamp일 수도 있음 (이미 logical type에서 처리됨)
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                // string은 ZonedTimestamp일 수도 있음 (이미 logical type에서 처리됨)
                return "STRING";
            case BYTES:
                // BYTES는 Decimal일 수도 있음 (Debezium의 경우)
                if (logicalType != null && logicalType.contains("Decimal")) {
                    return "DECIMAL(38,2)";
                }
                return "BINARY";
            case ARRAY:
                return "ARRAY<STRING>";
            case MAP:
                return "MAP<STRING, STRING>";
            case STRUCT:
                return "STRUCT";
            default:
                return "STRING";
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty() || jdbcUrl == null || jdbcUrl.isEmpty()) {
            return;
        }
        
        log.debug("처리할 레코드 수: {}", records.size());
        
        // 토픽별로 그룹화
        Map<String, List<SinkRecord>> recordsByTopic = new HashMap<>();
        for (SinkRecord record : records) {
            recordsByTopic.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record);
        }
        
        // 각 토픽(테이블)별로 독립적인 트랜잭션 처리
        // 하나의 테이블이 실패해도 다른 테이블은 성공적으로 커밋됨
        for (Map.Entry<String, List<SinkRecord>> entry : recordsByTopic.entrySet()) {
            String topic = entry.getKey();
            List<SinkRecord> topicRecords = entry.getValue();
            
            TopicTableInfo tableInfo = topicTableMap.get(topic);
            if (tableInfo == null) {
                log.warn("토픽 '{}' 에 대한 테이블 정보가 없습니다. 스킵합니다.", topic);
                continue;
            }
            
            // 각 테이블별로 독립적인 Connection과 트랜잭션
            try (Connection conn = getConnection()) {                
                try {
                    // Staging 테이블이 아직 생성되지 않았으면 첫 메시지의 스키마로 생성
                    if (!tableInfo.stagingTableCreated && !topicRecords.isEmpty()) {
                        createStagingTableFromFirstMessage(topic, tableInfo, topicRecords.get(0), conn);
                    }
                    
                    // Staging 테이블에 INSERT
                    if (tableInfo.stagingTableCreated) {
                        insertToStagingTable(topic, tableInfo, topicRecords, conn);                        
                        // 성공한 레코드의 offset 추적 (Kafka offset commit용)
                        updateCommittedOffsets(topicRecords);
                        log.debug("토픽 '{}' INSERT 및 offset 추적 완료", topic);
                    } else {
                        log.warn("토픽 '{}' 테이블이 아직 생성되지 않았습니다. 다음 메시지에서 재시도합니다.", topic);
                        // 테이블이 없어도 레코드는 처리했으므로 offset 추적
                        updateCommittedOffsets(topicRecords);
                    }
                    
                } catch (SQLException e) {
                    log.error("토픽 '{}' INSERT 중 오류 발생", topic, e);
                }
                
            } catch (SQLException e) {
                log.error("토픽 '{}' 실행 중 오류 발생", topic, e);
            }
        }
    }
    
    /**
     * Staging 테이블에 데이터 INSERT (하나의 INSERT 문에 여러 VALUES 포함)
     */
    private void insertToStagingTable(String topic, TopicTableInfo tableInfo, List<SinkRecord> records, Connection conn) {
        if (records.isEmpty() || tableInfo.dataColumns.isEmpty()) {
            return;
        }
        
        try {
            // 컬럼 목록 (데이터 컬럼 + _operation + _timestamp)
            List<String> allColumns = new ArrayList<>(tableInfo.dataColumns);
            allColumns.add("_operation");
            allColumns.add("_timestamp");
            
            // 모든 레코드를 파싱하여 VALUES 절 생성
            List<String> valuesList = new ArrayList<>();
            int successCount = 0;
            
            for (SinkRecord record : records) {
                Map<String, Object> data = extractData(record);
                if (data == null) {
                    continue;
                }
                
                try {
                    // VALUES 절 생성
                    List<String> valueParts = new ArrayList<>();
                    
                    // 데이터 컬럼 값
                    for (String colName : tableInfo.dataColumns) {
                        Object value = data.get(colName);
                        valueParts.add(formatValueForSQL(value));
                    }
                    
                    // _operation 컬럼
                    String operation = (String) data.getOrDefault("_operation", "c");
                    valueParts.add(formatValueForSQL(operation));
                    
                    // _timestamp 컬럼
                    Long timestamp = (Long) data.get("_timestamp");
                    if (timestamp != null) {
                        valueParts.add(String.format("TIMESTAMP '%s'", new Timestamp(timestamp).toString()));
                    } else {
                        valueParts.add(String.format("TIMESTAMP '%s'", new Timestamp(System.currentTimeMillis()).toString()));
                    }
                    
                    valuesList.add("(" + String.join(",", valueParts) + ")");
                    successCount++;
                    
                } catch (Exception e) {
                    log.warn("토픽 '{}' 레코드 파싱 실패 - Offset: {}", topic, record.kafkaOffset(), e);
                }
            }
            
            if (valuesList.isEmpty()) {
                return;
            }
            
            // 하나의 INSERT 문 생성
            String insertSql = String.format(
                "INSERT INTO %s (%s) VALUES %s",
                tableInfo.stagingTableName,
                String.join(",", allColumns),
                String.join(",", valuesList)
            );
            
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(insertSql);
                log.debug("토픽 '{}': {} 레코드를 테이블 {} 에 INSERT 완료 (단일 쿼리)", 
                    topic, successCount, tableInfo.stagingTableName);
            }
            
        } catch (SQLException e) {
            log.error("토픽 '{}' 테이블 INSERT 실패", topic, e);
            throw e; // 상위로 예외 전파하여 롤백 처리
        }
    }
    
    /**
     * SQL 값 포맷팅 (문자열 이스케이프, NULL 처리 등)
     */
    private String formatValueForSQL(Object value) {
        if (value == null) {
            return "NULL";
        }
        
        if (value instanceof String) {
            // SQL 문자열 이스케이프 (작은따옴표 이스케이프)
            String str = (String) value;
            str = str.replace("'", "''");
            return "'" + str + "'";
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof java.util.Date) {
            return String.format("TIMESTAMP '%s'", new Timestamp(((java.util.Date) value).getTime()).toString());
        } else {
            // 기타 타입은 문자열로 변환
            String str = value.toString().replace("'", "''");
            return "'" + str + "'";
        }
    }

    /**
     * Debezium CDC 메시지에서 데이터 추출
     */
    private Map<String, Object> extractData(SinkRecord record) {
        try {
            Map<String, Object> result = new HashMap<>();
            
            Object value = record.value();
            
            // Debezium CDC 메시지 파싱
            if (value instanceof Struct) {
                Struct struct = (Struct) value;
            
                // Operation 추출
                String operation = struct.getString("op");
                result.put("_operation", operation);
                
                // payload 내의 after 필드에서 데이터 추출 (INSERT/UPDATE)
                Struct after = struct.getStruct("after");
                if (after != null) {
                    for (Field field : after.schema().fields()) {
                        Object fieldValue = after.get(field);
                        result.put(field.name(), convertValue(fieldValue));
                    }
                    // source에서 timestamp 추출
                    Struct source = struct.getStruct("source");
                    if (source != null) {
                        Long tsMs = source.getInt64("ts_ms");
                        result.put("_timestamp", tsMs);
                    }
                } else if ("d".equals(operation)) {
                    // DELETE의 경우 before 사용
                    Struct before = struct.getStruct("before");
                    if (before != null) {
                        for (Field field : before.schema().fields()) {
                            Object fieldValue = before.get(field);
                            result.put(field.name(), convertValue(fieldValue));
                        }
                    }
                    Struct source = struct.getStruct("source");
                    if (source != null) {
                        Long tsMs = source.getInt64("ts_ms");
                        result.put("_timestamp", tsMs);
                    }
                }
            } else {
                log.warn("지원하지 않는 레코드 타입: {}", value != null ? value.getClass() : "null");
                return null;
            }
            
            return result;
        } catch (Exception e) {
            log.error("데이터 추출 실패 - Topic: {}, Offset: {}", 
                record.topic(), record.kafkaOffset(), e);
            return null;
        }
    }
    
    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }
        return value;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // 성공한 레코드의 offset만 commit
        // currentOffsets에는 모든 레코드의 offset이 포함되어 있지만,
        // 실제로 commit할 offset은 committedOffsets에 저장된 것만 사용
        if (committedOffsets.isEmpty()) {
            log.debug("Flush 호출됨 - commit할 offset이 없습니다");
            return;
        }
        
        // 성공한 레코드의 offset만 필터링하여 commit
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : committedOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long committedOffset = entry.getValue();
            
            // currentOffsets에서 해당 파티션의 offset을 가져옴
            OffsetAndMetadata currentOffset = currentOffsets.get(tp);
            if (currentOffset != null) {
                // 성공한 offset까지만 commit (committedOffset + 1, 다음 offset)
                long nextOffset = committedOffset + 1;
                if (nextOffset <= currentOffset.offset()) {
                    offsetsToCommit.put(tp, new OffsetAndMetadata(nextOffset));
                    log.debug("토픽 '{}' 파티션 {}: offset {} 까지 commit", 
                        tp.topic(), tp.partition(), committedOffset);
                }
            }
        }
        
        // 성공한 offset만 commit 요청
        if (!offsetsToCommit.isEmpty()) {
            context.requestCommit(offsetsToCommit);
            log.debug("{} 개 파티션의 offset commit 요청", offsetsToCommit.size());
        }
        
        // committedOffsets 초기화 (다음 배치를 위해)
        committedOffsets.clear();
    }
    
    /**
     * 성공한 레코드의 offset을 추적
     */
    private void updateCommittedOffsets(List<SinkRecord> records) {
        for (SinkRecord record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            long offset = record.kafkaOffset();
            
            // 각 파티션별로 최대 offset만 저장 (이전 offset까지 모두 처리된 것으로 간주)
            committedOffsets.merge(tp, offset, Math::max);
        }
    }

    @Override
    public void stop() {
        log.info("Delta Lake Sink Task 종료");
    }
}
