package org.apache.kafka.connect.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Databricks Sink Connector
 * Kafka 토픽의 데이터를 Databricks Delta Lake로 전송하는 Connector
 * Debezium CDC 메시지를 받아 Databricks staging 테이블에 INSERT 수행
 */
public class DeltaLakeSinkConnector extends SinkConnector {
    
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = new HashMap<>(props);
        // 설정 검증
        DeltaLakeSinkConfig config = new DeltaLakeSinkConfig(props);
        // 초기화 로직
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DeltaLakeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProperties);
            taskConfigs.add(taskConfig);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // 정리 작업
    }

    @Override
    public ConfigDef config() {
        return DeltaLakeSinkConfig.CONFIG_DEF;
    }
}

