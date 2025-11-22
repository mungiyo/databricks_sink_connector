package org.apache.kafka.connect.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DeltaLakeSinkConnector 단위 테스트
 */
class DeltaLakeSinkConnectorTest {

    private DeltaLakeSinkConnector connector;
    private Map<String, String> props;

    @BeforeEach
    void setUp() {
        connector = new DeltaLakeSinkConnector();
        props = new HashMap<>();
        props.put("databricks.jdbc.url", "jdbc:databricks://test:443/default");
        props.put("databricks.token", "dapi123");
        props.put("databricks.catalog", "development");
        props.put("databricks.schema", "default");
    }

    @Test
    void testVersion() {
        assertEquals("1.0.0", connector.version());
    }

    @Test
    void testTaskClass() {
        assertEquals(DeltaLakeSinkTask.class, connector.taskClass());
    }

    @Test
    void testStart() {
        assertDoesNotThrow(() -> connector.start(props));
    }

    @Test
    void testTaskConfigs() {
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);
        
        assertNotNull(taskConfigs);
        assertEquals(3, taskConfigs.size());
        
        // 각 task config가 원본 설정을 포함하는지 확인
        for (Map<String, String> taskConfig : taskConfigs) {
            assertEquals("jdbc:databricks://test:443/default", taskConfig.get("databricks.jdbc.url"));
            assertEquals("dapi123", taskConfig.get("databricks.token"));
            assertEquals("development", taskConfig.get("databricks.catalog"));
            assertEquals("default", taskConfig.get("databricks.schema"));
        }
    }

    @Test
    void testStop() {
        connector.start(props);
        assertDoesNotThrow(() -> connector.stop());
    }

    @Test
    void testConfig() {
        assertNotNull(connector.config());
    }
}

