package org.apache.kafka.connect.file;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DeltaLakeSinkConfig 단위 테스트
 */
class DeltaLakeSinkConfigTest {

    @Test
    void testConfigWithAllProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("databricks.jdbc.url", "jdbc:databricks://test:443/default");
        props.put("databricks.token", "dapi123");
        props.put("databricks.catalog", "development");
        props.put("databricks.schema", "default");

        DeltaLakeSinkConfig config = new DeltaLakeSinkConfig(props);

        assertEquals("jdbc:databricks://test:443/default", config.getDatabricksJdbcUrl());
        assertEquals("dapi123", config.getDatabricksToken());
        assertEquals("development", config.getDatabricksCatalog());
        assertEquals("default", config.getDatabricksSchema());
    }

    @Test
    void testConfigDefNotNull() {
        assertNotNull(DeltaLakeSinkConfig.CONFIG_DEF);
    }
}

