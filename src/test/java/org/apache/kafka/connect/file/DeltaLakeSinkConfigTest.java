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
        props.put("delta.table.path", "/tmp/delta-table");
        props.put("flush.size", "500");
        props.put("partition.column", "date");

        DeltaLakeSinkConfig config = new DeltaLakeSinkConfig(props);

        assertEquals("/tmp/delta-table", config.getDeltaTablePath());
        assertEquals(500, config.getFlushSize());
        assertEquals("date", config.getPartitionColumn());
    }

    @Test
    void testConfigWithDefaultValues() {
        Map<String, String> props = new HashMap<>();
        props.put("delta.table.path", "/tmp/delta-table");

        DeltaLakeSinkConfig config = new DeltaLakeSinkConfig(props);

        assertEquals("/tmp/delta-table", config.getDeltaTablePath());
        assertEquals(1000, config.getFlushSize()); // 기본값
        assertEquals("", config.getPartitionColumn()); // 기본값
    }

    @Test
    void testConfigDefNotNull() {
        assertNotNull(DeltaLakeSinkConfig.CONFIG_DEF);
    }
}

