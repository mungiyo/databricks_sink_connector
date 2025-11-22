package org.apache.kafka.connect.file;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DeltaLakeSinkTask 단위 테스트
 */
class DeltaLakeSinkTaskTest {

    private DeltaLakeSinkTask task;
    private Map<String, String> props;

    @BeforeEach
    void setUp() {
        task = new DeltaLakeSinkTask();
        props = new HashMap<>();
        props.put("delta.table.path", "/tmp/delta-test");
        props.put("flush.size", "100");
    }

    @Test
    void testVersion() {
        assertEquals("1.0.0", task.version());
    }

    @Test
    void testStart() {
        assertDoesNotThrow(() -> task.start(props));
    }

    @Test
    void testPutEmptyRecords() {
        task.start(props);
        Collection<SinkRecord> emptyRecords = new ArrayList<>();
        assertDoesNotThrow(() -> task.put(emptyRecords));
    }

    @Test
    void testPutWithRecords() {
        task.start(props);
        
        Collection<SinkRecord> records = new ArrayList<>();
        SinkRecord record = new SinkRecord(
            "test-topic",
            0,
            null,
            "key1",
            null,
            "value1",
            0
        );
        records.add(record);
        
        assertDoesNotThrow(() -> task.put(records));
    }

    @Test
    void testFlush() {
        task.start(props);
        assertDoesNotThrow(() -> task.flush(new HashMap<>()));
    }

    @Test
    void testStop() {
        task.start(props);
        assertDoesNotThrow(() -> task.stop());
    }
}

