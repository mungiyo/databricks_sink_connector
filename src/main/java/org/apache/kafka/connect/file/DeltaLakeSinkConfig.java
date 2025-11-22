package org.apache.kafka.connect.file;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Delta Lake Sink Connector 설정
 */
public class DeltaLakeSinkConfig extends AbstractConfig {

    // 설정 키
    public static final String DELTA_TABLE_PATH = "delta.table.path";
    private static final String DELTA_TABLE_PATH_DOC = "Delta Lake 테이블 경로 (예: /tmp/delta-table)";

    public static final String FLUSH_SIZE = "flush.size";
    private static final String FLUSH_SIZE_DOC = "몇 개의 레코드마다 flush할지 지정";
    private static final int FLUSH_SIZE_DEFAULT = 1000;

    public static final String PARTITION_COLUMN = "partition.column";
    private static final String PARTITION_COLUMN_DOC = "파티션 컬럼 이름 (선택사항)";
    private static final String PARTITION_COLUMN_DEFAULT = "";

    // Databricks 설정
    public static final String DATABRICKS_JDBC_URL = "databricks.jdbc.url";
    private static final String DATABRICKS_JDBC_URL_DOC = "Databricks JDBC URL";
    
    public static final String DATABRICKS_TOKEN = "databricks.token";
    private static final String DATABRICKS_TOKEN_DOC = "Databricks Personal Access Token";
    
    public static final String DATABRICKS_CATALOG = "databricks.catalog";
    private static final String DATABRICKS_CATALOG_DOC = "Databricks Catalog 이름";
    private static final String DATABRICKS_CATALOG_DEFAULT = "main";
    
    public static final String DATABRICKS_SCHEMA = "databricks.schema";
    private static final String DATABRICKS_SCHEMA_DOC = "Databricks Schema 이름";
    private static final String DATABRICKS_SCHEMA_DEFAULT = "default";

    // ConfigDef 정의
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            DELTA_TABLE_PATH,
            Type.STRING,
            "",
            Importance.LOW,
            DELTA_TABLE_PATH_DOC
        )
        .define(
            FLUSH_SIZE,
            Type.INT,
            FLUSH_SIZE_DEFAULT,
            Importance.MEDIUM,
            FLUSH_SIZE_DOC
        )
        .define(
            PARTITION_COLUMN,
            Type.STRING,
            PARTITION_COLUMN_DEFAULT,
            Importance.LOW,
            PARTITION_COLUMN_DOC
        )
        .define(
            DATABRICKS_JDBC_URL,
            Type.STRING,
            "",
            Importance.HIGH,
            DATABRICKS_JDBC_URL_DOC
        )
        .define(
            DATABRICKS_TOKEN,
            Type.PASSWORD,
            "",
            Importance.HIGH,
            DATABRICKS_TOKEN_DOC
        )
        .define(
            DATABRICKS_CATALOG,
            Type.STRING,
            DATABRICKS_CATALOG_DEFAULT,
            Importance.HIGH,
            DATABRICKS_CATALOG_DOC
        )
        .define(
            DATABRICKS_SCHEMA,
            Type.STRING,
            DATABRICKS_SCHEMA_DEFAULT,
            Importance.MEDIUM,
            DATABRICKS_SCHEMA_DOC
        );

    public DeltaLakeSinkConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String getDeltaTablePath() {
        return getString(DELTA_TABLE_PATH);
    }

    public int getFlushSize() {
        return getInt(FLUSH_SIZE);
    }

    public String getPartitionColumn() {
        return getString(PARTITION_COLUMN);
    }
    
    public String getDatabricksJdbcUrl() {
        return getString(DATABRICKS_JDBC_URL);
    }
    
    public String getDatabricksToken() {
        return getPassword(DATABRICKS_TOKEN).value();
    }
    
    public String getDatabricksCatalog() {
        return getString(DATABRICKS_CATALOG);
    }
    
    public String getDatabricksSchema() {
        return getString(DATABRICKS_SCHEMA);
    }
}

