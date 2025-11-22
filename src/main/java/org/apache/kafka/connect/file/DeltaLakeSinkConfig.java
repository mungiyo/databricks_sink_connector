package org.apache.kafka.connect.file;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Databricks Sink Connector 설정
 * Kafka Connect에서 Databricks로 데이터를 전송하는 Connector 설정
 */
public class DeltaLakeSinkConfig extends AbstractConfig {

    // Databricks 설정
    public static final String DATABRICKS_JDBC_URL = "databricks.jdbc.url";
    private static final String DATABRICKS_JDBC_URL_DOC = "Databricks JDBC URL (예: jdbc:databricks://<workspace-url>:443/default;transportMode=http;ssl=1)";
    
    public static final String DATABRICKS_TOKEN = "databricks.token";
    private static final String DATABRICKS_TOKEN_DOC = "Databricks Personal Access Token";
    
    public static final String DATABRICKS_CATALOG = "databricks.catalog";
    private static final String DATABRICKS_CATALOG_DOC = "Databricks Catalog 이름 (예: development, main)";
    
    public static final String DATABRICKS_SCHEMA = "databricks.schema";
    private static final String DATABRICKS_SCHEMA_DOC = "Databricks Schema 이름 (예: default)";

    // ConfigDef 정의
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            DATABRICKS_JDBC_URL,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            DATABRICKS_JDBC_URL_DOC
        )
        .define(
            DATABRICKS_TOKEN,
            Type.PASSWORD,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            DATABRICKS_TOKEN_DOC
        )
        .define(
            DATABRICKS_CATALOG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            DATABRICKS_CATALOG_DOC
        )
        .define(
            DATABRICKS_SCHEMA,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            DATABRICKS_SCHEMA_DOC
        );

    public DeltaLakeSinkConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
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

