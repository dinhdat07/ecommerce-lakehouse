-- Iceberg JDBC catalog metadata tables (V0 layout).
-- Source: Trino test fixture + org.apache.iceberg.jdbc.JdbcUtil (Iceberg 1.6.x).
-- Required before Trino or Spark can use jdbc:postgresql://.../iceberg as the Iceberg catalog backend.
-- Trino constructs JdbcCatalog with initializeCatalogTables=false, so these tables must exist.

\connect iceberg

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(5500),
    property_value VARCHAR(5500),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(5500),
    previous_metadata_location VARCHAR(5500),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

ALTER TABLE iceberg_namespace_properties OWNER TO iceberg;
ALTER TABLE iceberg_tables OWNER TO iceberg;
