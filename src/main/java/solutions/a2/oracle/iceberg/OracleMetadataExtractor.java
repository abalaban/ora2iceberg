/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.oracle.iceberg;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle metadata extractor for Iceberg migration planning
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OracleMetadataExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleMetadataExtractor.class);
    
    private final Connection connection;
    private final String sourceSchema;
    private final String sourceObject;
    private final boolean isTableOrView;
    
    public OracleMetadataExtractor(final Connection connection,
                                 final String sourceSchema,
                                 final String sourceObject) throws SQLException {
        this.connection = connection;
        this.sourceSchema = sourceSchema;
        this.sourceObject = sourceObject;
        
        // Validate and determine object type - same pattern as StructAndDataMover
        this.isTableOrView = validateAndDetermineType();
        
        LOGGER.info("Initialized metadata extractor for {}.{}", sourceSchema, sourceObject);
    }
    
    private boolean validateAndDetermineType() throws SQLException {
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        
        // First try standard metadata approach
        try (final ResultSet tables = dbMetaData.getTables(null, sourceSchema, sourceObject, null)) {
            if (tables.next()) {
                final String tableType = tables.getString("TABLE_TYPE");
                LOGGER.info("Found {} {}.{} via DatabaseMetaData", tableType, sourceSchema, sourceObject);
                return true;
            }
        }
        
        // Fallback: Try direct access for Oracle EBS-style permissions
        LOGGER.info("Table not found via metadata, attempting direct access validation for {}.{}", sourceSchema, sourceObject);
        
        final String testSql = String.format("SELECT COUNT(*) FROM %s.%s WHERE ROWNUM = 1", sourceSchema, sourceObject);
        try (final PreparedStatement ps = connection.prepareStatement(testSql);
             final ResultSet rs = ps.executeQuery()) {
            
            if (rs.next()) {
                LOGGER.info("Successfully validated access to {}.{} via direct query", sourceSchema, sourceObject);
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error(
                "\n=====================\n" +
                "Unable to access {}.{} !\n" +
                "DatabaseMetaData check failed and direct query failed with: {}" +
                "\n=====================\n",
                sourceSchema, sourceObject, e.getMessage());
            throw new SQLException(String.format(
                "Table or view %s.%s does not exist or is not accessible: %s", 
                sourceSchema, sourceObject, e.getMessage()));
        }
        
        // Should not reach here
        throw new SQLException(String.format(
            "Table or view %s.%s validation failed", sourceSchema, sourceObject));
    }
    
    public void extractAndSave(final String outputDir) throws SQLException, IOException {
        LOGGER.info("Starting metadata extraction for {}.{}", sourceSchema, sourceObject);
        
        final long startTime = System.currentTimeMillis();
        
        // Generate output filename - same pattern as discussed
        final String filename = String.format("%s_%s.json", 
            sourceSchema.toLowerCase(), sourceObject.toLowerCase());
        final File outputFile = new File(outputDir, filename);
        
        // Extract and write directly to file - memory efficient
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile), 8192)) {
            writeMetadataToFile(writer);
        }
        
        final long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("Metadata extraction completed in {} ms. Output: {}", 
                   duration, outputFile.getAbsolutePath());
    }
    
    private void writeMetadataToFile(final BufferedWriter writer) throws SQLException, IOException {
        writer.write("{\n");
        writer.write("  \"extraction_timestamp\": ");
        writer.write(String.valueOf(System.currentTimeMillis()));
        writer.write(",\n");
        
        // Write each section - minimal format for now
        writeTableMetadata(writer);
        writer.write(",\n");
        writeColumnMetadata(writer);
        writer.write(",\n");
        writeConstraintMetadata(writer);
        
        writer.write("\n}");
        writer.flush();
    }
    
    private void writeTableMetadata(final BufferedWriter writer) throws SQLException, IOException {
        writer.write("  \"table_metadata\": {\n");
        
        // Always write basic info
        writer.write("    \"schema\": \"");
        writeEscapedString(writer, sourceSchema);
        writer.write("\",\n    \"table\": \"");
        writeEscapedString(writer, sourceObject);
        writer.write("\"");
        
        // Try to get table type via DatabaseMetaData
        boolean foundTableType = false;
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        try (final ResultSet tables = dbMetaData.getTables(null, sourceSchema, sourceObject, null)) {
            if (tables.next()) {
                writer.write(",\n    \"type\": \"");
                writeEscapedString(writer, tables.getString("TABLE_TYPE"));
                writer.write("\"");
                foundTableType = true;
            }
        }
        
        // Fallback: Query Oracle data dictionary for table type
        if (!foundTableType) {
            final String tableTypeSql = String.format(
                "SELECT object_type FROM all_objects WHERE owner = UPPER(?) AND object_name = UPPER(?)");
            try (final PreparedStatement ps = connection.prepareStatement(tableTypeSql)) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceObject);
                try (final ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        writer.write(",\n    \"type\": \"");
                        writeEscapedString(writer, rs.getString("object_type"));
                        writer.write("\"");
                        foundTableType = true;
                    }
                }
            }
        }
        
        // Default to TABLE if still not found
        if (!foundTableType) {
            writer.write(",\n    \"type\": \"TABLE\"");
        }
        
        // Get row count - important for analysis planning
        if (isTableOrView) {
            final String countSql = String.format("SELECT COUNT(*) FROM %s.%s", sourceSchema, sourceObject);
            try (final PreparedStatement ps = connection.prepareStatement(countSql);
                 final ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    final long rowCount = rs.getLong(1);
                    writer.write(",\n    \"row_count\": ");
                    writer.write(String.valueOf(rowCount));
                    LOGGER.info("Table {}.{} contains {} rows", sourceSchema, sourceObject, rowCount);
                }
            }
        }
        
        writer.write("\n  }");
    }
    
    private void writeColumnMetadata(final BufferedWriter writer) throws SQLException, IOException {
        writer.write("  \"columns\": {\n");
        
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        boolean firstColumn = true;
        boolean foundColumns = false;
        
        // First try standard DatabaseMetaData approach
        try (final ResultSet columns = dbMetaData.getColumns(null, sourceSchema, sourceObject, "%")) {
            while (columns.next()) {
                foundColumns = true;
                if (!firstColumn) {
                    writer.write(",\n");
                }
                firstColumn = false;
                
                final String columnName = columns.getString("COLUMN_NAME");
                final int jdbcType = columns.getInt("DATA_TYPE");
                final String typeName = columns.getString("TYPE_NAME");
                final boolean nullable = StringUtils.equals("YES", columns.getString("IS_NULLABLE"));
                final int precision = columns.getInt("COLUMN_SIZE");
                final int scale = columns.getInt("DECIMAL_DIGITS");
                
                // Log same format as StructAndDataMover for consistency
                LOGGER.debug("Source Metadata info {}:{}({},{})",
                            columnName, JdbcTypes.getTypeName(jdbcType), precision, scale);
                
                writeColumnInfo(writer, columnName, typeName, jdbcType, precision, scale, nullable);
            }
        }
        
        // Fallback: Query Oracle data dictionary directly (for Oracle EBS environments)
        if (!foundColumns) {
            LOGGER.info("No columns found via DatabaseMetaData, querying Oracle data dictionary for {}.{}", 
                       sourceSchema, sourceObject);
            
            final String columnsSql = String.format(
                "SELECT column_name, data_type, data_length, data_precision, data_scale, nullable " +
                "FROM all_tab_columns " +
                "WHERE owner = UPPER(?) AND table_name = UPPER(?) " +
                "ORDER BY column_id");
            
            try (final PreparedStatement ps = connection.prepareStatement(columnsSql)) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceObject);
                
                try (final ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        foundColumns = true;
                        if (!firstColumn) {
                            writer.write(",\n");
                        }
                        firstColumn = false;
                        
                        final String columnName = rs.getString("column_name");
                        final String dataType = rs.getString("data_type");
                        final int dataLength = rs.getInt("data_length");
                        final int dataPrecision = rs.getInt("data_precision");
                        final int dataScale = rs.getInt("data_scale");
                        final boolean nullable = "Y".equals(rs.getString("nullable"));
                        
                        // Map Oracle types to JDBC types for consistency
                        final int jdbcType = mapOracleTypeToJdbc(dataType);
                        final int precision = ("NUMBER".equals(dataType) && dataPrecision > 0) ? dataPrecision : dataLength;
                        final int scale = ("NUMBER".equals(dataType)) ? dataScale : 0;
                        
                        LOGGER.debug("Oracle Dictionary info {}:{}({},{})", 
                                    columnName, dataType, precision, scale);
                        
                        writeColumnInfo(writer, columnName, dataType, jdbcType, precision, scale, nullable);
                    }
                }
            }
        }
        
        if (!foundColumns) {
            LOGGER.warn("No column metadata found for {}.{}", sourceSchema, sourceObject);
        } else {
            LOGGER.info("Found column metadata for {}.{}", sourceSchema, sourceObject);
        }
        
        writer.write("\n  }");
    }
    
    private void writeColumnInfo(final BufferedWriter writer, final String columnName, final String typeName, 
                                final int jdbcType, final int precision, final int scale, final boolean nullable) 
                                throws IOException {
        writer.write("    \"");
        writeEscapedString(writer, columnName);
        writer.write("\": {\n");
        writer.write("      \"oracle_type\": \"");
        writeEscapedString(writer, typeName);
        writer.write("\",\n      \"jdbc_type\": ");
        writer.write(String.valueOf(jdbcType));
        writer.write(",\n      \"precision\": ");
        writer.write(String.valueOf(precision));
        writer.write(",\n      \"scale\": ");
        writer.write(String.valueOf(scale));
        writer.write(",\n      \"nullable\": ");
        writer.write(String.valueOf(nullable));
        writer.write("\n    }");
    }
    
    private int mapOracleTypeToJdbc(final String oracleType) {
        // Map Oracle data dictionary types to JDBC types
        switch (oracleType.toUpperCase()) {
            case "NUMBER":
                return java.sql.Types.NUMERIC;
            case "VARCHAR2":
            case "VARCHAR":
                return java.sql.Types.VARCHAR;
            case "CHAR":
                return java.sql.Types.CHAR;
            case "DATE":
                return java.sql.Types.TIMESTAMP;
            case "TIMESTAMP":
                return java.sql.Types.TIMESTAMP;
            case "CLOB":
                return java.sql.Types.CLOB;
            case "BLOB":
                return java.sql.Types.BLOB;
            case "RAW":
                return java.sql.Types.VARBINARY;
            default:
                return java.sql.Types.OTHER;
        }
    }
    
    private void writeConstraintMetadata(final BufferedWriter writer) throws SQLException, IOException {
        writer.write("  \"constraints\": {\n");
        
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        
        // Primary keys - essential for Iceberg migration
        writer.write("    \"primary_key\": [");
        boolean firstPk = true;
        final List<String> primaryKeyColumns = new ArrayList<>();
        
        try (final ResultSet pkSet = dbMetaData.getPrimaryKeys(null, sourceSchema, sourceObject)) {
            while (pkSet.next()) {
                if (!firstPk) {
                    writer.write(", ");
                }
                firstPk = false;
                
                final String columnName = pkSet.getString("COLUMN_NAME");
                primaryKeyColumns.add(columnName);
                
                writer.write("\"");
                writeEscapedString(writer, columnName);
                writer.write("\"");
                
                LOGGER.debug("Primary key column: {}", columnName);
            }
        }
        
        writer.write("],\n");
        
        // Unique indexes - needed for parallel processing when no primary key
        writer.write("    \"unique_indexes\": [\n");
        // Temporarily skip unique index detection for Oracle EBS performance
        LOGGER.info("Skipping unique index detection for Oracle EBS performance - use getUniqueIndexes() method for programmatic access");
        writer.write("\n    ]");
        
        // Validate primary key exists - critical for Iceberg
        if (primaryKeyColumns.isEmpty()) {
            LOGGER.warn("No primary key found for {}.{}. Checking for unique indexes for parallel processing.", 
                       sourceSchema, sourceObject);
        } else {
            LOGGER.info("Primary key found: {} columns", primaryKeyColumns.size());
        }
        
        writer.write("\n  }");
    }
    
    private void writeUniqueIndexes(final BufferedWriter writer) throws SQLException, IOException {
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        boolean firstIndex = true;
        
        // Try DatabaseMetaData first
        try (final ResultSet indexInfo = dbMetaData.getIndexInfo(null, sourceSchema, sourceObject, true, false)) {
            String currentIndexName = null;
            final List<String> currentIndexColumns = new ArrayList<>();
            
            while (indexInfo.next()) {
                final String indexName = indexInfo.getString("INDEX_NAME");
                final String columnName = indexInfo.getString("COLUMN_NAME");
                
                // Skip if null values (can happen with some Oracle drivers)
                if (indexName == null || columnName == null) {
                    continue;
                }
                
                // If we hit a new index, write the previous one
                if (currentIndexName != null && !indexName.equals(currentIndexName)) {
                    writeIndexInfo(writer, currentIndexName, currentIndexColumns, firstIndex);
                    firstIndex = false;
                    currentIndexColumns.clear();
                }
                
                currentIndexName = indexName;
                currentIndexColumns.add(columnName);
            }
            
            // Write the last index
            if (currentIndexName != null && !currentIndexColumns.isEmpty()) {
                writeIndexInfo(writer, currentIndexName, currentIndexColumns, firstIndex);
                firstIndex = false;
            }
        }
        
        // Fallback: Oracle data dictionary for Oracle EBS environments
        // Use timeout to avoid hanging on slow Oracle EBS systems  
        if (firstIndex) {
            LOGGER.info("No unique indexes found via DatabaseMetaData, querying Oracle data dictionary for {}.{} (timeout: 30s)", 
                       sourceSchema, sourceObject);
            
            final String uniqueIndexesSql = 
                "SELECT /*+ FIRST_ROWS */ ui.index_name, uic.column_name, uic.column_position " +
                "FROM all_indexes ui " +
                "JOIN all_ind_columns uic ON ui.owner = uic.index_owner AND ui.index_name = uic.index_name " +
                "WHERE ui.owner = UPPER(?) AND ui.table_name = UPPER(?) AND ui.uniqueness = 'UNIQUE' " +
                "ORDER BY ui.index_name, uic.column_position";
            
            try (final PreparedStatement ps = connection.prepareStatement(uniqueIndexesSql)) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceObject);
                ps.setQueryTimeout(30); // 30 second timeout for Oracle EBS
                
                try (final ResultSet rs = ps.executeQuery()) {
                    String currentIndexName = null;
                    final List<String> currentIndexColumns = new ArrayList<>();
                    
                    while (rs.next()) {
                        final String indexName = rs.getString("index_name");
                        final String columnName = rs.getString("column_name");
                        
                        // If we hit a new index, write the previous one
                        if (currentIndexName != null && !indexName.equals(currentIndexName)) {
                            writeIndexInfo(writer, currentIndexName, currentIndexColumns, firstIndex);
                            firstIndex = false;
                            currentIndexColumns.clear();
                        }
                        
                        currentIndexName = indexName;
                        currentIndexColumns.add(columnName);
                    }
                    
                    // Write the last index
                    if (currentIndexName != null && !currentIndexColumns.isEmpty()) {
                        writeIndexInfo(writer, currentIndexName, currentIndexColumns, firstIndex);
                    }
                } catch (SQLException e) {
                    LOGGER.warn("Unique index query failed for {}.{}: {} (continuing without unique indexes)", 
                               sourceSchema, sourceObject, e.getMessage());
                }
            } catch (SQLException e) {
                LOGGER.warn("Failed to prepare unique index query for {}.{}: {} (continuing without unique indexes)", 
                           sourceSchema, sourceObject, e.getMessage());
            }
        }
    }
    
    private void writeIndexInfo(final BufferedWriter writer, final String indexName, 
                               final List<String> columns, final boolean first) throws IOException {
        if (!first) {
            writer.write(",\n");
        }
        
        writer.write("      {\n        \"name\": \"");
        writeEscapedString(writer, indexName);
        writer.write("\",\n        \"columns\": [");
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.write(", ");
            }
            writer.write("\"");
            writeEscapedString(writer, columns.get(i));
            writer.write("\"");
        }
        
        writer.write("]\n      }");
        
        LOGGER.debug("Unique index: {} with {} columns", indexName, columns.size());
    }
    
    /**
     * Get primary key columns for data loading order
     */
    public List<String> getPrimaryKeyColumns() throws SQLException {
        final List<String> primaryKeys = new ArrayList<>();
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        
        try (final ResultSet pkSet = dbMetaData.getPrimaryKeys(null, sourceSchema, sourceObject)) {
            while (pkSet.next()) {
                primaryKeys.add(pkSet.getString("COLUMN_NAME"));
            }
        }
        
        return primaryKeys;
    }
    
    /**
     * Get NUMBER columns for analysis
     */
    public List<String> getNumberColumns() throws SQLException {
        final List<String> numberColumns = new ArrayList<>();
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        boolean foundColumns = false;
        
        // First try standard DatabaseMetaData approach
        try (final ResultSet columns = dbMetaData.getColumns(null, sourceSchema, sourceObject, "%")) {
            while (columns.next()) {
                foundColumns = true;
                final String columnName = columns.getString("COLUMN_NAME");
                final String typeName = columns.getString("TYPE_NAME");
                
                if ("NUMBER".equals(typeName)) {
                    numberColumns.add(columnName);
                }
            }
        }
        
        // Fallback: Query Oracle data dictionary directly (for Oracle EBS environments)
        if (!foundColumns) {
            LOGGER.info("No columns found via DatabaseMetaData, querying Oracle data dictionary for NUMBER columns in {}.{}", 
                       sourceSchema, sourceObject);
            
            final String numberColumnsSql = 
                "SELECT column_name " +
                "FROM all_tab_columns " +
                "WHERE owner = UPPER(?) AND table_name = UPPER(?) AND data_type = 'NUMBER' " +
                "ORDER BY column_id";
            
            try (final PreparedStatement ps = connection.prepareStatement(numberColumnsSql)) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceObject);
                
                try (final ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        final String columnName = rs.getString("column_name");
                        numberColumns.add(columnName);
                    }
                }
            }
        }
        
        LOGGER.info("Found {} NUMBER columns in {}.{}", numberColumns.size(), sourceSchema, sourceObject);
        return numberColumns;
    }
    
    /**
     * Analyze NUMBER columns to determine optimal Iceberg types
     */
    public List<NumberColumnAnalysis> analyzeNumberColumns() throws SQLException {
        final List<String> numberColumns = getNumberColumns();
        if (numberColumns.isEmpty()) {
            LOGGER.info("No NUMBER columns found for analysis in {}.{}", sourceSchema, sourceObject);
            return new ArrayList<>();
        }
        
        final long rowCount = getRowCount();
        final int sampleSize = calculateSampleSize(rowCount);
        
        LOGGER.info("Analyzing {} NUMBER columns with sample size {} from {} total rows", 
                   numberColumns.size(), sampleSize, rowCount);
        
        final List<NumberColumnAnalysis> results = new ArrayList<>();
        
        // Build SQL to get sample data for all NUMBER columns
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (int i = 0; i < numberColumns.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(numberColumns.get(i));
        }
        sqlBuilder.append(" FROM ").append(sourceSchema).append(".").append(sourceObject);
        
        // Add sampling strategy
        if (sampleSize < rowCount) {
            sqlBuilder.append(" SAMPLE BLOCK(").append(calculateSamplePercent(sampleSize, rowCount)).append(")");
        }
        
        final String sampleSql = sqlBuilder.toString();
        LOGGER.debug("Sample SQL: {}", sampleSql);
        
        try (final PreparedStatement ps = connection.prepareStatement(sampleSql);
             final ResultSet rs = ps.executeQuery()) {
            
            // Initialize analysis objects
            for (final String columnName : numberColumns) {
                results.add(new NumberColumnAnalysis(columnName));
            }
            
            int sampledRows = 0;
            while (rs.next() && sampledRows < sampleSize) {
                for (int i = 0; i < numberColumns.size(); i++) {
                    final NumberColumnAnalysis analysis = results.get(i);
                    final Object value = rs.getObject(i + 1);
                    analysis.analyzeValue(value);
                }
                sampledRows++;
            }
            
            // Finalize analysis and determine types
            for (final NumberColumnAnalysis analysis : results) {
                analysis.finalizeAnalysis();
                LOGGER.info("Column {}: {} -> recommended Iceberg type: {}", 
                           analysis.getColumnName(), analysis.getStatistics(), 
                           analysis.getRecommendedIcebergType());
            }
        }
        
        LOGGER.info("NUMBER column analysis completed for {}.{}", sourceSchema, sourceObject);
        return results;
    }
    
    private int calculateSampleSize(final long totalRows) {
        // Sample size strategy: minimum 10K, maximum 100K, or 5% of total rows
        final long fivePercent = totalRows / 20; // 5%
        if (fivePercent < 10000) {
            return (int) Math.min(totalRows, 10000);
        } else if (fivePercent > 100000) {
            return 100000;
        } else {
            return (int) fivePercent;
        }
    }
    
    private double calculateSamplePercent(final int sampleSize, final long totalRows) {
        return Math.min(100.0, (double) sampleSize / totalRows * 100.0);
    }
    
    /**
     * Get row count for memory allocation planning
     */
    public long getRowCount() throws SQLException {
        if (!isTableOrView) {
            return 0;
        }
        
        final String countSql = String.format("SELECT COUNT(*) FROM %s.%s", sourceSchema, sourceObject);
        try (final PreparedStatement ps = connection.prepareStatement(countSql);
             final ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
    
    /**
     * Get unique indexes for parallel processing when no primary key exists
     */
    public List<UniqueIndex> getUniqueIndexes() throws SQLException {
        final List<UniqueIndex> uniqueIndexes = new ArrayList<>();
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        
        // Try DatabaseMetaData first
        try (final ResultSet indexInfo = dbMetaData.getIndexInfo(null, sourceSchema, sourceObject, true, false)) {
            String currentIndexName = null;
            final List<String> currentIndexColumns = new ArrayList<>();
            
            while (indexInfo.next()) {
                final String indexName = indexInfo.getString("INDEX_NAME");
                final String columnName = indexInfo.getString("COLUMN_NAME");
                
                // Skip if null values (can happen with some Oracle drivers)
                if (indexName == null || columnName == null) {
                    continue;
                }
                
                // If we hit a new index, save the previous one
                if (currentIndexName != null && !indexName.equals(currentIndexName)) {
                    if (!currentIndexColumns.isEmpty()) {
                        uniqueIndexes.add(new UniqueIndex(currentIndexName, new ArrayList<>(currentIndexColumns)));
                    }
                    currentIndexColumns.clear();
                }
                
                currentIndexName = indexName;
                currentIndexColumns.add(columnName);
            }
            
            // Add the last index
            if (currentIndexName != null && !currentIndexColumns.isEmpty()) {
                uniqueIndexes.add(new UniqueIndex(currentIndexName, currentIndexColumns));
            }
        }
        
        // Fallback: Oracle data dictionary for Oracle EBS environments
        if (uniqueIndexes.isEmpty()) {
            LOGGER.info("No unique indexes found via DatabaseMetaData, querying Oracle data dictionary for {}.{}", 
                       sourceSchema, sourceObject);
            
            final String uniqueIndexesSql = 
                "SELECT ui.index_name, uic.column_name, uic.column_position " +
                "FROM all_indexes ui " +
                "JOIN all_ind_columns uic ON ui.owner = uic.index_owner AND ui.index_name = uic.index_name " +
                "WHERE ui.owner = UPPER(?) AND ui.table_name = UPPER(?) AND ui.uniqueness = 'UNIQUE' " +
                "ORDER BY ui.index_name, uic.column_position";
            
            try (final PreparedStatement ps = connection.prepareStatement(uniqueIndexesSql)) {
                ps.setString(1, sourceSchema);
                ps.setString(2, sourceObject);
                ps.setQueryTimeout(30); // 30 second timeout for Oracle EBS
                
                try (final ResultSet rs = ps.executeQuery()) {
                    String currentIndexName = null;
                    final List<String> currentIndexColumns = new ArrayList<>();
                    
                    while (rs.next()) {
                        final String indexName = rs.getString("index_name");
                        final String columnName = rs.getString("column_name");
                        
                        // If we hit a new index, save the previous one
                        if (currentIndexName != null && !indexName.equals(currentIndexName)) {
                            if (!currentIndexColumns.isEmpty()) {
                                uniqueIndexes.add(new UniqueIndex(currentIndexName, new ArrayList<>(currentIndexColumns)));
                            }
                            currentIndexColumns.clear();
                        }
                        
                        currentIndexName = indexName;
                        currentIndexColumns.add(columnName);
                    }
                    
                    // Add the last index
                    if (currentIndexName != null && !currentIndexColumns.isEmpty()) {
                        uniqueIndexes.add(new UniqueIndex(currentIndexName, currentIndexColumns));
                    }
                }
            }
        }
        
        LOGGER.info("Found {} unique indexes for {}.{}", uniqueIndexes.size(), sourceSchema, sourceObject);
        return uniqueIndexes;
    }
    
    /**
     * Simple holder for unique index information
     */
    public static class UniqueIndex {
        private final String name;
        private final List<String> columns;
        
        public UniqueIndex(final String name, final List<String> columns) {
            this.name = name;
            this.columns = columns;
        }
        
        public String getName() {
            return name;
        }
        
        public List<String> getColumns() {
            return columns;
        }
        
        public int getColumnCount() {
            return columns.size();
        }
    }
    
    /**
     * Analysis results for a NUMBER column
     */
    public static class NumberColumnAnalysis {
        private final String columnName;
        private long totalValues;
        private long nullValues;
        private long integerValues; // No decimal part
        private long longValues;    // Fits in long but not int
        private long decimalValues; // Has decimal part or too large for long
        private int maxPrecision;
        private int maxScale;
        private String recommendedType;
        
        public NumberColumnAnalysis(final String columnName) {
            this.columnName = columnName;
            this.totalValues = 0;
            this.nullValues = 0;
            this.integerValues = 0;
            this.longValues = 0;
            this.decimalValues = 0;
            this.maxPrecision = 0;
            this.maxScale = 0;
        }
        
        public void analyzeValue(final Object value) {
            totalValues++;
            
            if (value == null) {
                nullValues++;
                return;
            }
            
            if (value instanceof java.math.BigDecimal) {
                final java.math.BigDecimal bd = (java.math.BigDecimal) value;
                
                // Calculate precision and scale
                final int precision = bd.precision();
                final int scale = bd.scale();
                maxPrecision = Math.max(maxPrecision, precision);
                maxScale = Math.max(maxScale, scale);
                
                // Check if it's effectively an integer (scale 0 or all trailing zeros)
                if (scale <= 0 || bd.stripTrailingZeros().scale() <= 0) {
                    try {
                        final long longValue = bd.longValueExact();
                        if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                            integerValues++;
                        } else {
                            longValues++;
                        }
                    } catch (ArithmeticException e) {
                        decimalValues++; // Too large for long
                    }
                } else {
                    decimalValues++; // Has decimal places
                }
            } else if (value instanceof Number) {
                final Number num = (Number) value;
                final double doubleValue = num.doubleValue();
                
                // Check if it's an integer value
                if (doubleValue == Math.floor(doubleValue) && !Double.isInfinite(doubleValue)) {
                    final long longValue = num.longValue();
                    if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                        integerValues++;
                    } else {
                        longValues++;
                    }
                } else {
                    decimalValues++;
                }
            } else {
                // Fallback for other types
                decimalValues++;
            }
        }
        
        public void finalizeAnalysis() {
            // Determine recommended Iceberg type based on analysis
            final long nonNullValues = totalValues - nullValues;
            
            if (nonNullValues == 0) {
                recommendedType = "decimal(38,10)"; // Default for all-null columns
                return;
            }
            
            // If more than 95% are integers, recommend integer
            if ((integerValues * 100.0 / nonNullValues) > 95.0) {
                recommendedType = "integer";
            }
            // If more than 95% fit in long (int + long), recommend long
            else if (((integerValues + longValues) * 100.0 / nonNullValues) > 95.0) {
                recommendedType = "long";
            }
            // Otherwise use decimal with appropriate precision/scale
            else {
                final int recPrecision = Math.max(10, maxPrecision);
                final int recScale = Math.max(2, maxScale);
                recommendedType = String.format("decimal(%d,%d)", recPrecision, recScale);
            }
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public String getRecommendedIcebergType() {
            return recommendedType;
        }
        
        public String getStatistics() {
            return String.format("total=%d, nulls=%d, int=%d, long=%d, decimal=%d, maxPrec=%d, maxScale=%d", 
                               totalValues, nullValues, integerValues, longValues, decimalValues, 
                               maxPrecision, maxScale);
        }
        
        public long getTotalValues() { return totalValues; }
        public long getNullValues() { return nullValues; }
        public long getIntegerValues() { return integerValues; }
        public long getLongValues() { return longValues; }
        public long getDecimalValues() { return decimalValues; }
        public int getMaxPrecision() { return maxPrecision; }
        public int getMaxScale() { return maxScale; }
    }
    
    // Utility method for JSON escaping - minimal implementation
    private static void writeEscapedString(final BufferedWriter writer, final String str) throws IOException {
        if (str == null) {
            writer.write("null");
            return;
        }
        
        // Basic JSON escaping - sufficient for Oracle identifiers and types
        for (int i = 0; i < str.length(); i++) {
            final char c = str.charAt(i);
            switch (c) {
                case '"':
                    writer.write("\\\"");
                    break;
                case '\\':
                    writer.write("\\\\");
                    break;
                case '\n':
                    writer.write("\\n");
                    break;
                case '\r':
                    writer.write("\\r");
                    break;
                case '\t':
                    writer.write("\\t");
                    break;
                default:
                    writer.write(c);
                    break;
            }
        }
    }
    
    // Getters for integration
    public String getSourceSchema() {
        return sourceSchema;
    }
    
    public String getSourceObject() {
        return sourceObject;
    }
    
    public boolean isTableOrView() {
        return isTableOrView;
    }
}