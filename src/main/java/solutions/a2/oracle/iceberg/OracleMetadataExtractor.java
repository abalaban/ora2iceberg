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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
// imports intentionally omitted; using FQNs in code to avoid extra deps here
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
    private Map<String, NumberColumnAnalysis> inferenceByColumn = new HashMap<>();
    private Ora2IcebergTypeMapper typeMapper;
    
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
        // Recommended type for ALL columns
        final NumberColumnAnalysis analysis = inferenceByColumn.get(columnName);
        final String recommended;
        if (analysis != null && analysis.getRecommendedIcebergType() != null) {
            // Feed inference into mapper so that the single source (mapper) emits the final type
            final String rec = analysis.getRecommendedIcebergType();
            if (StringUtils.equalsIgnoreCase(rec, "integer")) {
                if (typeMapper != null) typeMapper.addExactOverride(columnName, "INTEGER");
            } else if (StringUtils.equalsIgnoreCase(rec, "long")) {
                if (typeMapper != null) typeMapper.addExactOverride(columnName, "BIGINT");
            } else if (StringUtils.startsWithIgnoreCase(rec, "decimal(")) {
                if (typeMapper != null) typeMapper.addExactOverride(columnName, rec.toUpperCase());
            }
        }
        if (typeMapper != null) {
            final org.apache.commons.lang3.tuple.Pair<Integer, org.apache.iceberg.types.Type> mapped =
                    typeMapper.icebergType(columnName, jdbcType, precision, scale);
            final org.apache.iceberg.types.Type t = mapped.getRight();
            if (t instanceof org.apache.iceberg.types.Types.IntegerType) recommended = "integer";
            else if (t instanceof org.apache.iceberg.types.Types.LongType) recommended = "long";
            else if (t instanceof org.apache.iceberg.types.Types.DecimalType) {
                final org.apache.iceberg.types.Types.DecimalType dt = (org.apache.iceberg.types.Types.DecimalType) t;
                recommended = String.format("decimal(%d,%d)", dt.precision(), dt.scale());
            } else if (t instanceof org.apache.iceberg.types.Types.BooleanType) recommended = "boolean";
            else if (t instanceof org.apache.iceberg.types.Types.DoubleType) recommended = "double";
            else if (t instanceof org.apache.iceberg.types.Types.FloatType) recommended = "float";
            else if (t instanceof org.apache.iceberg.types.Types.TimestampType) recommended = "timestamp";
            else if (t instanceof org.apache.iceberg.types.Types.DateType) recommended = "date";
            else if (t instanceof org.apache.iceberg.types.Types.StringType) recommended = "string";
            else if (t instanceof org.apache.iceberg.types.Types.BinaryType) recommended = "binary";
            else recommended = "string";
        } else {
            recommended = deriveRecommendedType(columnName, typeName, jdbcType, precision, scale, analysis);
        }
        writer.write(",\n      \"recommended_iceberg_type\": \"");
        writeEscapedString(writer, recommended);
        writer.write("\"");
        // Stats only for NUMBER columns when analysis is available
        if (analysis != null) {
            writer.write(",\n      \"stats\": {\n");
            writer.write("        \"total_values\": ");
            writer.write(String.valueOf(analysis.getTotalValues()));
            writer.write(",\n        \"null_values\": ");
            writer.write(String.valueOf(analysis.getNullValues()));
            writer.write(",\n        \"integer_values\": ");
            writer.write(String.valueOf(analysis.getIntegerValues()));
            writer.write(",\n        \"long_values\": ");
            writer.write(String.valueOf(analysis.getLongValues()));
            writer.write(",\n        \"decimal_values\": ");
            writer.write(String.valueOf(analysis.getDecimalValues()));
            writer.write(",\n        \"max_precision\": ");
            writer.write(String.valueOf(analysis.getMaxPrecision()));
            writer.write(",\n        \"max_scale\": ");
            writer.write(String.valueOf(analysis.getMaxScale()));
            writer.write("\n      }");
        }
        writer.write("\n    }");
    }

    private String deriveRecommendedType(final String columnName,
                                         final String oracleType,
                                         final int jdbcType,
                                         final int precision,
                                         final int scale,
                                         final NumberColumnAnalysis analysis) {
        // Prefer analysis for NUMBER columns
        if (analysis != null && analysis.getRecommendedIcebergType() != null) {
            return analysis.getRecommendedIcebergType();
        }
        // Try mapper when available to align with migration mapping/overrides
        if (typeMapper != null) {
            final org.apache.commons.lang3.tuple.Pair<Integer, org.apache.iceberg.types.Type> mapped =
                    typeMapper.icebergType(columnName, jdbcType, precision, scale);
            final org.apache.iceberg.types.Type t = mapped.getRight();
            if (t instanceof org.apache.iceberg.types.Types.IntegerType) return "integer";
            if (t instanceof org.apache.iceberg.types.Types.LongType) return "long";
            if (t instanceof org.apache.iceberg.types.Types.DecimalType) {
                final org.apache.iceberg.types.Types.DecimalType dt = (org.apache.iceberg.types.Types.DecimalType) t;
                return String.format("decimal(%d,%d)", dt.precision(), dt.scale());
            }
            if (t instanceof org.apache.iceberg.types.Types.BooleanType) return "boolean";
            if (t instanceof org.apache.iceberg.types.Types.DoubleType) return "double";
            if (t instanceof org.apache.iceberg.types.Types.FloatType) return "float";
            if (t instanceof org.apache.iceberg.types.Types.TimestampType) return "timestamp";
            if (t instanceof org.apache.iceberg.types.Types.DateType) return "date";
            if (t instanceof org.apache.iceberg.types.Types.StringType) return "string";
            if (t instanceof org.apache.iceberg.types.Types.BinaryType) return "binary";
        }
        switch (jdbcType) {
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                return "integer";
            case java.sql.Types.BIGINT:
                return "long";
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL: {
                int p = precision > 0 ? precision : 38;
                int s = scale > 0 ? scale : 0;
                if (s == 0 && p < 10) return "integer";
                if (s == 0 && p < 19) return "long";
                if (p > 38) p = 38;
                if (s > p) s = p;
                if (s < 0) s = 0;
                return String.format("decimal(%d,%d)", p, s);
            }
            case java.sql.Types.FLOAT:
                return "float";
            case java.sql.Types.DOUBLE:
                return "double";
            case java.sql.Types.BOOLEAN:
                return "boolean";
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return "timestamp";
            case java.sql.Types.DATE:
                // Oracle DATE often mapped to TIMESTAMP in JDBC; prefer date if original type is DATE
                if (oracleType != null && "DATE".equalsIgnoreCase(oracleType)) return "date";
                return "timestamp";
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                return "binary";
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:
            case java.sql.Types.SQLXML:
                return "string";
            case java.sql.Types.ROWID:
                return "string";
            default:
                return "string";
        }
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
        if (Boolean.getBoolean("ora2iceberg.skipConstraints")) {
            writer.write("    \"skipped\": true\n");
            writer.write("  }");
            return;
        }
        
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
        writeUniqueIndexes(writer);
        
        // Validate primary key exists - critical for Iceberg
        if (primaryKeyColumns.isEmpty()) {
            LOGGER.warn("No primary key found for {}.{}. Use getUniqueIndexes() method to find alternatives for parallel processing.", 
                       sourceSchema, sourceObject);
        } else {
            LOGGER.info("Primary key found: {} columns", primaryKeyColumns.size());
        }
        
        writer.write("  }");
    }
    
    private void writeUniqueIndexes(final BufferedWriter writer) throws SQLException, IOException {
        writer.write("    \"unique_indexes\": [");
        
        try {
            final List<UniqueIndex> uniqueIndexes = getUniqueIndexes();
            boolean firstIndex = true;
            
            for (final UniqueIndex index : uniqueIndexes) {
                if (!firstIndex) {
                    writer.write(",");
                }
                writer.write("\n      {");
                writer.write("\n        \"name\": \"" + index.getName() + "\",");
                writer.write("\n        \"columns\": [");
                
                boolean firstColumn = true;
                for (final String column : index.getColumns()) {
                    if (!firstColumn) {
                        writer.write(", ");
                    }
                    writer.write("\"" + column + "\"");
                    firstColumn = false;
                }
                
                writer.write("]");
                writer.write("\n      }");
                firstIndex = false;
            }
            
            if (!firstIndex) {
                writer.write("\n    ");
            }
        } catch (final SQLException e) {
            LOGGER.warn("Failed to retrieve unique indexes for JSON output: {}", e.getMessage());
            // Continue with empty array
        }
        
        writer.write("]");
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
     * Analyze NUMBER columns to determine optimal Iceberg types using parallel extraction
     */
    public List<NumberColumnAnalysis> analyzeNumberColumns() throws SQLException {
        final List<String> numberColumns = getNumberColumns();
        if (numberColumns.isEmpty()) {
            LOGGER.info("No NUMBER columns found for analysis in {}.{}", sourceSchema, sourceObject);
            return new ArrayList<>();
        }
        
        final long rowCount = getRowCount();
        final int sampleSize = calculateSampleSize(rowCount);
        
        LOGGER.info("Analyzing {} NUMBER columns with sample size {} from {} total rows using parallel extraction", 
                   numberColumns.size(), sampleSize, rowCount);
        
        final List<NumberColumnAnalysis> results = new ArrayList<>();
        
        // Initialize analysis objects
        for (final String columnName : numberColumns) {
            results.add(new NumberColumnAnalysis(columnName));
        }
        
        // Try parallel extraction using unique indexes first for performance
        if (extractDataParallel(numberColumns, results, sampleSize)) {
            LOGGER.info("Used parallel extraction with unique index ranges");
        } else {
            // Fallback to optimized sequential extraction if no suitable index
            LOGGER.info("No suitable unique index found, using optimized sequential extraction");
            extractDataSequential(numberColumns, results, sampleSize, rowCount);
        }
        
        // Finalize analysis and determine types
        for (final NumberColumnAnalysis analysis : results) {
            analysis.finalizeAnalysis();
            LOGGER.info("Column {}: {} -> recommended Iceberg type: {}", 
                       analysis.getColumnName(), analysis.getStatistics(), 
                       analysis.getRecommendedIcebergType());
        }
        
        LOGGER.info("NUMBER column analysis completed for {}.{}", sourceSchema, sourceObject);
        return results;
    }

    /**
     * Perform a full-table streaming analysis over all rows for NUMBER columns.
     * Attempts to leverage Oracle PQ via a generic PARALLEL hint; falls back to regular full scan.
     */
    public List<NumberColumnAnalysis> analyzeNumberColumnsFull() throws SQLException {
        final List<String> numberColumns = getNumberColumns();
        if (numberColumns.isEmpty()) {
            LOGGER.info("No NUMBER columns found for analysis in {}.{}", sourceSchema, sourceObject);
            return new ArrayList<>();
        }

        final List<NumberColumnAnalysis> results = new ArrayList<>(numberColumns.size());
        for (final String columnName : numberColumns) {
            results.add(new NumberColumnAnalysis(columnName));
        }

        // Build projection
        final StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < numberColumns.size(); i++) {
            if (i > 0) columnList.append(", ");
            columnList.append(numberColumns.get(i));
        }

        final Long scn = tryFetchCurrentScn();

        final String fromClause;
        if (scn == null) {
            fromClause = String.format("FROM %s.%s t", sourceSchema, sourceObject);
        } else {
            // Oracle requires alias after the flashback clause
            fromClause = String.format("FROM %s.%s AS OF SCN %d t", sourceSchema, sourceObject, scn.longValue());
        }

        final int pqDegree = 8;
        final String sql = String.format(
                "SELECT /*+ PARALLEL(t, %d) FULL(t) */ %s %s",
                pqDegree, columnList.toString(), fromClause);

        LOGGER.info("Starting FULL scan inference for {}.{} ({} NUMBER columns){}",
                sourceSchema, sourceObject, numberColumns.size(), scn == null ? "" : (" at SCN " + scn));

        int rowCounter = 0;
        final long start = System.currentTimeMillis();
        try (final PreparedStatement ps = connection.prepareStatement(sql)) {
            try {
                ps.setFetchSize(10000);
            } catch (final Exception e) {
                // ignore if driver doesn't honor
            }
            try (final ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    for (int i = 0; i < numberColumns.size(); i++) {
                        final NumberColumnAnalysis analysis = results.get(i);
                        final Object value = rs.getObject(i + 1);
                        analysis.analyzeValue(value);
                    }
                    rowCounter++;
                    if ((rowCounter % 500000) == 0) {
                        final long elapsed = System.currentTimeMillis() - start;
                        LOGGER.info("Processed {} rows in {} ms", rowCounter, elapsed);
                    }
                }
            }
        }

        for (final NumberColumnAnalysis a : results) {
            a.finalizeAnalysis();
        }

        final long elapsed = System.currentTimeMillis() - start;
        LOGGER.info("Completed FULL scan inference for {}.{}: {} rows in {} ms", sourceSchema, sourceObject, rowCounter, elapsed);
        return results;
    }
    
    /**
     * ORA_HASH(ROWID) client-parallel full scan when PQ is unavailable/undesired.
     * Controls via system properties:
     *   -Dora2iceberg.workers=N (default 0 disables; when >0, use N workers)
     *   -Dora2iceberg.fetchSize=K (default 10000)
     */
    public List<NumberColumnAnalysis> analyzeNumberColumnsFullRowIdParallel(final String jdbcUrl, final String user, final String password) throws SQLException {
        final int workers = Integer.getInteger("ora2iceberg.workers", 0);
        if (workers <= 0) {
            return analyzeNumberColumnsFull();
        }

        final List<String> numberColumns = getNumberColumns();
        if (numberColumns.isEmpty()) {
            return new ArrayList<>();
        }
        final List<NumberColumnAnalysis> merged = new ArrayList<>(numberColumns.size());
        for (final String c : numberColumns) merged.add(new NumberColumnAnalysis(c));

        final StringBuilder projection = new StringBuilder();
        for (int i = 0; i < numberColumns.size(); i++) {
            if (i > 0) projection.append(", ");
            projection.append(numberColumns.get(i));
        }

        final Long scn = tryFetchCurrentScn();
        final String baseFrom = scn == null
                ? String.format("%s.%s t", sourceSchema, sourceObject)
                : String.format("%s.%s AS OF SCN %d t", sourceSchema, sourceObject, scn.longValue());

        final int fetchSize = Integer.getInteger("ora2iceberg.fetchSize", 10000);

        final ExecutorService pool = Executors.newFixedThreadPool(workers);
        final List<Future<List<NumberColumnAnalysis>>> futures = new ArrayList<>(workers);
        for (int k = 0; k < workers; k++) {
            final int bucket = k;
            futures.add(pool.submit(new Callable<List<NumberColumnAnalysis>>() {
                @Override
                public List<NumberColumnAnalysis> call() throws Exception {
                    try (final Connection c = java.sql.DriverManager.getConnection(jdbcUrl, user, password)) {
                        final List<NumberColumnAnalysis> local = new ArrayList<>(numberColumns.size());
                        for (final String col : numberColumns) local.add(new NumberColumnAnalysis(col));
                        final String sql = String.format(
                                "SELECT %s FROM %s WHERE ORA_HASH(ROWID, %d) = %d",
                                projection.toString(), baseFrom, workers - 1, bucket);
                        try (final PreparedStatement ps = c.prepareStatement(sql)) {
                            try { ps.setFetchSize(fetchSize); } catch (Throwable ignore) {}
                            try (final ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    for (int i = 0; i < numberColumns.size(); i++) {
                                        local.get(i).analyzeValue(rs.getObject(i + 1));
                                    }
                                }
                            }
                        }
                        for (final NumberColumnAnalysis a : local) a.finalizeAnalysis();
                        return local;
                    }
                }
            }));
        }

        pool.shutdown();
        for (final Future<List<NumberColumnAnalysis>> f : futures) {
            try {
                final List<NumberColumnAnalysis> part = f.get();
                for (int i = 0; i < merged.size(); i++) {
                    final NumberColumnAnalysis target = merged.get(i);
                    final NumberColumnAnalysis src = part.get(i);
                    // merge counters and maxima
                    target.totalValues += src.totalValues;
                    target.nullValues += src.nullValues;
                    target.integerValues += src.integerValues;
                    target.longValues += src.longValues;
                    target.decimalValues += src.decimalValues;
                    target.maxPrecision = Math.max(target.maxPrecision, src.maxPrecision);
                    target.maxScale = Math.max(target.maxScale, src.maxScale);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new SQLException(e);
            }
        }
        for (final NumberColumnAnalysis a : merged) a.finalizeAnalysis();
        return merged;
    }

    private Long tryFetchCurrentScn() {
        // Try v$database first
        try (final PreparedStatement ps = connection.prepareStatement("SELECT current_scn FROM v$database");
             final ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (final SQLException ignore) {
            // ignore
        }
        // Try DBMS_FLASHBACK
        try (final PreparedStatement ps = connection.prepareStatement("SELECT dbms_flashback.get_system_change_number FROM dual");
             final ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (final SQLException ignore) {
            // ignore
        }
        return null;
    }

    /** Inject externally computed inference results so they are written to JSON. */
    public void setInferenceResults(final List<NumberColumnAnalysis> analysis) {
        this.inferenceByColumn.clear();
        if (analysis != null) {
            for (final NumberColumnAnalysis a : analysis) {
                this.inferenceByColumn.put(a.getColumnName(), a);
            }
        }
    }

    /**
     * Set optional type mapper to align JSON recommendations with migration mapping/overrides.
     */
    public void setTypeMapper(final Ora2IcebergTypeMapper mapper) {
        this.typeMapper = mapper;
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
     * Extract data using parallel queries based on unique index ranges with detailed performance monitoring
     */
    private boolean extractDataParallel(final List<String> numberColumns, final List<NumberColumnAnalysis> results, final int sampleSize) throws SQLException {
        final long parallelStartTime = System.currentTimeMillis();
        LOGGER.info("=== PARALLEL EXTRACTION START ===");
        
        final List<UniqueIndex> uniqueIndexes = getUniqueIndexes();
        
        // Find a single-column numeric unique index suitable for range queries
        UniqueIndex bestIndex = null;
        String indexColumn = null;
        
        final long indexSearchStart = System.currentTimeMillis();
        for (final UniqueIndex index : uniqueIndexes) {
            if (index.getColumnCount() == 1) {
                final String columnName = index.getColumns().get(0);
                LOGGER.debug("Checking index {} on column {} for numeric compatibility", index.getName(), columnName);
                // Check if this column is numeric (suitable for range queries)
                if (isNumericColumn(columnName)) {
                    bestIndex = index;
                    indexColumn = columnName;
                    LOGGER.info("Selected index {} on numeric column {} for parallel extraction", index.getName(), columnName);
                    break;
                } else {
                    LOGGER.debug("Index {} on column {} is not suitable (not numeric)", index.getName(), columnName);
                }
            } else {
                LOGGER.debug("Index {} has {} columns (need single-column index for range queries)", index.getName(), index.getColumnCount());
            }
        }
        
        final long indexSearchTime = System.currentTimeMillis() - indexSearchStart;
        LOGGER.info("Index selection completed in {} ms", indexSearchTime);
        
        if (bestIndex == null) {
            LOGGER.info("No suitable numeric single-column unique index found for parallel extraction");
            return false; // No suitable index for parallel extraction
        }
        
        // Get min/max values for the index column to determine ranges
        LOGGER.info("Determining value range for parallel extraction using index {}", bestIndex.getName());
        final String minMaxSql = String.format(
            "SELECT /*+ INDEX(%s %s) */ MIN(%s), MAX(%s) FROM %s.%s", 
            sourceObject, bestIndex.getName(), indexColumn, indexColumn, sourceSchema, sourceObject);
        
        LOGGER.debug("MIN/MAX SQL: {}", minMaxSql);
        final long minMaxStart = System.currentTimeMillis();
        
        long minValue, maxValue;
        try (final PreparedStatement ps = connection.prepareStatement(minMaxSql);
             final ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                LOGGER.warn("No data returned from MIN/MAX query for {}.{}", sourceSchema, sourceObject);
                return false;
            }
            minValue = rs.getLong(1);
            maxValue = rs.getLong(2);
        }
        
        final long minMaxTime = System.currentTimeMillis() - minMaxStart;
        LOGGER.info("MIN/MAX query completed in {} ms: range {} to {} (span: {})", 
                   minMaxTime, minValue, maxValue, maxValue - minValue + 1);
        
        // Calculate ranges for parallel extraction (4 threads)
        final int numThreads = 4;
        final long totalRange = maxValue - minValue + 1;
        final long rangeSize = totalRange / numThreads;
        final int targetPerThread = sampleSize / numThreads;
        
        LOGGER.info("Parallel extraction plan: {} threads, {} samples per thread, {} range size per thread", 
                   numThreads, targetPerThread, rangeSize);
        
        // Build column list for SELECT
        final StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < numberColumns.size(); i++) {
            if (i > 0) columnList.append(", ");
            columnList.append(numberColumns.get(i));
        }
        LOGGER.debug("Column list for extraction ({} columns): {}", numberColumns.size(), 
                    columnList.length() > 200 ? columnList.substring(0, 200) + "..." : columnList.toString());
        
        int totalSampled = 0;
        final long[] chunkTimes = new long[numThreads];
        final int[] chunkSamples = new int[numThreads];
        final long extractionStartTime = System.currentTimeMillis();
        
        // Extract data in parallel ranges
        for (int thread = 0; thread < numThreads && totalSampled < sampleSize; thread++) {
            final long chunkStart = System.currentTimeMillis();
            final long rangeStart = minValue + (thread * rangeSize);
            final long rangeEnd = (thread == numThreads - 1) ? maxValue : rangeStart + rangeSize - 1;
            final int remainingSamples = sampleSize - totalSampled;
            final int threadSamples = Math.min(targetPerThread, remainingSamples);
            
            LOGGER.info("CHUNK {} START: Range {}-{} (size: {}), target samples: {}", 
                       thread + 1, rangeStart, rangeEnd, rangeEnd - rangeStart + 1, threadSamples);
            
            final String rangeSql = String.format(
                "SELECT /*+ INDEX(%s %s) FIRST_ROWS(%d) */ %s FROM %s.%s WHERE %s BETWEEN ? AND ? AND ROWNUM <= ?",
                sourceObject, bestIndex.getName(), threadSamples, columnList.toString(), sourceSchema, sourceObject, indexColumn);
            
            LOGGER.debug("CHUNK {} SQL: {}", thread + 1, rangeSql.length() > 300 ? rangeSql.substring(0, 300) + "..." : rangeSql);
            
            final long sqlPrepStart = System.currentTimeMillis();
            try (final PreparedStatement ps = connection.prepareStatement(rangeSql)) {
                ps.setLong(1, rangeStart);
                ps.setLong(2, rangeEnd);
                ps.setInt(3, threadSamples);
                
                final long sqlPrepTime = System.currentTimeMillis() - sqlPrepStart;
                LOGGER.debug("CHUNK {} SQL preparation: {} ms", thread + 1, sqlPrepTime);
                
                final long executeStart = System.currentTimeMillis();
                try (final ResultSet rs = ps.executeQuery()) {
                    final long executeTime = System.currentTimeMillis() - executeStart;
                    LOGGER.debug("CHUNK {} SQL execution: {} ms", thread + 1, executeTime);
                    
                    final long fetchStart = System.currentTimeMillis();
                    int threadSampledRows = 0;
                    int rowsProcessed = 0;
                    
                    while (rs.next() && threadSampledRows < threadSamples && totalSampled < sampleSize) {
                        for (int i = 0; i < numberColumns.size(); i++) {
                            final NumberColumnAnalysis analysis = results.get(i);
                            final Object value = rs.getObject(i + 1);
                            analysis.analyzeValue(value);
                        }
                        threadSampledRows++;
                        totalSampled++;
                        rowsProcessed++;
                        
                        // Log progress every 5000 rows
                        if (rowsProcessed % 5000 == 0) {
                            final long currentTime = System.currentTimeMillis();
                            final double rowsPerSec = rowsProcessed * 1000.0 / (currentTime - fetchStart);
                            LOGGER.debug("CHUNK {} progress: {} rows processed, {:.0f} rows/sec", 
                                       thread + 1, rowsProcessed, rowsPerSec);
                        }
                    }
                    
                    final long fetchTime = System.currentTimeMillis() - fetchStart;
                    final long chunkTime = System.currentTimeMillis() - chunkStart;
                    chunkTimes[thread] = chunkTime;
                    chunkSamples[thread] = threadSampledRows;
                    
                    final double chunkRowsPerSec = threadSampledRows * 1000.0 / Math.max(fetchTime, 1);
                    final double chunkMBPerSec = (threadSampledRows * numberColumns.size() * 8) / (1024.0 * 1024.0) / Math.max(fetchTime / 1000.0, 0.001);
                    
                    LOGGER.info("CHUNK {} COMPLETE: {} rows in {} ms (exec: {}ms, fetch: {}ms) | {:.0f} rows/sec, {:.2f} MB/sec", 
                               thread + 1, threadSampledRows, chunkTime, executeTime, fetchTime, chunkRowsPerSec, chunkMBPerSec);
                }
            } catch (final SQLException e) {
                final long chunkTime = System.currentTimeMillis() - chunkStart;
                LOGGER.error("CHUNK {} FAILED after {} ms: {}", thread + 1, chunkTime, e.getMessage());
                chunkTimes[thread] = chunkTime;
                chunkSamples[thread] = 0;
            }
        }
        
        final long totalExtractionTime = System.currentTimeMillis() - extractionStartTime;
        final long totalParallelTime = System.currentTimeMillis() - parallelStartTime;
        
        // Performance summary
        LOGGER.info("=== PARALLEL EXTRACTION SUMMARY ===");
        LOGGER.info("Total samples extracted: {}", totalSampled);
        LOGGER.info("Total extraction time: {} ms", totalExtractionTime);
        LOGGER.info("Total parallel operation time: {} ms", totalParallelTime);
        
        if (totalSampled > 0) {
            final double totalRowsPerSec = totalSampled * 1000.0 / Math.max(totalExtractionTime, 1);
            final double totalMBPerSec = (totalSampled * numberColumns.size() * 8) / (1024.0 * 1024.0) / Math.max(totalExtractionTime / 1000.0, 0.001);
            LOGGER.info("Overall throughput: {:.0f} rows/sec, {:.2f} MB/sec", totalRowsPerSec, totalMBPerSec);
        }
        
        // Individual chunk performance
        for (int i = 0; i < numThreads; i++) {
            if (chunkSamples[i] > 0) {
                final double chunkRowsPerSec = chunkSamples[i] * 1000.0 / Math.max(chunkTimes[i], 1);
                LOGGER.info("Chunk {} performance: {} rows in {} ms ({:.0f} rows/sec)", 
                           i + 1, chunkSamples[i], chunkTimes[i], chunkRowsPerSec);
            }
        }
        
        LOGGER.info("=== PARALLEL EXTRACTION END ===");
        return true;
    }
    
    /**
     * Extract data using optimized sequential query (fallback) with performance monitoring
     */
    private void extractDataSequential(final List<String> numberColumns, final List<NumberColumnAnalysis> results, final int sampleSize, final long rowCount) throws SQLException {
        final long sequentialStartTime = System.currentTimeMillis();
        LOGGER.info("=== SEQUENTIAL EXTRACTION START ===");
        
        final StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < numberColumns.size(); i++) {
            if (i > 0) columnList.append(", ");
            columnList.append(numberColumns.get(i));
        }
        
        final double samplePercent = calculateSamplePercent(sampleSize, rowCount);
        LOGGER.info("Sequential extraction plan: {} samples from {} total rows ({}% sample rate)", 
                   sampleSize, rowCount, String.format("%.4f", samplePercent));
        
        // Use TABLESAMPLE instead of SAMPLE BLOCK for better performance
        final String sql = String.format(
            "SELECT /*+ FIRST_ROWS(%d) */ %s FROM %s.%s TABLESAMPLE(%.4f) WHERE ROWNUM <= ?",
            sampleSize, columnList.toString(), sourceSchema, sourceObject, samplePercent);
        
        LOGGER.debug("Sequential SQL: {}", sql.length() > 300 ? sql.substring(0, 300) + "..." : sql);
        
        final long sqlPrepStart = System.currentTimeMillis();
        try (final PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, sampleSize);
            
            final long sqlPrepTime = System.currentTimeMillis() - sqlPrepStart;
            LOGGER.info("SQL preparation: {} ms", sqlPrepTime);
            
            final long executeStart = System.currentTimeMillis();
            try (final ResultSet rs = ps.executeQuery()) {
                final long executeTime = System.currentTimeMillis() - executeStart;
                LOGGER.info("SQL execution: {} ms", executeTime);
                
                final long fetchStart = System.currentTimeMillis();
                int sampledRows = 0;
                
                while (rs.next() && sampledRows < sampleSize) {
                    for (int i = 0; i < numberColumns.size(); i++) {
                        final NumberColumnAnalysis analysis = results.get(i);
                        final Object value = rs.getObject(i + 1);
                        analysis.analyzeValue(value);
                    }
                    sampledRows++;
                    
                    // Log progress every 10000 rows
                    if (sampledRows % 10000 == 0) {
                        final long currentTime = System.currentTimeMillis();
                        final double rowsPerSec = sampledRows * 1000.0 / (currentTime - fetchStart);
                        LOGGER.info("Sequential progress: {} rows processed, {:.0f} rows/sec", 
                                   sampledRows, rowsPerSec);
                    }
                }
                
                final long fetchTime = System.currentTimeMillis() - fetchStart;
                final long totalTime = System.currentTimeMillis() - sequentialStartTime;
                
                final double totalRowsPerSec = sampledRows * 1000.0 / Math.max(fetchTime, 1);
                final double totalMBPerSec = (sampledRows * numberColumns.size() * 8) / (1024.0 * 1024.0) / Math.max(fetchTime / 1000.0, 0.001);
                
                LOGGER.info("=== SEQUENTIAL EXTRACTION SUMMARY ===");
                LOGGER.info("Total samples extracted: {}", sampledRows);
                LOGGER.info("SQL execution time: {} ms", executeTime);
                LOGGER.info("Data fetch time: {} ms", fetchTime);
                LOGGER.info("Total operation time: {} ms", totalTime);
                LOGGER.info("Overall throughput: {:.0f} rows/sec, {:.2f} MB/sec", totalRowsPerSec, totalMBPerSec);
                LOGGER.info("=== SEQUENTIAL EXTRACTION END ===");
            }
        }
    }
    
    /**
     * Check if a column is numeric (suitable for range queries)
     */
    private boolean isNumericColumn(final String columnName) throws SQLException {
        final String sql = "SELECT data_type FROM all_tab_columns WHERE owner = UPPER(?) AND table_name = UPPER(?) AND column_name = UPPER(?)";
        
        try (final PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            ps.setString(2, sourceObject);
            ps.setString(3, columnName);
            
            try (final ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    final String dataType = rs.getString("data_type");
                    return "NUMBER".equals(dataType);
                }
            }
        } catch (final SQLException e) {
            LOGGER.debug("Failed to check if column {} is numeric: {}", columnName, e.getMessage());
        }
        
        return false;
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
        
        // Fallback: Oracle data dictionary with optimized two-step approach
        if (uniqueIndexes.isEmpty()) {
            LOGGER.info("No unique indexes found via DatabaseMetaData, trying optimized Oracle data dictionary queries for {}.{}", 
                       sourceSchema, sourceObject);
            
            // Step 1: Get unique index names only (fast query) 
            final String uniqueIndexNamesSql = 
                "SELECT /*+ FIRST_ROWS(20) */ index_name " +
                "FROM all_indexes " +
                "WHERE owner = UPPER(?) AND table_name = UPPER(?) AND uniqueness = 'UNIQUE' " +
                "AND ROWNUM <= 50"; // More generous limit for programmatic access
            
            try (final PreparedStatement ps1 = connection.prepareStatement(uniqueIndexNamesSql)) {
                ps1.setString(1, sourceSchema);
                ps1.setString(2, sourceObject);
                ps1.setQueryTimeout(10); // Quick timeout for index names
                
                final List<String> indexNames = new ArrayList<>();
                try (final ResultSet rs1 = ps1.executeQuery()) {
                    while (rs1.next()) {
                        indexNames.add(rs1.getString("index_name"));
                    }
                    LOGGER.debug("Found {} unique indexes for {}.{}", indexNames.size(), sourceSchema, sourceObject);
                }
                
                // Step 2: Get columns for each index separately (avoids slow JOIN)
                if (!indexNames.isEmpty()) {
                    final String indexColumnsSql = 
                        "SELECT /*+ FIRST_ROWS */ column_name " +
                        "FROM all_ind_columns " +
                        "WHERE index_owner = UPPER(?) AND index_name = ? " +
                        "ORDER BY column_position";
                    
                    try (final PreparedStatement ps2 = connection.prepareStatement(indexColumnsSql)) {
                        ps2.setQueryTimeout(5); // Quick timeout per index
                        
                        for (final String indexName : indexNames) {
                            ps2.setString(1, sourceSchema);
                            ps2.setString(2, indexName);
                            
                            final List<String> indexColumns = new ArrayList<>();
                            try (final ResultSet rs2 = ps2.executeQuery()) {
                                while (rs2.next()) {
                                    indexColumns.add(rs2.getString("column_name"));
                                }
                                if (!indexColumns.isEmpty()) {
                                    uniqueIndexes.add(new UniqueIndex(indexName, indexColumns));
                                    LOGGER.debug("Found unique index: {} with {} columns", indexName, indexColumns.size());
                                }
                            } catch (SQLException e) {
                                LOGGER.debug("Failed to get columns for index {}: {}", indexName, e.getMessage());
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                LOGGER.warn("Failed to query unique indexes for {}.{}: {}", sourceSchema, sourceObject, e.getMessage());
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
                int recPrecision = Math.max(10, maxPrecision);
                int recScale = Math.max(2, maxScale);
                // Clamp to Iceberg max precision 38 and keep scale <= precision
                if (recPrecision > 38) recPrecision = 38;
                if (recScale > recPrecision) recScale = recPrecision;
                if (recScale < 0) recScale = 0;
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