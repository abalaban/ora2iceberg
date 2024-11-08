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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;
import oracle.sql.NUMBER;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class StructAndDataMover {

	private static final Logger LOGGER = LoggerFactory.getLogger(StructAndDataMover.class);
	private static final int TYPE_POS = 0;
	private static final int PRECISION_POS = 1;
	private static final int SCALE_POS = 2;
	private static final int NULL_POS = 3;
	private static final int INFO_SIZE = 4;

	private final Connection connection;
	private final boolean isTableOrView;
	private final String sourceSchema;
	private final String sourceObject;
	private final Map<String, int[]> columnsMap;
	private final Table table;
	private final long targetFileSize;


	StructAndDataMover(
			final DatabaseMetaData dbMetaData,
			final String sourceSchema,
			final String sourceObject,
			final boolean isTableOrView,
			final BaseMetastoreCatalog catalog,
			final TableIdentifier icebergTable,
			final Set<String> idColumnNames,
			//TODO
			//final Set<String> partitionDefs,
			final List<Triple<String, String, Integer>>  partitionDefs,
			final long targetFileSize) throws SQLException {
		connection = dbMetaData.getConnection();
		columnsMap = new HashMap<>();
		this.isTableOrView = isTableOrView;
		this.sourceSchema = sourceSchema;
		this.sourceObject = sourceObject;
		this.targetFileSize = targetFileSize;

		final String sourceCatalog;
		if (isTableOrView) {
			final ResultSet tables = dbMetaData.getTables(null, sourceSchema, sourceObject, null);
			if (tables.next()) {
				sourceCatalog = tables.getString("TABLE_CAT");
				LOGGER.info("Working with {} {}.{}",
						tables.getString("TABLE_TYPE"), sourceSchema, sourceObject);
			} else {
				LOGGER.error(
						"\n=====================\n" +
								"Unable to access {}.{} !" +
								"\n=====================\n",
						sourceSchema, sourceObject);
				throw new SQLException();
			}
			tables.close();

			final List<Types.NestedField> allColumns = new ArrayList<>();
			final Set<Integer> pkIds = new HashSet<>();
			int columnId = 0;

			final boolean idColumnsPresent = idColumnNames != null && !idColumnNames.isEmpty();
			final ResultSet columns = dbMetaData.getColumns(sourceCatalog, sourceSchema, sourceObject, "%");
			while (columns.next()) {
				final String columnName = columns.getString("COLUMN_NAME");
				final int jdbcType = columns.getInt("DATA_TYPE");
				final boolean nullable = StringUtils.equals("YES", columns.getString("IS_NULLABLE"));
				final int precision = columns.getInt("COLUMN_SIZE");
				final int scale = columns.getInt("DECIMAL_DIGITS");
				boolean addColumn = false;
				final Type type;
				final int mappedType;
				switch (jdbcType) {
					case java.sql.Types.BOOLEAN:
						type = Types.BooleanType.get();
						mappedType = java.sql.Types.BOOLEAN;
						addColumn = true;
						break;
					case java.sql.Types.NUMERIC:
						if (scale == 0 && precision < 10) {
							mappedType = java.sql.Types.INTEGER;
							type = Types.IntegerType.get();
						} else if (scale == 0 && precision < 19) {
							mappedType = java.sql.Types.BIGINT;
							type = Types.LongType.get();
						} else {
							mappedType = java.sql.Types.NUMERIC;
							//TODO
							//TODO
							//TODO
							type = Types.DecimalType.of(
									precision <= 0 ? 38 : precision,
									scale < 0 ? 19: scale);
						}
						addColumn = true;
						break;
					case OracleTypes.BINARY_FLOAT:
						mappedType = java.sql.Types.FLOAT;
						type = Types.FloatType.get();
						addColumn = true;
						break;
					case OracleTypes.BINARY_DOUBLE:
						mappedType = java.sql.Types.DOUBLE;
						type = Types.DoubleType.get();
						addColumn = true;
						break;
					case java.sql.Types.VARCHAR:
						mappedType = java.sql.Types.VARCHAR;
						type = Types.StringType.get();
						addColumn = true;
						break;
					case java.sql.Types.TIMESTAMP:
						mappedType = java.sql.Types.TIMESTAMP;
						type = Types.TimestampType.withoutZone();
						addColumn = true;
						break;
					case OracleTypes.TIMESTAMPLTZ:
					case OracleTypes.TIMESTAMPTZ:
						mappedType = java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
						type = Types.TimestampType.withZone();
						addColumn = true;
						break;
					default:
						mappedType = Integer.MAX_VALUE;
						type = null;
						LOGGER.warn("Skipping column {} with jdbcType {}", columnName, jdbcType);
				}
				if (addColumn) {
					final int[] typeAndScale = new int[INFO_SIZE];
					typeAndScale[TYPE_POS] = mappedType;
					//TODO - precision!
					typeAndScale[PRECISION_POS] = mappedType != java.sql.Types.NUMERIC ? Integer.MIN_VALUE :
							precision <= 0 ? 38 : precision;
					//TODO - scale!
					typeAndScale[SCALE_POS] = mappedType != java.sql.Types.NUMERIC ? Integer.MIN_VALUE :
							scale < 0 ? 19: scale;
					typeAndScale[NULL_POS] = nullable ? 1 : 0;
					columnsMap.put(columnName, typeAndScale);
					columnId++;
					if (nullable) {
						allColumns.add(
								Types.NestedField.optional(columnId, columnName, type));
					} else {
						allColumns.add(
								Types.NestedField.required(columnId, columnName, type));
					}
					if (idColumnsPresent) {
						if (idColumnNames.contains(columnName) && !nullable) {
							pkIds.add(columnId);
						}
					}
				}
			}


			final Schema schema = pkIds.isEmpty() ? new Schema(allColumns) : new Schema(allColumns, pkIds);
			final PartitionSpec spec;
			if (partitionDefs != null) {

				//spec = PartitionSpec.builderFor(schema).identity((String) partitionDefs.toArray()[0]).build();
				PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
				for (Triple<String, String, Integer> partitionDef : partitionDefs) {
					switch (partitionDef.getMiddle()) {
						case "IDENTITY":
							specBuilder = specBuilder.identity(partitionDef.getLeft());
							break;
						case "YEAR":
							specBuilder = specBuilder.year(partitionDef.getLeft());
							break;
						case "MONTH":
							specBuilder = specBuilder.month(partitionDef.getLeft());
							break;
						case "DAY":
							specBuilder = specBuilder.day(partitionDef.getLeft());
							break;
						case "HOUR":
							specBuilder = specBuilder.hour(partitionDef.getLeft());
							break;
						case "BUCKET":
							specBuilder = specBuilder.bucket(partitionDef.getLeft(), partitionDef.getRight());
							break;
						case "TRUNCATE":
							specBuilder = specBuilder.truncate(partitionDef.getLeft(), partitionDef.getRight());
							break;

						//TODO Create Else with exception - if partition type does not exist
					}

				}

				spec = specBuilder.build();





			}  else {
				spec = PartitionSpec.unpartitioned();
			}
			table = catalog.createTable(
					icebergTable,
					schema,
					spec);
		} else {
			//TODO
			//TODO
			//TODO
			throw new SQLException("Not supported yet!");
		}
	}

	void loadData() throws SQLException {

		int partitionId = 1, taskId = 1;
		final GenericAppenderFactory af = new GenericAppenderFactory(table.schema());
		final OutputFileFactory off = OutputFileFactory.builderFor(table, partitionId, taskId).format(FileFormat.PARQUET).build();
		final PartitionKey partitionKey = new PartitionKey(table.spec(), table.spec().schema());

		if (isTableOrView) {

			PartitionedFanoutWriter<Record> partitionedFanoutWriter = new PartitionedFanoutWriter<Record>(
					table.spec(),
					//TODO - only parquet?
					FileFormat.PARQUET,
					af, off, table.io(),
					targetFileSize) {
				@Override
				protected PartitionKey partition(Record record) {
					partitionKey.partition(record);
					return partitionKey;
				}
			};

			//TODO - where clause!!!
			final PreparedStatement ps = connection.prepareStatement("select * from \"" + sourceSchema + "\".\"" + sourceObject + "\"");
			final OracleResultSet rs = (OracleResultSet) ps.executeQuery();
			//TODO - run statistic!
			//TODO - progress on screen!!!
			while (rs.next()) {
				final GenericRecord record = GenericRecord.create(table.schema());
				for (final Map.Entry<String, int[]> entry : columnsMap.entrySet()) {
					switch (entry.getValue()[TYPE_POS]) {
						case java.sql.Types.BOOLEAN:
							record.setField(entry.getKey(), rs.getBoolean(entry.getKey()));
							break;
						case java.sql.Types.INTEGER:
							record.setField(entry.getKey(), rs.getInt(entry.getKey()));
							break;
						case java.sql.Types.BIGINT:
							record.setField(entry.getKey(), rs.getLong(entry.getKey()));
							break;
						case java.sql.Types.NUMERIC:
							final NUMBER oraNum = rs.getNUMBER(entry.getKey());
							if (oraNum == null) {
								record.setField(entry.getKey(), null);
							} else {
								if (oraNum.isInf() || oraNum.isNegInf()) {
									//TODO
									//TODO - key values in output!!!
									//TODO
									LOGGER.warn(
											"\n=====================\n" +
													"Value of Oracle NUMBER column {} is {}! Setting value to {}!" +
													"\n=====================\n",
											entry.getKey(),
											oraNum.isInf() ? "Infinity" : "Negative infinity",
											entry.getValue()[NULL_POS] == 1 ? "NULL" :
													oraNum.isInf() ? "" + Float.MAX_VALUE : "" + Float.MIN_VALUE);
									if (entry.getValue()[NULL_POS] == 1) {
										record.setField(entry.getKey(), null);
									} else if (oraNum.isInf()) {
										record.setField(entry.getKey(), BigDecimal.valueOf(Float.MAX_VALUE).setScale(entry.getValue()[SCALE_POS]));
									} else {
										record.setField(entry.getKey(), BigDecimal.valueOf(Float.MIN_VALUE).setScale(entry.getValue()[SCALE_POS]));
									}
								} else {
									if (oraNum.isNull()) {
										record.setField(entry.getKey(), null);
									} else {
										final BigDecimal bd = oraNum
												.bigDecimalValue()
												.setScale(entry.getValue()[SCALE_POS], RoundingMode.HALF_UP);
										if (bd.precision() > entry.getValue()[PRECISION_POS]) {
											//TODO
											//TODO - key values in output!!!
											//TODO
											final StringBuilder oraNumFmt = new StringBuilder();
											final byte[] oraNumBytes = oraNum.getBytes();
											for (int i = 0; i < oraNumBytes.length; i++) {
												oraNumFmt
														.append(' ')
														.append(String.format("%02x", Byte.toUnsignedInt(oraNumBytes[i])));
											}
											LOGGER.warn(
													"\n=====================\n" +
															"Precision {} of Oracle NUMBER column {} with value '{}' is greater than allowed precision {}!\n" +
															"Dump value of NUMBER column ='{}'\n" +
															"Setting value to {}!" +
															"\n=====================\n",
													bd.precision(), entry.getKey(),
													oraNum.stringValue(), entry.getValue()[PRECISION_POS],
													oraNumFmt.toString(),
													entry.getValue()[NULL_POS] == 1 ? "NULL" : "" + Float.MAX_VALUE);
											if (entry.getValue()[NULL_POS] == 1) {
												record.setField(entry.getKey(), null);
											} else {
												//TODO - approximation required, not MAX_VALUE!
												record.setField(entry.getKey(), BigDecimal.valueOf(Float.MAX_VALUE).setScale(entry.getValue()[SCALE_POS]));
											}
										} else {
											record.setField(entry.getKey(), bd);
										}
									}
								}
							}
							break;
						case java.sql.Types.FLOAT:
							record.setField(entry.getKey(), rs.getFloat(entry.getKey()));
							break;
						case java.sql.Types.DOUBLE:
							record.setField(entry.getKey(), rs.getDouble(entry.getKey()));
							break;
						case java.sql.Types.TIMESTAMP:
						case java.sql.Types.TIME_WITH_TIMEZONE:
							final Timestamp ts =  rs.getTimestamp(entry.getKey());
							if (ts != null) {
								record.setField(entry.getKey(), ts.toLocalDateTime());
							} else {
								record.setField(entry.getKey(), null);
							}
							break;
						case java.sql.Types.VARCHAR:
							record.setField(entry.getKey(), rs.getString(entry.getKey()));
							break;
					}
				}
				columnsMap.forEach((columnName, jdbcType) -> {
				});
				try {
					partitionedFanoutWriter.write(record);
				} catch (IOException ioe) {
					throw new SQLException(ioe);
				}
			}

			rs.close();
			ps.close();

			AppendFiles appendFiles = table.newAppend();
			// submit datafiles to the table
			try {
				Arrays.stream(partitionedFanoutWriter.dataFiles()).forEach(appendFiles::appendFile);
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
			Snapshot newSnapshot = appendFiles.apply();
			appendFiles.commit();

		} else {
			//TODO
			//TODO
			//TODO
			throw new SQLException("Not supported yet!");
		}
	}

}