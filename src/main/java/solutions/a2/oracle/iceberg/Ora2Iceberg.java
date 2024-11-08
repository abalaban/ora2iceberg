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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Ora2Iceberg entry point
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class Ora2Iceberg {

	private static final Logger LOGGER = LoggerFactory.getLogger(Ora2Iceberg.class);
	private static final String ROWID_KEY = "ORA_ROW_ID";
	private static final Pattern SQL_EXPRESSION = Pattern.compile(
			"(.*?)SELECT(.*?)FROM(.*?)",
			Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	private static final long MAX_FILE_SIZE = 0x08000000;

	//TODO - do we need to add Snowflake and Glue catalogs?
	private static final String CATALOG_IMPL_REST = "REST";
	private static final String CATALOG_IMPL_JDBC = "JDBC";
	private static final String CATALOG_IMPL_HADOOP = "HADOOP";
	private static final String CATALOG_IMPL_HIVE = "HIVE";
	private static final String CATALOG_IMPL_NESSIE = "NESSIE";
	private static final String CATALOG_IMPL_GLUE = "GLUE";
	private static final String CATALOG_IMPL_DYNAMODB = "DYNAMODB";
	private static final Map<String, String> CATALOG_IMPL = new HashMap<>();
	static {
		CATALOG_IMPL.put(CATALOG_IMPL_REST, "org.apache.iceberg.rest.RESTCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_JDBC, "org.apache.iceberg.jdbc.JdbcCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_HADOOP, "org.apache.iceberg.hadoop.HadoopCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_HIVE, "org.apache.iceberg.hive.HiveCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_NESSIE, "org.apache.iceberg.nessie.NessieCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_GLUE, "org.apache.iceberg.aws.glue.GlueCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_DYNAMODB, "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog");
	}

	private static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
	private static final String PREFIX_POSTGRESQL = "jdbc:postgresql:";

	public static void main(String[] argv) {
		LOGGER.info("Starting...");

		// Command line options
		final Options options = new Options();
		setupCliOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			formatter.printHelp(Ora2Iceberg.class.getCanonicalName(), options);
			System.exit(1);
		}

		final Map<String, String> catalogProps = new HashMap<>();
		catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, cmd.getOptionValue("iceberg-warehouse-location"));
		catalogProps.put(CatalogProperties.URI, cmd.getOptionValue("iceberg-catalog-uri"));
		switch (StringUtils.upperCase(cmd.getOptionValue("iceberg-catalog-implementation"))) {
			case CATALOG_IMPL_REST:
			case CATALOG_IMPL_JDBC:
			case CATALOG_IMPL_HADOOP:
			case CATALOG_IMPL_HIVE:
			case CATALOG_IMPL_NESSIE:
			case CATALOG_IMPL_GLUE:
			case CATALOG_IMPL_DYNAMODB:
				catalogProps.put(CatalogProperties.CATALOG_IMPL,
						CATALOG_IMPL.get(StringUtils.upperCase(cmd.getOptionValue("iceberg-catalog-implementation"))));
				break;
			default:
				try {
					final Class<?> clazz = Class.forName(cmd.getOptionValue("iceberg-catalog-implementation"));
					if (!clazz.isAssignableFrom(BaseMetastoreCatalog.class)) {
						LOGGER.error("Class {} must extend {}!",
								clazz.getCanonicalName(),
								BaseMetastoreCatalog.class.getCanonicalName());
						System.exit(1);
					}
					catalogProps.put(CatalogProperties.CATALOG_IMPL,
							cmd.getOptionValue("iceberg-catalog-implementation"));
				} catch (ClassNotFoundException cnfe) {
					LOGGER.error("Unable to load class {} specified as an Apache Iceberg catalog implementation!\n" +
									"The following exception occured:\n{}\n",
							cmd.getOptionValue("iceberg-catalog-uri"), cnfe.getMessage());
					System.exit(1);
				}
		}
		catalogProps.put(CatalogProperties.URI, cmd.getOptionValue("iceberg-catalog-uri"));
		final String[] params = cmd.getOptionValues("P");
		if (params != null && params.length > 0) {
			if (params.length % 2 == 0) {
				for (int i = 0; i < params.length; i+=2) {
					catalogProps.put(params[i], params[i + 1]);
				}
			} else {
				LOGGER.error("Unable to parse from command line values of Apache Iceberg Catalog properties!\n" +
						"Please check parameters!");
				System.exit(1);
			}
		}
		if (StringUtils.equals(CATALOG_IMPL_JDBC, StringUtils.upperCase(cmd.getOptionValue("iceberg-catalog-implementation"))) &&
				StringUtils.startsWith(catalogProps.get(CatalogProperties.URI), PREFIX_POSTGRESQL)) {
			if (!isDriverLoaded(DRIVER_POSTGRESQL)) {
				try {
					Class.forName(DRIVER_POSTGRESQL);
				} catch (ClassNotFoundException cnf) { }
			}
		}
		BaseMetastoreCatalog catalog = null;
		try {
			final Class<?> clazz = Class.forName(catalogProps.get(CatalogProperties.CATALOG_IMPL));
			final Constructor<?> constructor = clazz.getConstructor();
			catalog = (BaseMetastoreCatalog) constructor.newInstance();
			if (catalog instanceof Configurable) {
				//EcsCatalog, GlueCatalog, JdbcCatalog, NessieCatalog, RESTCatalog, RESTSessionCatalog, SnowflakeCatalog
				((Configurable<Object>) catalog).setConf(new Configuration());
			}
			catalog.initialize(cmd.getOptionValue("iceberg-catalog-name"), catalogProps);
		} catch (ClassNotFoundException cnfe) {
			LOGGER.error("Unable to load class {} specified as an Apache Iceberg catalog implementation!\n" +
							"The following exception occured:\n{}\n",
					catalogProps.get(CatalogProperties.CATALOG_IMPL), cnfe.getMessage());
			System.exit(1);
		} catch (NoSuchMethodException | SecurityException ce) {
			final StringBuilder sb = new StringBuilder(0x400);
			sb.append("\n");
			Arrays.asList(ce.getStackTrace()).forEach(ste -> sb.append(ste.toString()));
			LOGGER.error("Unable to find no-arg constructor for class {} specified as an Apache Iceberg catalog implementation!\n" +
							"The following exception occured:\n{}\n{}",
					catalogProps.get(CatalogProperties.CATALOG_IMPL), ce.getMessage(), sb.toString());
			System.exit(1);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ie) {
			final StringBuilder sb = new StringBuilder(0x400);
			sb.append("\n");
			Arrays.asList(ie.getStackTrace()).forEach(ste -> sb.append(ste.toString()));
			LOGGER.error("Unable to instantiate constructor for class {} specified as an Apache Iceberg catalog implementation!\n" +
							"The following exception occured:\n{}\n{}",
					catalogProps.get(CatalogProperties.CATALOG_IMPL), ie.getMessage(), sb.toString());
			System.exit(1);
		}
		LOGGER.info(
				"\n=====================\n" +
						"Connected to Apache Iceberg Catalog {} located on {}" +
						"\n=====================\n",
				catalog.name(), catalogProps.get(CatalogProperties.URI));

		final String sourceUrl = cmd.getOptionValue("source-jdbc-url");
		final String sourceUser = cmd.getOptionValue("source-user");
		final String sourcePassword = cmd.getOptionValue("source-password");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(sourceUrl, sourceUser, sourcePassword);
		} catch (SQLException sqle) {
			final StringBuilder sb = new StringBuilder(0x400);
			sb.append("\n");
			Arrays.asList(sqle.getStackTrace()).forEach(ste -> sb.append(ste.toString()));
			LOGGER.error("Unable to connect to Oracle Database using jdbcUrl '{}' as user '{}' with password '{}'!\n" +
							"Exception: {}{}",
					sourceUrl, sourceUser, sourcePassword, sqle.getMessage(), sb.toString());
			System.exit(1);
		}

		try {
			final DatabaseMetaData dbMetaData = connection.getMetaData();
			LOGGER.info(
					"\n=====================\n" +
							"Connected to {}{}\nusing {} {}" +
							"\n=====================\n",
					dbMetaData.getDatabaseProductName(), dbMetaData.getDatabaseProductVersion(),
					dbMetaData.getDriverName(), dbMetaData.getDriverVersion());
			final String sourceSchema;
			if (StringUtils.isBlank(cmd.getOptionValue("source-schema"))) {
				sourceSchema = dbMetaData.getUserName();
			} else {
				if (StringUtils.startsWith(cmd.getOptionValue("source-schema"), "\"") &&
						StringUtils.endsWith(cmd.getOptionValue("source-schema"), "\"")) {
					sourceSchema = cmd.getOptionValue("source-schema");
				} else {
					sourceSchema = StringUtils.upperCase(cmd.getOptionValue("source-schema"));
				}
			}

			final String sourceObject;
			final boolean isTableOrView;
			if (StringUtils.containsWhitespace(cmd.getOptionValue("source-object"))) {
				isTableOrView = false;
				if (SQL_EXPRESSION.matcher(cmd.getOptionValue("source-object")).matches()) {
					sourceObject = cmd.getOptionValue("source-object");
				} else {
					sourceObject = null;
					LOGGER.error(
							"\n=====================\n" +
									"'{}' is not a valid SQL SELECT statement!" +
									"\n=====================\n",
							cmd.getOptionValue("source-object"));
					System.exit(1);
				}
			} else {
				isTableOrView = true;
				if (StringUtils.startsWith(cmd.getOptionValue("source-object"), "\"") &&
						StringUtils.endsWith(cmd.getOptionValue("source-object"), "\"")) {
					sourceObject = cmd.getOptionValue("source-object");
				} else {
					sourceObject = StringUtils.upperCase(cmd.getOptionValue("source-object"));
				}
			}

			final String icebergTableName;
			if (StringUtils.isBlank(cmd.getOptionValue("iceberg-table-name")) && !isTableOrView) {
				icebergTableName = null;
				LOGGER.error(
						"\n=====================\n" +
								"Must specify destination table using -T/--iceberg-table-name name when using SQL STATEMENT as source!" +
								"\n=====================\n",
						cmd.getOptionValue("source-object"));
				System.exit(1);
			} else if (StringUtils.isBlank(cmd.getOptionValue("iceberg-table-name"))) {
				icebergTableName = sourceObject;
			} else {
				icebergTableName = cmd.getOptionValue("iceberg-table-name");
			}

			final TableIdentifier icebergTable;
			switch (StringUtils.upperCase(cmd.getOptionValue("iceberg-catalog-implementation"))) {
				case CATALOG_IMPL_NESSIE:
					// Nessie namespaces are implicit and do not need to be explicitly created or deleted.
					// The create and delete namespace methods are no-ops for the NessieCatalog.
					icebergTable = TableIdentifier.of(icebergTableName);
					break;
				default:
					final Namespace namespace;
					if (StringUtils.isBlank(cmd.getOptionValue("iceberg-namespace"))) {
						namespace = Namespace.of(sourceSchema);
					} else {
						namespace = Namespace.of(cmd.getOptionValue("iceberg-namespace"));
					}
					icebergTable = TableIdentifier.of(namespace, icebergTableName);
					break;
			}

			if (catalog.tableExists(icebergTable)) {
				//TODO
				//TODO need option to purge or stop
				//TODO currently just purge and continue
				//TODO
				LOGGER.info("Dropping table {} from catalog {}", icebergTable.name(), catalog.name());
				if (!catalog.dropTable(icebergTable, true)) {
					LOGGER.error("Unable to drop table {} from catalog {}", icebergTable.name(), catalog.name());
					System.exit(1);
				}
			}

			final Set<String> idColumnNames;
			if (cmd.getOptionValues("I") == null || cmd.getOptionValues("I").length == 0) {
				idColumnNames = null;
			} else {
				idColumnNames = Arrays
						.stream(cmd.getOptionValues("I"))
						.collect(Collectors.toCollection(HashSet::new));
			}
			long maxFileSize;
			if (cmd.hasOption("iceberg-max-file-size")) {
				try {
					maxFileSize = ((Number) cmd.getParsedOptionValue("iceberg-max-file-size")).longValue();
				} catch (ParseException pe) {
					maxFileSize = MAX_FILE_SIZE;
					LOGGER.error(
							"Unable to parse value '{}' of option '{}'! Default {} will be used.",
							cmd.getOptionValue("iceberg-max-file-size"), "iceberg-max-file-size", MAX_FILE_SIZE);
				}
			} else {
				maxFileSize = MAX_FILE_SIZE;
			}

			//TODO
			//TODO options for partition!!!
			//TODO
//			final Set<String> partColumnNames;
			final List<Triple<String, String, Integer>> partColumnNames;

			if (cmd.getOptionValues("B") == null || cmd.getOptionValues("B").length == 0) {
				partColumnNames = null;
			} else {
//				partColumnNames = Arrays
//						.stream(cmd.getOptionValues("B"))
//						.collect(Collectors.toCollection(HashSet::new));
				partColumnNames = new ArrayList<>();
				final String[] partParams = cmd.getOptionValues("B");

				if (partParams.length % 2 == 0) {
					for (int i = 0; i < partParams.length; i+=2) {
						final String columnName = partParams[i];
						String partColumnType = partParams[i + 1];
						int partThirdParam = -1;
						if (StringUtils.contains(partColumnType, ",")) {
							// Parse 3rd numeric
							//TODO
							partThirdParam = Integer.parseInt(StringUtils.substringAfterLast(partColumnType, ","));
							partColumnType = StringUtils.substringBefore(partColumnType, ",");


							//TODO set numValue
						}
						partColumnNames.add(new ImmutableTriple<>(columnName, partColumnType, partThirdParam));
					}
					} else {
						LOGGER.error("Unable to parse from command line values of Apache Iceberg Catalog properties!\n" +
								"Please check parameters!");
						System.exit(1);
					}



				}


			final StructAndDataMover sdm = new StructAndDataMover(
					dbMetaData, sourceSchema, sourceObject, isTableOrView,
					catalog, icebergTable, idColumnNames, partColumnNames, maxFileSize);

			sdm.loadData();

		} catch (SQLException sqle) {
			//TODO
			//TODO
			//TODO
		}
	}

	private static void setupCliOptions(final Options options) {
		// Source connection
		final Option sourceJdbcUrl = Option.builder("u")
				.longOpt("source-jdbc-url")
				.hasArg(true)
				.required(true)
				.desc("Oracle JDBC URL of source connection")
				.build();
		options.addOption(sourceJdbcUrl);

		final Option sourceUser = Option.builder("u")
				.longOpt("source-user")
				.hasArg(true)
				.required(true)
				.desc("Oracle user for source connection ")
				.build();
		options.addOption(sourceUser);

		final Option sourcePassword = Option.builder("p")
				.longOpt("source-password")
				.hasArg(true)
				.required(true)
				.desc("Password for source connection")
				.build();
		options.addOption(sourcePassword);

		// Source object description
		final Option sourceSchema = Option.builder("s")
				.longOpt("source-schema")
				.hasArg(true)
				.required(false)
				.desc("Source schema name. If not specified - value of <source-user> is used")
				.build();
		options.addOption(sourceSchema);

		final Option sourceObject = Option.builder("o")
				.longOpt("source-object")
				.hasArg(true)
				.required(true)
				.desc("The name of source table or view, or valid SQL SELECT statement to query data")
				.build();
		options.addOption(sourceObject);

		final Option whereClause = Option.builder("w")
				.longOpt("where-clause")
				.hasArg(true)
				.required(false)
				.desc("Optional where clause for the <source-object>. Valid only when <source-object> points to table or view.")
				.build();
		options.addOption(whereClause);

		final Option addRowId = Option.builder("r")
				.longOpt("add-rowid-to-iceberg")
				.hasArg(false)
				.required(false)
				.desc("When specified ROWID pseudocolumn is added to destination as VARCHAR column with name ORA_ROW_ID ansd used as ID. Valid only when <source-object> points to a RDBMS table")
				.build();
		options.addOption(addRowId);

		final Option rowIdColumnName = Option.builder("n")
				.longOpt("rowid-column-name")
				.hasArg(true)
				.required(false)
				.desc("Specifies the name for the column in destination table storing the source ROWIDs. Default - " + ROWID_KEY)
				.build();
		options.addOption(rowIdColumnName);

		final Option catalogImpl = Option.builder("C")
				.longOpt("iceberg-catalog-implementation")
				.hasArg(true)
				.required(true)
				.desc("One of " +
						CATALOG_IMPL_REST + "," +
						CATALOG_IMPL_JDBC + "," +
						CATALOG_IMPL_HADOOP + "," +
						CATALOG_IMPL_HIVE + "," +
						CATALOG_IMPL_NESSIE + "," +
						CATALOG_IMPL_GLUE + "," +
						CATALOG_IMPL_DYNAMODB +
						" or full-qualified name of class extending org.apache.iceberg.BaseMetastoreCatalog.")
				.build();
		options.addOption(catalogImpl);

		final Option catalogName = Option.builder("N")
				.longOpt("iceberg-catalog-name")
				.hasArg(true)
				.required(true)
				.desc("Apache Iceberg Catalog name")
				.build();
		options.addOption(catalogName);

		final Option catalogUri = Option.builder("U")
				.longOpt("iceberg-catalog-uri")
				.hasArg(true)
				.required(true)
				.desc("Apache Iceberg Catalog URI")
				.build();
		options.addOption(catalogUri);

		final Option catalogWarehouse = Option.builder("W")
				.longOpt("iceberg-warehouse-location")
				.hasArg(true)
				.required(true)
				.desc("Apache Iceberg warehouse location")
				.build();
		options.addOption(catalogWarehouse);

		final Option catalogProperties = Option.builder("P")
				.argName("iceberg-catalog-properties")
				.hasArgs()
				.valueSeparator('=')
				.desc("Additional properties for Apache Iceberg catalog implementation")
				.build();
		options.addOption(catalogProperties);

		final Option namespace = Option.builder("A")
				.longOpt("iceberg-namespace")
				.hasArg(true)
				.required(false)
				.desc("Apache Iceberg Catalog namespace. If not specified - value of source schema will used.")
				.build();
		options.addOption(namespace);

		final Option icebergTable = Option.builder("T")
				.longOpt("iceberg-table-name")
				.hasArg(true)
				.required(false)
				.desc("Apache Iceberg table name. When not specified and <source-object> is view or table, name of <source-object> is used.")
				.build();
		options.addOption(icebergTable);

		final Option idColumns = Option.builder("I")
				.argName("iceberg-table-id-columns")
				.hasArgs()
				.desc("Apache Iceberg table identifier column names")
				.build();
		options.addOption(idColumns);

		final Option partitionBy = Option.builder("B")
				.argName("iceberg-table-partition-by")
				.hasArgs()
				.valueSeparator('=')
				.desc("Partitioning definition for table")
				.build();
		options.addOption(partitionBy);

		final Option maxFileSize = Option.builder("Z")
				.longOpt("iceberg-max-file-size")
				.type(Number.class)
				.hasArg()
				.desc("Max file size. Default - " + MAX_FILE_SIZE)
				.build();
		options.addOption(maxFileSize);

		//TODO
		//TODO option for column datatype remap, especially NUMBER to INT/LONG !!!
		//TODO
	}

	private static boolean isDriverLoaded(final String driverClass) {
		final Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
		while (availableDrivers.hasMoreElements()) {
			final Driver driver = availableDrivers.nextElement();
			if (StringUtils.equals(driverClass, driver.getClass().getCanonicalName())) {
				return true;
			}
		}
		return false;
	}

}