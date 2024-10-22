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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

	private static final String CATALOG_IMPL_REST = "REST";
	private static final String CATALOG_IMPL_JDBC = "JDBC";
	private static final String CATALOG_IMPL_HADOOP = "HADOOP";
	private static final String CATALOG_IMPL_HIVE = "HIVE";
	private static final String CATALOG_IMPL_NESSIE = "NESSIE";
	private static final Map<String, String> CATALOG_IMPL = new HashMap<>();
	static {
		CATALOG_IMPL.put(CATALOG_IMPL_REST, "org.apache.iceberg.rest.RESTCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_JDBC, "org.apache.iceberg.jdbc.JdbcCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_HADOOP, "org.apache.iceberg.hadoop.HadoopCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_HIVE, "org.apache.iceberg.hive.HiveCatalog");
		CATALOG_IMPL.put(CATALOG_IMPL_NESSIE, "org.apache.iceberg.nessie.NessieCatalog");
	}

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

		final String sourceUrl = cmd.getOptionValue("source-jdbc-url");
		final String sourceUser = cmd.getOptionValue("source-user");
		final String sourcePassword = cmd.getOptionValue("source-password");
		
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

		final Option catalogImpl = Option.builder("I")
				.longOpt("iceberg-catalog-implementation")
				.hasArg(true)
				.required(true)
				.desc("One of " +
						CATALOG_IMPL_REST + "," +
						CATALOG_IMPL_JDBC + "," +
						CATALOG_IMPL_HADOOP + "," +
						CATALOG_IMPL_HIVE + "," +
						CATALOG_IMPL_NESSIE + " or full-qualified name of class extending org.apache.iceberg.view.BaseMetastoreViewCatalog.")
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
				.longOpt("iceberg-catalog-properties")
				.hasArgs()
				.required(false)
				.desc("Additional properties for Apache Iceberg catalog implementation")
				.build();
		options.addOption(catalogProperties);

		final Option icebergTable = Option.builder("T")
				.longOpt("iceberg-table-name")
				.hasArg(true)
				.required(false)
				.desc("Apache Iceberg table name. When not specified and <source-object> is view or table, name of <source-object> is used.")
				.build();
		options.addOption(icebergTable);

	}
}
