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
 * ExpImpPipe entry point
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class Ora2Iceberg {

	private static final Logger LOGGER = LoggerFactory.getLogger(Ora2Iceberg.class);
	private static final String ROWID_KEY = "ORA_ROW_ID";

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
		final Option sourceJdbcUrl = new Option("src", "source-jdbc-url", true,
				"Oracle JDBC URL of source connection");
		sourceJdbcUrl.setRequired(true);
		options.addOption(sourceJdbcUrl);
		final Option sourceUser = new Option("u", "source-user", true,
				"Oracle user for source connection");
		sourceUser.setRequired(true);
		options.addOption(sourceUser);
		final Option sourcePassword = new Option("p", "source-password", true,
				"Password for source connection");
		sourcePassword.setRequired(true);
		options.addOption(sourcePassword);
		// Destination connection
		final Option destJdbcUrl = new Option("dest", "destination-jdbc-url", true,
				"Oracle JDBC URL of destination connection");
		destJdbcUrl.setRequired(true);
		options.addOption(destJdbcUrl);
		final Option destUser = new Option("U", "destination-user", true,
				"Oracle user for destination connection");
		destUser.setRequired(true);
		options.addOption(destUser);
		final Option destPassword = new Option("P", "destination-password", true,
				"Password for destination connection");
		destPassword.setRequired(true);
		options.addOption(destPassword);
		// Object description
		final Option sourceSchema = new Option("s", "source-schema", true,
				"Source schema name");
		sourceSchema.setRequired(true);
		options.addOption(sourceSchema);
		final Option sourceTable = new Option("t", "source-table", true,
				"Source table name");
		sourceTable.setRequired(true);
		options.addOption(sourceTable);
		final Option destSchema = new Option("S", "destination-schema", true,
				"Destination schema name, if not specified value of --source-schema used");
		destSchema.setRequired(false);
		options.addOption(destSchema);
		final Option destTable = new Option("T", "destination-table", true,
				"Destination table name, if not specified value of --source-table used");
		destTable.setRequired(false);
		options.addOption(destTable);
		final Option whereClause = new Option("w", "where-clause", true,
				"Optional where clause for source table");
		whereClause.setRequired(false);
		options.addOption(whereClause);
		final Option addRowId = new Option("r", "add-rowid-to-dest", false,
				"When specified ROWID pseudocolumn is added to destination as VARCHAR column with name ORA_ROW_ID");
		addRowId.setRequired(false);
		options.addOption(addRowId);
		final Option rowIdColumnName = new Option("n", "rowid-column-name", true,
				"Specifies the name for the column in destination table storing the source ROWIDs. Default - " + ROWID_KEY);
		rowIdColumnName.setRequired(false);
		options.addOption(rowIdColumnName);

	}
}
