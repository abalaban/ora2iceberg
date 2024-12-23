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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.model.CreateNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.CreateNamespaceResponse;
import software.amazon.awssdk.services.s3tables.model.GetNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.GetNamespaceResponse;
import software.amazon.awssdk.services.s3tables.model.NotFoundException;
import software.amazon.awssdk.services.s3tables.model.S3TablesException;

public class AwsUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(AwsUtil.class);

	static boolean checkAndCreateGlueDbIfMissed(final String dbName) throws IOException {
		try {
			final GlueClient glue = GlueClient.builder().build();
			final GetDatabaseRequest dbRequest = GetDatabaseRequest.builder()
								.name(dbName)
								.build();
			GetDatabaseResponse response = null;
			try {
				response = glue.getDatabase(dbRequest);
			} catch(EntityNotFoundException nfe) {
				LOGGER.warn("database {} not found in default Glue catalog!", dbName);
			}
			if (response != null &&
				response.database() != null &&
				StringUtils.equalsIgnoreCase(dbName, response.database().name())) {
				return true;
			} else {
				final DatabaseInput di = DatabaseInput.builder()
						.name(dbName)
						.build();
				final CreateDatabaseRequest crDbRequest = CreateDatabaseRequest.builder()
						.databaseInput(di)
						.build();
				final CreateDatabaseResponse crResponse = glue.createDatabase(crDbRequest);
				if (crResponse != null &&
						crResponse.sdkHttpResponse() != null &&
						crResponse.sdkHttpResponse().isSuccessful()) {
					return true;
				}
			}
		} catch (GlueException ge) {
			throw new IOException(ge);
		}
		return false;
	}

	static boolean checkAndCreateS3TablesDbIfMissed(final String dbName, final String arn) throws IOException {
		try {
			final S3TablesClient s3Tables = S3TablesClient.builder().build();
			final GetNamespaceRequest nsRequest = GetNamespaceRequest.builder()
					.tableBucketARN(arn)
					.namespace(dbName)
					.build();
			GetNamespaceResponse response = null;
			try {
				response = s3Tables.getNamespace(nsRequest);
			} catch(NotFoundException nfe) {
				LOGGER.warn("namespace {} not found in default S3 Tables catalog!", dbName);
			}
			if (response != null &&
					response.hasNamespace()) {
				boolean result = false;
				for (final String ns : response.namespace()) {
					if (StringUtils.equals(ns, dbName)) {
						result = true;
						break;
					}
				}
				if (result) {
					return true;
				}
				final CreateNamespaceRequest crNsRequest =  CreateNamespaceRequest.builder()
						.namespace(dbName)
						.build();
				final CreateNamespaceResponse crNsResponse = s3Tables.createNamespace(crNsRequest);
				if (crNsResponse != null &&
						crNsResponse.sdkHttpResponse() != null &&
								crNsResponse.sdkHttpResponse().isSuccessful()) {
					return true;
				}
			}
		} catch (S3TablesException s3e) {
			throw new IOException(s3e);
		}
		return false;
	}

}
