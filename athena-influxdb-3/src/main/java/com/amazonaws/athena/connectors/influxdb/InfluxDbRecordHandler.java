/*-
 * #%L
 * athena-influxdb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.influxdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.SOURCE_TYPE;

public class InfluxDbRecordHandler
        extends
            RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbRecordHandler.class);
    private static final ZoneId UTC = ZoneId.of("UTC");

    private static final long MAX_PLAUSIBLE_EPOCH_MILLIS =
            LocalDate.of(2100, 1, 1).atStartOfDay(UTC).toInstant().toEpochMilli();

    private final InfluxDbConnectionFactory connectionFactory;

    public InfluxDbRecordHandler(final Map<String, String> configOptions)
    {
        this(S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                new InfluxDbConnectionFactory(configOptions, null),
                configOptions);
    }

    @VisibleForTesting
    protected InfluxDbRecordHandler(
            final S3Client s3Client,
            final SecretsManagerClient secretsManager,
            final AthenaClient athena,
            final InfluxDbConnectionFactory connectionFactory,
            final Map<String, String> configOptions)
    {
        super(s3Client, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.connectionFactory = connectionFactory;
        if (connectionFactory != null) {
            connectionFactory.setHandler(this);
        }
    }

    @Override
    protected void readWithConstraint(final BlockSpiller spiller, final ReadRecordsRequest recordsRequest,
            final QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        final Schema schema = recordsRequest.getSchema();
        String tableName = recordsRequest.getTableName().getTableName();
        final String schemaName = recordsRequest.getTableName().getSchemaName();

        // Use the original case-sensitive table name stored by the MetadataHandler.
        final String originalTableName = schema.getCustomMetadata().get("originalTableName");
        if (originalTableName != null) {
            tableName = originalTableName;
        }

        final String resolvedDb = connectionFactory.resolveDatabase(schemaName);

        final String sql = InfluxDbQueryBuilder.buildSql(schema, tableName, recordsRequest.getConstraints());
        logger.info("readWithConstraint: schema={}, sql={}", schemaName, sql);

        final List<Field> fields = schema.getFields();
        // Pre-compute which columns are timestamps
        final boolean[] isTimestamp = new boolean[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            final Types.MinorType mt = Types.getMinorTypeForArrowType(fields.get(i).getType());
            isTimestamp[i] = (mt == Types.MinorType.TIMESTAMPMILLITZ || mt == Types.MinorType.DATEMILLI);
        }

        try (InfluxDBClient client = connectionFactory.getClient(resolvedDb)) {
            try (Stream<Object[]> stream = client.query(sql)) {
                stream.forEach(row -> {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    spiller.writeRows((final Block block, final int rowNum) -> {
                        boolean matched = true;
                        for (int i = 0; i < fields.size(); i++) {
                            Object val = row[i];
                            if (val != null && isTimestamp[i]) {
                                // BlockUtils.setValue for TIMESTAMPMILLITZ requires a ZonedDateTime.
                                val = toZonedDateTime(val);
                            }
                            matched &= block.offerValue(fields.get(i).getName(), rowNum, val);
                        }
                        return matched ? 1 : 0;
                    });
                });
            }
        }
    }

    /**
     * Converts a timestamp value from InfluxDB into a ZonedDateTime. BlockUtils.setValue for TIMESTAMPMILLITZ expects ZonedDateTime.
     */
    static ZonedDateTime toZonedDateTime(final Object value)
    {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        }
        if (value instanceof Instant) {
            return ((Instant) value).atZone(UTC);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(UTC);
        }
        if (value instanceof Number) {
            long val = ((Number) value).longValue();
            if (val > MAX_PLAUSIBLE_EPOCH_MILLIS) {
                // Too large to be milliseconds, so it must be nanoseconds.
                val = TimeUnit.NANOSECONDS.toMillis(val);
            }
            return Instant.ofEpochMilli(val).atZone(UTC);
        }
        return Instant.parse(String.valueOf(value)).atZone(UTC);
    }

    static long toEpochMillis(final Object value)
    {
        return toZonedDateTime(value).toInstant().toEpochMilli();
    }
}
