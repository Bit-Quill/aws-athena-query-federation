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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InfluxDbRecordHandlerTest
{
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account",
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());

    private BufferAllocator arrowAllocator;
    private BlockAllocator blockAllocator;
    private InfluxDbConnectionFactory mockFactory;
    private InfluxDBClient mockClient;
    private InfluxDbRecordHandler handler;

    @Before
    public void setUp() throws Exception
    {
        arrowAllocator = new RootAllocator();
        blockAllocator = new BlockAllocatorImpl();
        mockFactory = mock(InfluxDbConnectionFactory.class);
        mockClient = mock(InfluxDBClient.class);
        // Run the query lambda directly against the mock client (mirrors executeWithTokenRetry).
        when(mockFactory.executeWithTokenRetry(any(), any())).thenAnswer(invocation -> {
            final InfluxDbConnectionFactory.InfluxDbQuery<?> query = invocation.getArgument(1);
            return query.run(mockClient);
        });

        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");

        handler = new InfluxDbRecordHandler(
                mock(software.amazon.awssdk.services.s3.S3Client.class),
                mock(software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.class),
                mock(software.amazon.awssdk.services.athena.AthenaClient.class),
                mockFactory,
                config);
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
        arrowAllocator.close();
    }

    /** Projection schema: host (VARCHAR), usage_idle (FLOAT8), time (TIMESTAMPMILLITZ). */
    private Schema projectionSchema()
    {
        return new SchemaBuilder()
                .addMetadata("originalTableName", "cpu")
                .addMetadata("resolvedDatabaseName", "testdb")
                .addField("host", Types.MinorType.VARCHAR.getType())
                .addField("usage_idle", Types.MinorType.FLOAT8.getType())
                .addField("time", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))
                .build();
    }

    /** Builds a 2-row Arrow batch matching the projection schema (same field order). */
    private VectorSchemaRoot buildBatch()
    {
        final VarCharVector host = new VarCharVector("host", arrowAllocator);
        final Float8Vector usageIdle = new Float8Vector("usage_idle", arrowAllocator);
        final Field timeField = new Field("time",
                new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null), null);
        final TimeStampMilliTZVector time = new TimeStampMilliTZVector(timeField, arrowAllocator);

        host.allocateNew(2);
        usageIdle.allocateNew(2);
        time.allocateNew(2);

        host.set(0, new Text("server01"));
        host.set(1, new Text("server02"));
        usageIdle.set(0, 95.0);
        usageIdle.set(1, 42.5);
        time.set(0, 1764764130000L);
        time.set(1, 1764764131000L);

        host.setValueCount(2);
        usageIdle.setValueCount(2);
        time.setValueCount(2);

        return new VectorSchemaRoot(Arrays.asList(host, usageIdle, time));
    }

    private ReadRecordsRequest readRequest(final Schema schema)
    {
        final Split split = mock(Split.class);
        when(split.getProperty(anyString())).thenReturn(null);
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);
        return new ReadRecordsRequest(IDENTITY, "catalog", "queryId",
                new TableName("testdb", "cpu"), schema, split, constraints, 100_000, 100_000);
    }

    /**
     * A mock spiller whose writeRows invokes the connector's RowWriter against a real Block, so the
     * value-extraction/timestamp-conversion code in readWithConstraint is actually exercised.
     */
    private BlockSpiller spillerWritingTo(final Block block, final AtomicInteger rowsWritten)
    {
        final BlockSpiller spiller = mock(BlockSpiller.class);
        doAnswer(invocation -> {
            final BlockWriter.RowWriter writer = invocation.getArgument(0);
            final int written = writer.writeRows(block, rowsWritten.get());
            rowsWritten.addAndGet(written);
            return written;
        }).when(spiller).writeRows(any());
        return spiller;
    }

    @Test
    public void testReadWithConstraintWritesRows() throws Exception
    {
        final Schema schema = projectionSchema();
        final VectorSchemaRoot root = buildBatch();
        when(mockClient.queryBatches(anyString())).thenReturn(Stream.of(root).onClose(root::close));

        final QueryStatusChecker checker = mock(QueryStatusChecker.class);
        when(checker.isQueryRunning()).thenReturn(true);

        final Block block = blockAllocator.createBlock(schema);
        block.constrain(ConstraintEvaluator.emptyEvaluator());
        final AtomicInteger rowsWritten = new AtomicInteger();
        final BlockSpiller spiller = spillerWritingTo(block, rowsWritten);

        handler.readWithConstraint(spiller, readRequest(schema), checker);

        // Both rows in the batch were converted and written.
        assertEquals(2, rowsWritten.get());
        // Values (including the timestamp conversion path) landed in the block.
        assertEquals("server01", block.getFieldReader("host").readText().toString());
    }

    @Test
    public void testReadWithConstraintStopsWhenQueryNotRunning() throws Exception
    {
        final Schema schema = projectionSchema();
        final VectorSchemaRoot root = buildBatch();
        when(mockClient.queryBatches(anyString())).thenReturn(Stream.of(root).onClose(root::close));

        final QueryStatusChecker checker = mock(QueryStatusChecker.class);
        when(checker.isQueryRunning()).thenReturn(false);

        final Block block = blockAllocator.createBlock(schema);
        block.constrain(ConstraintEvaluator.emptyEvaluator());
        final AtomicInteger rowsWritten = new AtomicInteger();
        final BlockSpiller spiller = spillerWritingTo(block, rowsWritten);

        handler.readWithConstraint(spiller, readRequest(schema), checker);

        // Cancelled query: the batch is skipped and nothing is written.
        assertEquals(0, rowsWritten.get());
        verify(spiller, never()).writeRows(any());
    }

    @Test
    public void testToZonedDateTimeNanoseconds()
    {
        final long nanos = 1782258710000000000L;
        assertEquals(Instant.ofEpochSecond(0L, nanos).atZone(UTC),
                InfluxDbRecordHandler.toZonedDateTime(nanos, TimeUnit.NANOSECOND));
    }

    @Test
    public void testToZonedDateTimeMilliseconds()
    {
        final long millis = 1782258710000L;
        assertEquals(Instant.ofEpochMilli(millis).atZone(UTC),
                InfluxDbRecordHandler.toZonedDateTime(millis, TimeUnit.MILLISECOND));
    }

    /**
     * The same instant expressed in each supported unit must decode identically —
     * this is exactly the ambiguity the magnitude heuristic could not guarantee.
     */
    @Test
    public void testAllUnitsResolveToSameInstant()
    {
        final long seconds = 1782258710L;
        final ZonedDateTime expected = Instant.ofEpochSecond(seconds).atZone(UTC);
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds, TimeUnit.SECOND));
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds * 1_000L, TimeUnit.MILLISECOND));
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds * 1_000_000L, TimeUnit.MICROSECOND));
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds * 1_000_000_000L, TimeUnit.NANOSECOND));
    }
}
