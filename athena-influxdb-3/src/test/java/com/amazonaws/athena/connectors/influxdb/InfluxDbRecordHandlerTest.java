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

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Test;

public class InfluxDbRecordHandlerTest {
    @Test
    public void testToZonedDateTimeWithZonedDateTime() {
        final ZonedDateTime zonedDateTime = ZonedDateTime.now();
        assertEquals(InfluxDbRecordHandler.toZonedDateTime(zonedDateTime), zonedDateTime);
    }

    @Test
    public void testToZonedDateTimeWithInstant() {
        final Instant instant = Instant.now();
        assertEquals(InfluxDbRecordHandler.toZonedDateTime(instant), instant.atZone(ZoneId.of("UTC")));
    }

    @Test
    public void testToZonedDateTimeWithLocalDateTime() {
        final LocalDateTime localDateTime = LocalDateTime.now();
        assertEquals(InfluxDbRecordHandler.toZonedDateTime(localDateTime), localDateTime.atZone(ZoneId.of("UTC")));
    }

    @Test
    public void testToZonedDateTimeWithNanoseconds() {
        final Long nanoseconds = 1782258710000000000L;
        assertEquals(Instant.ofEpochMilli(nanoseconds / 1_000_000L).atZone(ZoneId.of("UTC")),
                InfluxDbRecordHandler.toZonedDateTime(nanoseconds));
    }

    @Test
    public void testToZonedDateTimeWithMilliSeconds() {
        final Long milliseconds = 1782258710000L;
        assertEquals(Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of("UTC")),
                InfluxDbRecordHandler.toZonedDateTime(milliseconds));
    }
}
