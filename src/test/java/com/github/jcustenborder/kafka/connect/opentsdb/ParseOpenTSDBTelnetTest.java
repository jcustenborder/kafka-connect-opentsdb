/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.opentsdb;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class ParseOpenTSDBTelnetTest {

  ParseOpenTSDB.Value<SinkRecord> transformation;

  @BeforeEach
  public void before() {
    this.transformation = new ParseOpenTSDB.Value<>();
    this.transformation.configure(
        ImmutableMap.of(ParseOpenTSDBConfig.PROTOCOL_TYPE_CONF,
			ParseOpenTSDBConfig.ProtocolType.TelnetProtocol.toString())
    );
  }

  @TestFactory
  public Stream<DynamicTest> apply() {
    Map<String, Struct> testcases = new LinkedHashMap<>();
    testcases.put(
        "(ignore me)put  mysql.bytes_received  1287333217 327810227706 schema=foo   host=db1",
        new Struct(OpenTSDBParser.SCHEMA)
            .put("metricName", "mysql.bytes_received")
            .put("timestamp", new Date(1287333217L * 1000L))
            .put("value", 327810227706D)
            .put("tags", ImmutableMap.of("schema", "foo", "host", "db1"))
    );
    testcases.put("put mysql.bytes_sent 1287333217 6604859181710 schema=foo host=db1",
        new Struct(OpenTSDBParser.SCHEMA)
            .put("metricName", "mysql.bytes_sent")
            .put("timestamp", new Date(1287333217L * 1000L))
            .put("value", 6604859181710D)
            .put("tags", ImmutableMap.of("schema", "foo", "host", "db1"))
    );
    testcases.put("put mysql.bytes_received 1287333232 -327812421706.2718 schema=foo host=db1",
        new Struct(OpenTSDBParser.SCHEMA)
            .put("metricName", "mysql.bytes_received")
            .put("timestamp", new Date(1287333232L * 1000L))
            .put("value", -327812421706.2718D)
            .put("tags", ImmutableMap.of("schema", "foo", "host", "db1"))
    );
    testcases.put("!@#$%^&*()-_+=1234567890[]{}\\|put mysql.bytes_sent 1287333232 6604901075387 schema=foo host=db1",
        new Struct(OpenTSDBParser.SCHEMA)
            .put("metricName", "mysql.bytes_sent")
            .put("timestamp", new Date(1287333232L * 1000L))
            .put("value", 6604901075387D)
            .put("tags", ImmutableMap.of("schema", "foo", "host", "db1"))
    );
    testcases.put("-->puttputtputput mysql.bytes_put 1287333321 340899533915 schema=put host=db2",
        new Struct(OpenTSDBParser.SCHEMA)
            .put("metricName", "mysql.bytes_put")
            .put("timestamp", new Date(1287333321L * 1000L))
            .put("value", 340899533915D)
            .put("tags", ImmutableMap.of("schema", "put", "host", "db2"))
    );
    testcases.put("put mysql.bytes_sent 1287333321 5506469130707 schema=foo host=db2",
        new Struct(OpenTSDBParser.SCHEMA)
            .put("metricName", "mysql.bytes_sent")
            .put("timestamp", new Date(1287333321L * 1000L))
            .put("value", 5506469130707D)
            .put("tags", ImmutableMap.of("schema", "foo", "host", "db2"))
    );

    return testcases.entrySet().stream()
        .map(e -> dynamicTest(e.getKey(), () -> {
          final SinkRecord input = new SinkRecord(
              "test",
              1,
              null,
              null,
              Schema.STRING_SCHEMA,
              e.getKey(),
              123412L
          );
          final SinkRecord output = this.transformation.apply(input);
          assertNotNull(output);
          assertTrue(output.value() instanceof Struct, "output.value() should be a struct.");
          assertStruct(e.getValue(), (Struct) output.value());
        }));
  }

}
