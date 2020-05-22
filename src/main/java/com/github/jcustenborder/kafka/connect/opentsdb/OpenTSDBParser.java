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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OpenTSDBParser {
  private static final Logger log = LoggerFactory.getLogger(OpenTSDBParser.class);

  public static final Schema SCHEMA = SchemaBuilder.struct()
      .name("net.opentsdb.model.DataPoint")
      .field("metricName", SchemaBuilder.string().doc("").build())
      .field("timestamp", Timestamp.builder().doc("").build())
      .field("value", SchemaBuilder.float64().optional().doc("").build())
      .field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).doc("").build())
      .build();

  static final Pattern METRIC_PATTERN = Pattern.compile("^(\\S+)\\s+(\\d+)\\s+([0-9.]+)");
  static final Pattern KEY_VALUE_PATTERN = Pattern.compile("\\s*(\\S+)=(\\S+)");

  public SchemaAndValue parse(String text) {
    log.trace("parse() - text = '{}'", text);
    if (null == text) {
      return null;
    }
    Matcher matcher = METRIC_PATTERN.matcher(text);
    boolean found = matcher.find();
    if (!found) {
      throw new IllegalStateException("");
    } else {
      log.trace("parse() - matches = {} start = {} end = {}", found, matcher.start(), matcher.end());
    }
    int end = matcher.end();
    String metricName = matcher.group(1);

    Date timestamp;
    try {
      log.trace("parse() - parsing group 2 to long. text = '{}'", matcher.group(2));
      long timestampLong = Long.parseLong(matcher.group(2));
      if (timestampLong < 946684800000L) {
        timestampLong = timestampLong * 1000L;
      }

      //TODO: Check if seconds and adjust.
      timestamp = new Date(timestampLong);
    } catch (Exception ex) {
      IllegalStateException exception = new IllegalStateException(
          String.format(
              "Could not parse '%s'. '%s'",
              matcher.group(2),
              text
          )
      );
      exception.initCause(ex);
      throw exception;
    }
    double value;
    try {
      log.trace("parse() - parsing group 3 to double. text = '{}'", matcher.group(3));
      value = Double.parseDouble(matcher.group(3));
    } catch (Exception ex) {
      IllegalStateException exception = new IllegalStateException(
          String.format(
              "Could not parse '%s'. '%s'",
              matcher.group(3),
              text
          )
      );
      exception.initCause(ex);
      throw exception;
    }

    log.trace("parse() - metricName = '{}' timestamp = '{}' value = '{}'", metricName, timestamp, value);
    matcher = KEY_VALUE_PATTERN.matcher(text);
    Map<String, String> tags = new LinkedHashMap<>(10);
    while (end < text.length()) {
      if (!matcher.find(end)) {
        break;
      }
      String tagKey = matcher.group(1);
      String tagValue = matcher.group(2);
      tags.put(tagKey, tagValue);
      log.trace("put() - tagKey = '{}' tagValue = '{}'", tagKey, tagValue);
      end = matcher.end();
    }

    Struct struct = new Struct(SCHEMA)
        .put("metricName", metricName)
        .put("value", value)
        .put("timestamp", timestamp)
        .put("tags", tags);

    return new SchemaAndValue(struct.schema(), struct);
  }
}
