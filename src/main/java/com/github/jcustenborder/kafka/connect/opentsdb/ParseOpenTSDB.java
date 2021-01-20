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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;


@Title("Parse OpenTSDB transformation")
@Description("The ParseOpenTSDB transformation will parse data that is formatted with the OpenTSDB " +
    "wire protocol.")
@DocumentationTip("This transformation expects data to be a String. You are " +
    "most likely going to use the StringConverter.")
public class ParseOpenTSDB<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  OpenTSDBParser parser;

  protected ParseOpenTSDB(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return ParseOpenTSDBConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.parser = new OpenTSDBParser(new ParseOpenTSDBConfig(settings));
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    return parser.parse(input);
  }

  public static class Key<R extends ConnectRecord<R>> extends ParseOpenTSDB<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends ParseOpenTSDB<R> {
    public Value() {
      super(false);
    }
  }
}
