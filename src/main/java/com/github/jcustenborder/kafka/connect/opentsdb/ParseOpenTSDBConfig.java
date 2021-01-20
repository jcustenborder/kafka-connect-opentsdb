/**
 * Copyright © 2020 David Chaiken (chaiken@acm.org)
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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

// public ???
class ParseOpenTSDBConfig extends AbstractConfig {

  public static final String PROTOCOL_TYPE_CONF = "protocol.type";
  public static final String PROTOCOL_TYPE_DOC = ConfigUtils.enumDescription(ProtocolType.class);

  public enum ProtocolType {
    @Description("The WireProtocol (default) option is the most standard OpenTSDB format.")
    WireProtocol,
    // When this Description is missing, instantiating the ParseOpenTSDBConfig class fails
    // with a NoClassDefFoundError. That seems like a pretty extreme consequence of a missing
    // comment.
    @Description("The TelnetProtocol is the format associated with the put command" +
                 " of the OpenTSDB Telnet Style API.")
    TelnetProtocol
  }

  public final ProtocolType protocolType;

  public ParseOpenTSDBConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.protocolType = ConfigUtils.getEnum(ProtocolType.class, this, PROTOCOL_TYPE_CONF);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(PROTOCOL_TYPE_CONF, ConfigDef.Type.STRING)
                .importance(ConfigDef.Importance.HIGH)
                .documentation(PROTOCOL_TYPE_DOC)
                .defaultValue(ProtocolType.WireProtocol.toString())
                .validator(Validators.validEnum(ProtocolType.class))
                .recommender(Recommenders.enumValues(ProtocolType.class))
                .build()
        );
  }
}
