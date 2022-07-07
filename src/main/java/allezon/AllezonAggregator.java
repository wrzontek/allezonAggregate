/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package allezon;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;


public class AllezonAggregator {

    static final String USER_TAGS_TOPIC = "user_tags";
    static final Duration windowSize = Duration.ofMinutes(1);
    static final String VIEW_TOPIC = "user_tags_views";
    static final String BUY_TOPIC = "user_tags_buys";

    static final String bootstrapServers = "10.112.127.110:9092";

    static final Serde<KafkaUserTag> kafkaUserTagSerde = new KafkaUserTagSerde();

    public static void main(final String[] args) {
        final KafkaStreams streams = buildStream();
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static KafkaStreams buildStream() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "allezon-aggregate");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "allezon-aggregate-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaUserTagSerde.class);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);


        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, KafkaUserTag> userTags = builder.stream(USER_TAGS_TOPIC); // todo split for VIEW/BUY
        final KStream<KafkaUserTag, KafkaUserTag> userTagsMapped = userTags
                .map((k, v) -> new KeyValue<>(v, v));


        final TimeWindowedKStream<KafkaUserTag, KafkaUserTag> userTagsTable = userTagsMapped
                .groupByKey(Grouped.with(kafkaUserTagSerde, kafkaUserTagSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize));

        userTagsTable.count(Materialized.with(kafkaUserTagSerde, Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
