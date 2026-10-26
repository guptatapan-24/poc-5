package com.stockstream.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockstream.flink.function.ThresholdCrossingProcessFunction;
import com.stockstream.flink.model.PriceDowntrendEvent;
import com.stockstream.flink.model.PriceUpdate;
import com.stockstream.flink.model.ThresholdCrossingEvent;
import com.stockstream.flink.pattern.DowntrendCepPattern;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.Instant;

/**
 * Main Apache Flink Streaming Pipeline Job.
 * Connects to Kafka KRaft single broker, processes keyed stock prices,
 * evaluates threshold crossing and Flink CEP downtrend pattern,
 * and sinks derived alerts to Kafka 'stock-events' topic.
 */
public class StockStreamFlinkJob {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC_PRICES = "stock-prices";
    private static final String TOPIC_EVENTS = "stock-events";
    private static final String GROUP_ID = "flink-stock-processor";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing interval 10 seconds with local filesystem checkpoint storage
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        ObjectMapper objectMapper = new ObjectMapper();

        // 1. Kafka Source for 'stock-prices'
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC_PRICES)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Event-Time & Watermark Strategy with 2s Bounded Out-Of-Orderness & withIdleness
        WatermarkStrategy<PriceUpdate> watermarkStrategy = WatermarkStrategy
                .<PriceUpdate>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withIdleness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<PriceUpdate>) (element, recordTimestamp) -> {
                    try {
                        return Instant.parse(element.getObservedAt()).toEpochMilli();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });

        DataStream<PriceUpdate> priceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaPriceSource")
                .map(json -> objectMapper.readValue(json, PriceUpdate.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 3. Keyed by symbol
        KeyedStream<PriceUpdate, String> keyedStream = priceStream.keyBy(PriceUpdate::getSymbol);

        // 4. Branch A: Stateful Threshold Crossing Process Function
        DataStream<String> thresholdEvents = keyedStream
                .process(new ThresholdCrossingProcessFunction(180.0, "LESS_THAN_OR_EQUAL"))
                .name("ThresholdCrossingProcessFunction")
                .map(objectMapper::writeValueAsString);

        // 5. Branch B: Flink CEP 3-Consecutive Downtrend Pattern
        PatternStream<PriceUpdate> patternStream = CEP.pattern(keyedStream, DowntrendCepPattern.createPattern());

        DataStream<String> cepEvents = patternStream
                .select(DowntrendCepPattern.createSelectFunction())
                .name("DowntrendCepPattern")
                .map(objectMapper::writeValueAsString);

        // 6. Union Streams & Sink to Kafka 'stock-events'
        DataStream<String> allEvents = thresholdEvents.union(cepEvents);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(TOPIC_EVENTS)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        allEvents.sinkTo(sink).name("KafkaStockEventsSink");

        env.execute("Flink-Stock-Stream-Evaluator");
    }
}
