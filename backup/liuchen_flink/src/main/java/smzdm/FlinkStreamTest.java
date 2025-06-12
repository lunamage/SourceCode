package smzdm;

import smzdm.aggregator.CalImpAndClickAggregator;
import smzdm.function.WindowResultFunction;
import smzdm.service.MessageSplitter;
import smzdm.service.RedisSink;
import smzdm.config.FlinkConfiguration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Flink实时特征计算主应用
 */
public class FlinkStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamTest.class);
    
    private static final String IMP_LOG_STATUS = "\"type\":\"show\"";
    private static final String CLICK_LOG_STATUS = "\"type\":\"event\"";

    public static void main(String[] args) {
        try {
            String configPath = args.length > 0 ? args[0] : "config/config.properties";
            logger.info("Starting Flink Stream with config: {}", configPath);
            
            FlinkConfiguration config = new FlinkConfiguration(configPath);
            
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            configureEnvironment(env, config);
            buildPipeline(env, config);
            
            env.execute(config.getTaskGroup());
            
        } catch (Exception e) {
            logger.error("Application failed", e);
            System.exit(1);
        }
    }

    private static void configureEnvironment(StreamExecutionEnvironment env, FlinkConfiguration config) {
        env.setParallelism(config.getIntProperty("flink.parallelism", 4));
        
        long checkpointInterval = config.getLongProperty("flink.checkpoint.interval", 0);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }
        
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
    }

    private static void buildPipeline(StreamExecutionEnvironment env, FlinkConfiguration config) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setGroupId(config.getKafkaGroupId())
            .setTopics(config.getKafkaTopic())
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withIdleness(Duration.ofMinutes(1));

        env.fromSource(kafkaSource, watermarkStrategy, "kafka-source")
            .filter(log -> log != null && (log.contains(IMP_LOG_STATUS) || log.contains(CLICK_LOG_STATUS)))
            .flatMap(new MessageSplitter(config.getProperties()))
            .keyBy(tuple -> Tuple2.of(tuple.f0, tuple.f1))
            .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.minutes(config.getWindowSizeMinutes()))
            .aggregate(new CalImpAndClickAggregator(), new WindowResultFunction())
            .addSink(new RedisSink(config.getProperties()));
    }
}

