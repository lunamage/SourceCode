package com.smzdm;

import com.smzdm.service.MessageSplitter;
import com.smzdm.service.RedisSink;
import com.smzdm.utils.DateUtils;
import com.smzdm.utils.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileInputStream;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @author: liuchen
 * @date: 2019/1/11
 * @time: 3:14 PM
 * @Description: 实时特征计算
 */
public class FlinkStreamTest {

    private final static String IMP_LOG_STATUS = "\"type\":\"show\"";
    private final static String CLICK_LOG_STATUS = "\"type\":\"event\"";

    // 读取配置文件
    private static FileInputStream getFileInputStream(String path) {
        FileInputStream fis = null;
        try {
            File pfile = new File(path);
            fis = new FileInputStream(pfile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fis;
    }

    private final static String CONFIG_PATH = "config/config.properties";

    public static void main(String[] args) throws Exception {
        // 获取配置文件路径
        String configPath = Strings.isNullOrEmpty(args[0]) ? CONFIG_PATH : args[0];

        Properties propFile = new Properties();
        propFile.load(getFileInputStream(configPath));

        // 计算窗口
        Integer windowSize = Integer.valueOf(propFile.getProperty("flink.timeWindow.minutes.size"));
        // 计算步长
        Integer windowSlide = Integer.valueOf(propFile.getProperty("flink.timeWindow.minutes.slide"));
        // 任务名称
        String taskGroup = propFile.getProperty("flink.task.group");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", propFile.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", propFile.getProperty("group.id"));

        FlinkKafkaConsumer<String> myConsumer =
                new FlinkKafkaConsumer<>(propFile.getProperty("kafka.topic"), new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();
        env
                .addSource(myConsumer)
                .filter((FilterFunction<String>) log -> {
                    // 过滤出曝光以及点击事件
                    return log.contains(IMP_LOG_STATUS) || log.contains(CLICK_LOG_STATUS);
                })
                .flatMap(new MessageSplitter(propFile))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, String, String, Long>>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> tuple4) {
                        return tuple4.f3;
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(windowSize), Time.minutes(windowSlide))
                .aggregate(new calImpAndClickAgg(), new WindowResultFunction())
                .addSink(new RedisSink(propFile));
        env.execute(taskGroup);
    }

    public static class CalEntity {
        public int imp = 0;
        public int click = 0;
    }

    public static class ItemFeatureEntity {
        private String userProxyId;
        private Map<String, Object[]> val;
        private String windowEnd;

        public String getUserProxyId() {
            return userProxyId;
        }

        public Map<String, Object[]> getVal() {
            return val;
        }

        public String getWindowEnd() {
            return windowEnd;
        }

        public static ItemFeatureEntity getEntity(String userProxyId, Map<String, Object[]> val, String windowEnd) {
            ItemFeatureEntity entity = new ItemFeatureEntity();
            entity.userProxyId = userProxyId;
            entity.val = val;
            entity.windowEnd = windowEnd;
            return entity;
        }
    }

    public static class calImpAndClickAgg implements AggregateFunction<Tuple4<String, String, String, Long>, HashMap<String, CalEntity>, HashMap<String, CalEntity>> {

        @Override
        public HashMap<String, CalEntity> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, CalEntity> getResult(HashMap<String, CalEntity> o) {
            return o;
        }

        @Override
        public HashMap<String, CalEntity> merge(HashMap<String, CalEntity> entity1, HashMap<String, CalEntity> entity2) {
            return null;
        }

        @Override
        public HashMap<String, CalEntity> add(Tuple4<String, String, String, Long> val, HashMap<String, CalEntity> entity) {
            if (!entity.containsKey(val.f1)) {
                entity.put(val.f1, new CalEntity());
            }
            if (Objects.equals(val.f2, Utils.IMP)) {
                entity.get(val.f1).imp += 1;
            }
            if (Objects.equals(val.f2, Utils.CLICK)) {
                entity.get(val.f1).click += 1;
            }
            return entity;
        }
    }

    public static class WindowResultFunction implements WindowFunction<HashMap<String, CalEntity>, ItemFeatureEntity, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<HashMap<String, CalEntity>> entitys, Collector<ItemFeatureEntity> collector) {
            String userProxyId = tuple.getField(0);
            Map<String, CalEntity> entity = entitys.iterator().next();
            Map<String, Object[]> val = new HashMap<>();

            for (Map.Entry<String, CalEntity> map : entity.entrySet()) {
                Integer imp = map.getValue().imp;
                Integer click = map.getValue().click;
                DecimalFormat df = new DecimalFormat("0.00000");
                Double ctr = imp == 0 ? null : Double.valueOf(df.format((float) click / imp));
                val.put(map.getKey(), new Object[]{imp, click, ctr});
            }
            collector.collect(ItemFeatureEntity.getEntity(userProxyId, val, DateUtils.formatDate(new Date(timeWindow.getEnd()), DateUtils.YYYYMMDD_HMS)));
        }
    }
}

