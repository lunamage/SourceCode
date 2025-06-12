package com.smzdm.function;

import com.smzdm.entity.CalEntity;
import com.smzdm.entity.FeatureValue;
import com.smzdm.entity.ItemFeatureEntity;
import com.smzdm.utils.DateUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 窗口结果处理函数
 * 将聚合的展示和点击数据转换为特征值并输出
 * 
 * @author liuchen
 * @since 1.0
 */
public class WindowResultFunction implements 
    WindowFunction<HashMap<String, CalEntity>, ItemFeatureEntity, Tuple, TimeWindow> {
    
    @Override
    public void apply(Tuple key, TimeWindow window, 
                     Iterable<HashMap<String, CalEntity>> input, 
                     Collector<ItemFeatureEntity> out) {
        
        String userProxyId = key.getField(0);
        HashMap<String, CalEntity> aggregatedData = input.iterator().next();
        Map<String, FeatureValue> featureMap = new HashMap<>();

        // 将聚合数据转换为特征值
        for (Map.Entry<String, CalEntity> entry : aggregatedData.entrySet()) {
            String itemId = entry.getKey();
            CalEntity calEntity = entry.getValue();
            
            FeatureValue featureValue = FeatureValue.create(
                calEntity.getImp(), 
                calEntity.getClick()
            );
            featureMap.put(itemId, featureValue);
        }
        
        // 格式化窗口结束时间
        String windowEnd = DateUtils.formatDate(
            new Date(window.getEnd()), 
            DateUtils.YYYYMMDD_HMS
        );
        
        // 输出结果
        out.collect(ItemFeatureEntity.create(userProxyId, featureMap, windowEnd));
    }
} 