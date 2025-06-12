package com.smzdm.aggregator;

import com.smzdm.entity.CalEntity;
import com.smzdm.utils.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.HashMap;
import java.util.Objects;

/**
 * 展示和点击数聚合器
 * 负责在时间窗口内聚合用户的展示和点击行为数据
 * 
 * @author liuchen
 * @since 1.0
 */
public class CalImpAndClickAggregator implements 
    AggregateFunction<Tuple4<String, String, String, Long>, HashMap<String, CalEntity>, HashMap<String, CalEntity>> {

    @Override
    public HashMap<String, CalEntity> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<String, CalEntity> getResult(HashMap<String, CalEntity> accumulator) {
        return new HashMap<>(accumulator);
    }

    @Override
    public HashMap<String, CalEntity> merge(HashMap<String, CalEntity> acc1, HashMap<String, CalEntity> acc2) {
        HashMap<String, CalEntity> merged = new HashMap<>(acc1);
        acc2.forEach((key, value) ->
            merged.merge(key, value, (v1, v2) -> 
                new CalEntity(v1.getImp() + v2.getImp(), v1.getClick() + v2.getClick())
            )
        );
        return merged;
    }

    @Override
    public HashMap<String, CalEntity> add(Tuple4<String, String, String, Long> tuple, HashMap<String, CalEntity> accumulator) {
        String itemId = tuple.f1;
        String actionType = tuple.f2;
        
        // 初始化实体（如果不存在）
        accumulator.putIfAbsent(itemId, new CalEntity());
        CalEntity calEntity = accumulator.get(itemId);

        // 根据行为类型增加相应计数
        if (Objects.equals(actionType, Utils.IMP)) {
            calEntity.addImp();
        } else if (Objects.equals(actionType, Utils.CLICK)) {
            calEntity.addClick();
        }
        
        return accumulator;
    }
} 