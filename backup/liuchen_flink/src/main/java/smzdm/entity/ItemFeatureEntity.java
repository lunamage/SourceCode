package com.smzdm.entity;

import java.util.Map;

/**
 * 物品特征实体类，包含用户代理ID、特征值映射和窗口结束时间
 * 
 * @author liuchen
 * @since 1.0
 */
public class ItemFeatureEntity {
    private String userProxyId;  // 用户代理ID
    private Map<String, FeatureValue> featureMap;  // 特征值映射
    private String windowEnd;    // 窗口结束时间

    private ItemFeatureEntity() {}

    public String getUserProxyId() {
        return userProxyId;
    }

    public Map<String, FeatureValue> getVal() {
        return featureMap;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    /**
     * 创建ItemFeatureEntity实例
     * @param userProxyId 用户代理ID
     * @param featureMap 特征值映射
     * @param windowEnd 窗口结束时间
     * @return ItemFeatureEntity实例
     */
    public static ItemFeatureEntity create(String userProxyId, Map<String, FeatureValue> featureMap, String windowEnd) {
        ItemFeatureEntity entity = new ItemFeatureEntity();
        entity.userProxyId = userProxyId;
        entity.featureMap = featureMap;
        entity.windowEnd = windowEnd;
        return entity;
    }
    
    @Override
    public String toString() {
        return String.format("ItemFeatureEntity{userProxyId='%s', featureCount=%d, windowEnd='%s'}", 
                userProxyId, featureMap != null ? featureMap.size() : 0, windowEnd);
    }
} 