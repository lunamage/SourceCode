package com.smzdm.abtest;

import java.util.*;

/**
 * SQL生成器工具类
 * 提供便捷的方法来构建SQL参数和条件
 */
public class SqlGenerator {

    /**
     * 构建查询条件
     */
    public static class ConditionBuilder {
        private List<Map<String, Object>> conditions = new ArrayList<>();

        public ConditionBuilder where(String field, String operator, Object value) {
            Map<String, Object> condition = new HashMap<>();
            condition.put("field", field);
            condition.put("operator", operator);
            condition.put("value", value);
            condition.put("isString", value instanceof String);
            conditions.add(condition);
            return this;
        }

        public ConditionBuilder eq(String field, Object value) {
            return where(field, "=", value);
        }

        public ConditionBuilder ne(String field, Object value) {
            return where(field, "!=", value);
        }

        public ConditionBuilder gt(String field, Object value) {
            return where(field, ">", value);
        }

        public ConditionBuilder ge(String field, Object value) {
            return where(field, ">=", value);
        }

        public ConditionBuilder lt(String field, Object value) {
            return where(field, "<", value);
        }

        public ConditionBuilder le(String field, Object value) {
            return where(field, "<=", value);
        }

        public ConditionBuilder like(String field, String value) {
            return where(field, "LIKE", value);
        }

        public ConditionBuilder in(String field, List<Object> values) {
            StringBuilder inClause = new StringBuilder("(");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) inClause.append(", ");
                Object value = values.get(i);
                if (value instanceof String) {
                    inClause.append("'").append(value).append("'");
                } else {
                    inClause.append(value);
                }
            }
            inClause.append(")");
            return where(field, "IN", inClause.toString());
        }

        public List<Map<String, Object>> build() {
            // 添加hasNext标记
            for (int i = 0; i < conditions.size(); i++) {
                conditions.get(i).put("hasNext", i < conditions.size() - 1);
            }
            return conditions;
        }
    }

    /**
     * 构建更新字段
     */
    public static class UpdateBuilder {
        private List<Map<String, Object>> updates = new ArrayList<>();

        public UpdateBuilder set(String column, Object value) {
            Map<String, Object> update = new HashMap<>();
            update.put("column", column);
            update.put("value", value);
            update.put("isString", value instanceof String);
            updates.add(update);
            return this;
        }

        public List<Map<String, Object>> build() {
            // 添加hasNext标记
            for (int i = 0; i < updates.size(); i++) {
                updates.get(i).put("hasNext", i < updates.size() - 1);
            }
            return updates;
        }
    }

    /**
     * 构建聚合函数
     */
    public static class AggregationBuilder {
        private List<Map<String, Object>> aggregations = new ArrayList<>();

        public AggregationBuilder count(String column, String alias) {
            return addAggregation("COUNT", column, alias);
        }

        public AggregationBuilder sum(String column, String alias) {
            return addAggregation("SUM", column, alias);
        }

        public AggregationBuilder avg(String column, String alias) {
            return addAggregation("AVG", column, alias);
        }

        public AggregationBuilder max(String column, String alias) {
            return addAggregation("MAX", column, alias);
        }

        public AggregationBuilder min(String column, String alias) {
            return addAggregation("MIN", column, alias);
        }

        private AggregationBuilder addAggregation(String function, String column, String alias) {
            Map<String, Object> aggregation = new HashMap<>();
            aggregation.put("function", function);
            aggregation.put("column", column);
            aggregation.put("alias", alias);
            aggregations.add(aggregation);
            return this;
        }

        public List<Map<String, Object>> build() {
            // 添加hasNext标记
            for (int i = 0; i < aggregations.size(); i++) {
                aggregations.get(i).put("hasNext", i < aggregations.size() - 1);
            }
            return aggregations;
        }
    }

    /**
     * 添加hasNext标记到列表
     */
    public static List<Map<String, Object>> addHasNextToList(List<String> items) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("value", items.get(i));
            item.put("hasNext", i < items.size() - 1);
            result.add(item);
        }
        return result;
    }

    /**
     * 简单地为字符串列表添加hasNext标记
     */
    public static List<String> addHasNextToColumns(List<String> columns) {
        // 对于简单的列名列表，我们直接在模板中处理hasNext逻辑
        return columns;
    }

    /**
     * 构建插入数据行
     */
    public static List<Map<String, Object>> buildInsertRows(List<List<Object>> rows) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = new HashMap<>();
            List<Object> values = rows.get(i);
            
            // 为每个值添加类型信息
            List<Map<String, Object>> valuesList = new ArrayList<>();
            for (int j = 0; j < values.size(); j++) {
                Map<String, Object> valueMap = new HashMap<>();
                valueMap.put("value", values.get(j));
                valueMap.put("isString", values.get(j) instanceof String);
                valueMap.put("hasNext", j < values.size() - 1);
                valuesList.add(valueMap);
            }
            
            row.put("values", valuesList);
            row.put("hasNext", i < rows.size() - 1);
            result.add(row);
        }
        return result;
    }

    /**
     * 创建条件构建器
     */
    public static ConditionBuilder conditions() {
        return new ConditionBuilder();
    }

    /**
     * 创建更新构建器
     */
    public static UpdateBuilder updates() {
        return new UpdateBuilder();
    }

    /**
     * 创建聚合构建器
     */
    public static AggregationBuilder aggregations() {
        return new AggregationBuilder();
    }
} 