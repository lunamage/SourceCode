package com.smzdm.abtest;

import java.util.*;

/**
 * AB测试SQL模板演示类
 * 专门演示AB测试分析模板的使用
 */
public class SqlTemplateDemo {
    
    private static final String SEPARATOR = "================================================================================";

    public static void main(String[] args) {
        System.out.println("=== AB测试分析模板演示 ===\n");
        
        // 创建SQL模板引擎
        SqlTemplateEngine engine = new SqlTemplateEngine();
        
        // 加载AB测试模板
        loadABTestTemplates(engine);
        
        // 演示: 完整的AB测试分析
        demonstrateFullABTestAnalysis(engine);
    }

    /**
     * 加载AB测试模板
     */
    private static void loadABTestTemplates(SqlTemplateEngine engine) {
        System.out.println("加载AB测试分析模板...");
        
        SqlTemplateLoader loader = new SqlTemplateLoader();
        
        // 验证并加载配置文件
        if (loader.validateTemplateConfig("sql-templates.yaml")) {
            loader.loadTemplatesFromResource(engine, "sql-templates.yaml");
            System.out.println("AB测试模板加载成功!\n");
        } else {
            System.out.println("配置文件验证失败\n");
        }
    }

    /**
     * 演示完整的AB测试分析（包含所有功能）
     */
    private static void demonstrateFullABTestAnalysis(SqlTemplateEngine engine) {
        System.out.println("1. 完整AB测试分析演示（包含基线流对比和显著性检验）:");
        
        try {
            Map<String, Object> params = new HashMap<>();
            
            // 基础参数
            params.put("startDate", "2025-04-01 00:00:00");
            params.put("endDate", "2025-04-14 10:00:00");
            params.put("tableName", "bi_app.ads_zdm_user_abtest_background_v4_d");
            params.put("scence", "new_user");
            
            // AB测试分组配置
            params.put("controlGroups", "68_c1,68_b1,68_c,68_b");
            params.put("experimentGroups", "68_f1,68_d1,68_f,68_d");
            params.put("baselineGroups", "68_z64");
            
            // 版本过滤配置
            params.put("versionFilter", true);
            params.put("minVersion", "9.6.20");
            
            // 过滤条件
            params.put("filterCondition", "sum_app_search_def_sort_haojia_article_expose>0");
            
            // 分析选项（包含所有功能）
            params.put("includeBaselineComparison", true);
            params.put("includeSignificanceTest", true);
            params.put("includeBaselineSignificanceTest", true);
            
            // 指标字段配置（完整版本）
            params.put("metricFields", createFullMetricFields());

            String sql = engine.generateSql("abtest_analysis", params);
            System.out.println(sql);
        } catch (Exception e) {
            System.out.println("完整AB测试分析演示失败: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("\n" + SEPARATOR + "\n");
    }

    /**
     * 创建完整的指标字段配置
     */
    private static List<Map<String, Object>> createFullMetricFields() {
        List<Map<String, Object>> fields = new ArrayList<>();
        
        // 添加所有指标字段
        fields.add(createMetricField("app_search_synth_valid_order", "sum", true));
        fields.add(createMetricField("app_search_haojia_article_valid_cps", "sum", true));
        fields.add(createMetricField("app_search_synth_channel_expose", "sum", true));
        fields.add(createMetricField("app_search_def_sort_haojia_article_expose", "sum", true));
        fields.add(createMetricField("whole_valid_order", "sum", true));
        fields.add(createMetricField("app_main_scence_click", "sum", true));
        fields.add(createMetricField("app_main_scence_addtocart", "sum", true));
        fields.add(createMetricField("app_search_synth_valid_cps", "sum", true));
        fields.add(createMetricField("app_search_synth_channel_addtocart", "sum", true));
        fields.add(createMetricField("app_search_liucun7_rate", "MAX", true));
        fields.add(createMetricField("app_liucun1_rate", "MAX", true));
        fields.add(createMetricField("app_search_def_sort_haojia_article_click", "sum", true));
        fields.add(createMetricField("app_search_synth_channel_click", "sum", true));
        fields.add(createMetricField("whole_valid_cps", "sum", true));
        fields.add(createMetricField("app_search_haojia_article_valid_order", "sum", true));
        fields.add(createMetricField("app_search_liucun1_rate", "MAX", true));
        fields.add(createMetricField("whole_valid_gmv", "sum", true));
        fields.add(createMetricField("app_search_liucun3_rate", "MAX", true));
        fields.add(createMetricField("app_search_synth_valid_gmv", "sum", true));
        fields.add(createMetricField("app_search_def_sort_haojia_article_addtocart", "sum", true));
        fields.add(createMetricField("app_search_haojia_article_valid_gmv", "sum", false)); // 最后一个字段
        
        return fields;
    }

    /**
     * 创建指标字段对象
     */
    private static Map<String, Object> createMetricField(String fieldName, String aggregateFunction, boolean hasNext) {
        Map<String, Object> field = new HashMap<>();
        field.put("fieldName", fieldName);
        field.put("aggregateFunction", aggregateFunction);
        field.put("hasNext", hasNext);
        return field;
    }
} 