# AB测试SQL模板化系统

这是一个专为AB测试数据分析设计的SQL模板生成系统，基于Mustache模板引擎，能够根据配置参数动态生成复杂的AB测试分析SQL语句。

## 🚀 核心特性

- **专业AB测试分析**：专门针对AB测试场景设计的SQL模板
- **动态指标配置**：支持自定义指标字段和聚合函数
- **多维度对比**：支持对照组、实验组、基线流三组对比分析
- **统计显著性检验**：内置t检验计算，自动判断实验结果显著性
- **版本过滤支持**：智能版本号比较和过滤
- **灵活模板引擎**：基于Mustache的强大模板渲染能力
- **配置驱动**：支持YAML配置文件和Java代码两种定义方式

## 📋 系统架构

```
AB测试SQL模板化系统
├── SqlTemplateEngine     # 核心模板引擎
├── SqlTemplate          # 模板定义类
├── SqlGenerator         # SQL构建工具
├── SqlTemplateLoader    # YAML配置加载器
└── SqlTemplateDemo      # 使用演示
```

## 🏃 快速开始

### 1. 运行演示程序

```bash
# 编译并运行演示
mvn compile exec:java -Dexec.mainClass="com.smzdm.abtest.SqlTemplateDemo"
```

### 2. 基础使用示例

```java
// 创建AB测试SQL模板引擎  
SqlTemplateEngine engine = new SqlTemplateEngine();

// 加载AB测试专用模板
SqlTemplateLoader loader = new SqlTemplateLoader();
loader.loadTemplatesFromResource(engine, "sql-templates.yaml");

// 配置AB测试参数
Map<String, Object> params = new HashMap<>();
params.put("startDate", "2025-04-01 00:00:00");
params.put("endDate", "2025-04-14 10:00:00");
params.put("tableName", "bi_app.ads_zdm_user_abtest_background_v4_d");
params.put("scence", "new_user");

// AB测试分组配置
params.put("controlGroups", "68_c1,68_b1,68_c,68_b");
params.put("experimentGroups", "68_f1,68_d1,68_f,68_d");
params.put("baselineGroups", "68_z64");

// 版本过滤
params.put("versionFilter", true);
params.put("minVersion", "9.6.20");

// 配置分析指标
params.put("metricFields", Arrays.asList(
    createMetricField("app_search_synth_valid_order", "sum"),
    createMetricField("app_search_haojia_article_valid_cps", "sum"),
    createMetricField("whole_valid_gmv", "sum")
));

// 启用高级分析功能
params.put("includeBaselineComparison", true);
params.put("includeSignificanceTest", true);
params.put("includeBaselineSignificanceTest", true);

// 生成AB测试分析SQL
String sql = engine.generateSql("abtest_analysis", params);
System.out.println(sql);
```

## 🔧 核心组件详解

### SqlTemplateEngine - 模板引擎
AB测试专用SQL模板引擎，预编译模板提升性能。

**主要方法：**
```java
// 添加模板
engine.addTemplate(SqlTemplate template)

// 生成SQL
String sql = engine.generateSql("templateName", params)

// 获取模板列表
Set<String> templates = engine.getTemplateNames()

// 移除模板
engine.removeTemplate("templateName")
```

### SqlGenerator - SQL构建工具
提供便捷的条件、更新、聚合函数构建器。

**条件构建示例：**
```java
// 构建查询条件
List<Map<String, Object>> conditions = SqlGenerator.conditions()
    .eq("status", 1)                    // 等于条件
    .gt("age", 18)                      // 大于条件
    .like("name", "%张%")               // LIKE查询
    .in("category", Arrays.asList("A", "B", "C"))  // IN查询
    .build();
```

**聚合函数构建：**
```java
// 构建聚合指标
List<Map<String, Object>> aggregations = SqlGenerator.aggregations()
    .count("*", "total_users")          // 计数
    .sum("amount", "total_amount")      // 求和
    .avg("score", "avg_score")          // 平均值
    .max("price", "max_price")          // 最大值
    .min("price", "min_price")          // 最小值
    .build();
```

### SqlTemplateLoader - 配置加载器
从YAML配置文件加载AB测试模板定义。

```java
SqlTemplateLoader loader = new SqlTemplateLoader();

// 验证配置文件
if (loader.validateTemplateConfig("sql-templates.yaml")) {
    // 加载模板配置
    loader.loadTemplatesFromResource(engine, "sql-templates.yaml");
}
```

## 📊 AB测试模板功能

### 核心分析模板：`abtest_analysis`

这是系统的核心模板，支持完整的AB测试分析流程：

#### 🎯 基础数据统计
- **用户数统计**：各分组的用户数量
- **指标聚合**：自定义指标的平均值和标准差计算
- **按天分组**：支持按天聚合的用户行为分析

#### 📈 对比分析  
- **实验组vs对照组**：计算相对提升比例
- **实验组vs基线流**：与历史基线数据对比（可选）
- **多指标同步对比**：支持多个业务指标同时分析

#### 🔬 统计显著性检验
- **t检验计算**：基于均值和标准差的显著性判断
- **1.96阈值**：95%置信区间的显著性判断
- **双重验证**：对照组和基线流的双重显著性检验

#### 🔍 高级过滤功能
- **版本过滤**：智能版本号解析和比较
- **自定义条件**：支持任意SQL条件过滤
- **分组识别**：基于abtest_collection字段的智能分组

## 💡 使用场景

### 1. 新功能效果验证
```java
// 验证新搜索算法的效果
params.put("scence", "search_algorithm");
params.put("controlGroups", "old_algo_v1,old_algo_v2");
params.put("experimentGroups", "new_algo_v1,new_algo_v2");
params.put("metricFields", Arrays.asList(
    createMetricField("search_click_rate", "avg"),
    createMetricField("search_conversion_rate", "avg"),
    createMetricField("user_satisfaction_score", "avg")
));
```

### 2. 页面改版效果分析
```java
// 分析页面改版对用户行为的影响
params.put("scence", "page_redesign");
params.put("metricFields", Arrays.asList(
    createMetricField("page_view_duration", "avg"),
    createMetricField("bounce_rate", "avg"),
    createMetricField("conversion_rate", "avg")
));
params.put("includeSignificanceTest", true);
```

### 3. 推荐算法优化
```java
// 评估推荐算法的改进效果
params.put("scence", "recommendation");
params.put("filterCondition", "recommendation_expose_count > 0");
params.put("metricFields", Arrays.asList(
    createMetricField("recommendation_click_rate", "avg"),
    createMetricField("recommendation_purchase_rate", "avg"),
    createMetricField("user_engagement_score", "avg")
));
```

## 📋 配置参数详解

### 必需参数
| 参数名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `startDate` | String | 分析开始日期 | "2025-04-01 00:00:00" |
| `endDate` | String | 分析结束日期 | "2025-04-14 10:00:00" |
| `tableName` | String | 数据表名 | "bi_app.ads_zdm_user_abtest_background_v4_d" |
| `scence` | String | 测试场景 | "new_user" |
| `controlGroups` | String | 对照组标识 | "68_c1,68_b1" |
| `experimentGroups` | String | 实验组标识 | "68_f1,68_d1" |
| `metricFields` | List | 分析指标配置 | [见指标配置] |

### 可选参数
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `baselineGroups` | String | null | 基线流标识 |
| `versionFilter` | Boolean | false | 是否启用版本过滤 |
| `minVersion` | String | null | 最小版本号 |
| `filterCondition` | String | null | 自定义过滤条件 |
| `includeBaselineComparison` | Boolean | false | 包含基线流对比 |
| `includeSignificanceTest` | Boolean | false | 包含显著性检验 |
| `includeBaselineSignificanceTest` | Boolean | false | 包含基线流显著性检验 |

### 指标字段配置
```java
// 创建指标字段配置
private static Map<String, Object> createMetricField(String fieldName, String aggregateFunction, boolean hasNext) {
    Map<String, Object> field = new HashMap<>();
    field.put("fieldName", fieldName);           // 字段名
    field.put("aggregateFunction", aggregateFunction);  // 聚合函数：sum/avg/max/min
    field.put("hasNext", hasNext);               // 是否还有后续字段
    return field;
}
```

## 🎛️ 模板配置文件

`src/main/resources/sql-templates.yaml` 配置示例：

```yaml
templates:
  - name: "abtest_analysis"
    description: "AB测试分析模板，支持传入具体字段进行统计"  
    template: |
      -- AB测试核心分析SQL
      -- 基础数据统计部分
      select concat('{{startDate}}','~','{{endDate}}') as `date_range`,
             abtest,
             cast(user_cnt as String) as `user_num`,
             {{#metricFields}}
             cast(ifnull(avg_{{fieldName}},0) as String) as peruser_{{fieldName}}{{#hasNext}},{{/hasNext}}
             {{/metricFields}}
      from (
        -- 用户级别聚合统计
        select abtest,
               count(device_id) as `user_cnt`,
               {{#metricFields}}
               round(avg(sum_{{fieldName}}),6) as avg_{{fieldName}},
               round(stddevPop(sum_{{fieldName}}),6) as std_{{fieldName}}{{#hasNext}},{{/hasNext}}
               {{/metricFields}}
        from (
          -- 设备级别数据聚合
          select dt, device_id,
                 if(hasAny(splitByString(',',abtest_collection),['{{controlGroups}}'])=1,'对照组',
                    if(hasAny(splitByString(',',abtest_collection),['{{baselineGroups}}'])=1,'基线流','实验组')) as `abtest`,
                 {{#metricFields}}
                 {{aggregateFunction}}({{fieldName}}) as sum_{{fieldName}}{{#hasNext}},{{/hasNext}}
                 {{/metricFields}}
          from {{tableName}}
          where dt between toDate('{{startDate}}') and toDate('{{endDate}}')
            and scence = '{{scence}}'
            {{#versionFilter}}
            and match(app_version,'无')=0 
            and app_version<>'' 
            and app_version <>'null' 
            and match(app_version, '^[^一-龥]*$') = 1
            and (cast(CONCAT(leftPad(splitByString('.',app_version)[1],3,'0'),
                           leftPad(splitByString('.',app_version)[2],3,'0'),
                           leftPad(splitByString('.',app_version)[3],3,'0'),
                           leftPad(splitByString('.',app_version)[4],3,'0')) as bigint) >= 
                 cast(CONCAT(leftPad(splitByString('.','{{minVersion}}')[1],3,'0'),
                           leftPad(splitByString('.','{{minVersion}}')[2],3,'0'),
                           leftPad(splitByString('.','{{minVersion}}')[3],3,'0'),
                           leftPad(splitByString('.','{{minVersion}}')[4],3,'0')) as bigint))
            {{/versionFilter}}
            and (hasAny(splitByString(',',abtest_collection),['{{controlGroups}}'])=1 
                 or hasAny(splitByString(',',abtest_collection),['{{experimentGroups}}'])=1 
                 or hasAny(splitByString(',',abtest_collection),['{{baselineGroups}}'])=1)
          group by dt, device_id, abtest
        )
        {{#filterCondition}}
        where ({{filterCondition}})
        {{/filterCondition}}
        group by abtest
      )
      
      -- 实验组与对照组对比分析
      union all
      select concat('{{startDate}}','~','{{endDate}}') as `date_range`,
             '实验组/对照组' as abtest_scale,
             '--' as `user_num`,
             {{#metricFields}}
             cast(ifnull(round(sum(if(abtest='实验组',avg_{{fieldName}},0))/
                              sum(if(abtest='对照组',avg_{{fieldName}},0))-1,6),0) as String) as ratio_{{fieldName}}{{#hasNext}},{{/hasNext}}
             {{/metricFields}}
      from (
        -- [重复上面的用户级别聚合统计子查询]
      )
      
      {{#includeSignificanceTest}}
      -- 统计显著性检验
      union all  
      select concat('{{startDate}}','~','{{endDate}}') as `date_range`,
             '显著性' as abtest_scale,
             '--' as `user_num`,
             {{#metricFields}}
             if((abs(sum(if(abtest='实验组',avg_{{fieldName}},0))-sum(if(abtest='对照组',avg_{{fieldName}},0)))/
                sqrt((power(sum(if(abtest='实验组',std_{{fieldName}},0)),2)/sum(if(abtest='实验组',user_cnt,0))+
                      power(sum(if(abtest='对照组',std_{{fieldName}},0)),2)/sum(if(abtest='对照组',user_cnt,0)))))>1.96,
                '显著','不显著') as significance_{{fieldName}}{{#hasNext}},{{/hasNext}}
             {{/metricFields}}
      from (
        -- [重复上面的用户级别聚合统计子查询]
      )
      {{/includeSignificanceTest}};

default_params:
  versionFilter: false
  includeBaselineComparison: false
  includeSignificanceTest: false
  includeBaselineSignificanceTest: false
```

## 🔍 输出结果说明

### 基础统计行
- **date_range**: 分析时间范围
- **abtest**: 分组名称（对照组/实验组/基线流）  
- **user_num**: 该分组用户数量
- **peruser_[指标名]**: 每用户平均指标值

### 对比分析行
- **abtest_scale**: "实验组/对照组" 或 "实验组/基线流"
- **ratio_[指标名]**: 相对提升比例（实验组相对对照组的提升）

### 显著性检验行
- **abtest_scale**: "显著性" 或 "基线流显著性"
- **significance_[指标名]**: "显著" 或 "不显著"

## 🛠️ 最佳实践

### 1. 指标选择原则
```java
// ✅ 好的做法：选择核心业务指标
metricFields.add(createMetricField("conversion_rate", "avg", true));      // 转化率
metricFields.add(createMetricField("user_retention_rate", "avg", true));  // 留存率
metricFields.add(createMetricField("revenue_per_user", "avg", false));    // 用户价值

// ❌ 避免：选择过多相关性强的指标
```

### 2. 实验周期建议
```java
// ✅ 推荐：至少2周的测试周期
params.put("startDate", "2025-04-01 00:00:00");
params.put("endDate", "2025-04-14 23:59:59");

// ❌ 避免：过短的测试周期（<7天）
```

### 3. 版本过滤使用
```java
// ✅ 当需要排除旧版本干扰时启用
params.put("versionFilter", true);
params.put("minVersion", "9.6.20");  // 确保版本号格式正确

// ❌ 不要：在版本差异不大时过度过滤
```

### 4. 显著性检验最佳实践
```java
// ✅ 推荐：同时检验多个关键指标
params.put("includeSignificanceTest", true);

// ✅ 当有基线流时也检验基线对比
if (hasBaselineData) {
    params.put("includeBaselineComparison", true);
    params.put("includeBaselineSignificanceTest", true);
}
```

## ⚠️ 注意事项

1. **数据质量**：确保abtest_collection字段数据完整性
2. **样本大小**：保证各分组有足够的样本量（建议>1000用户）
3. **版本格式**：版本号必须符合x.y.z.w格式才能正确过滤
4. **指标相关性**：避免选择高度相关的指标，可能导致多重检验问题
5. **时间选择**：避免包含异常日期（如大促活动日）
6. **显著性解读**：显著≠重要，需要结合业务意义判断

## 📦 依赖项

```xml
<!-- Mustache模板引擎 -->
<dependency>
    <groupId>com.github.spullara.mustache.java</groupId>
    <artifactId>compiler</artifactId>
    <version>0.9.10</version>
</dependency>

<!-- Jackson YAML支持 -->
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-yaml</artifactId>
    <version>2.15.2</version>
</dependency>

<!-- SLF4J日志 -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.36</version>
</dependency>
```

## 🚀 系统要求

- **Java**: 8+
- **Maven**: 3.6+
- **数据库**: ClickHouse（推荐）或其他支持复杂SQL的数据库

---

通过这个AB测试SQL模板化系统，你可以快速生成专业的AB测试分析SQL，提升数据分析效率，确保实验结果的科学性和可靠性。 