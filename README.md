# 动态SQL模板生成系统

这是一个基于Mustache模板引擎的动态SQL生成系统，支持从Java代码或YAML配置文件定义SQL模板，并通过参数动态生成SQL语句。

## 功能特性

- 🚀 **基于Mustache模板引擎**：强大的模板渲染能力
- 📝 **多种SQL模板**：支持查询、插入、更新、聚合等各种SQL操作
- 🔧 **链式构建器**：提供便捷的条件、更新字段、聚合函数构建器
- 📄 **配置文件支持**：支持从YAML文件加载模板定义
- 🛡️ **类型安全**：自动处理字符串和数值类型的SQL拼接
- 📊 **复杂查询支持**：支持JOIN、GROUP BY、HAVING等复杂查询
- 🔍 **模板管理**：支持动态添加、删除、查询模板

## 快速开始

### 1. 运行演示程序

```bash
mvn compile exec:java -Dexec.mainClass="com.smzdm.abtest.SqlTemplateDemo"
```

### 2. 基本使用示例

```java
// 创建SQL模板引擎
SqlTemplateEngine engine = new SqlTemplateEngine();

// 构建查询参数
Map<String, Object> params = new HashMap<>();
params.put("tableName", "users");
params.put("columns", Arrays.asList("id", "name", "email"));
params.put("conditions", SqlGenerator.conditions()
    .eq("status", 1)
    .gt("age", 18)
    .like("name", "%张%")
    .build());
params.put("orderBy", "created_at DESC");
params.put("limit", 10);

// 生成SQL
String sql = engine.generateSql("user_query", params);
```

## 核心组件

### SqlTemplateEngine
SQL模板引擎，负责管理和渲染模板。

**主要方法：**
- `addTemplate(SqlTemplate template)` - 添加模板
- `generateSql(String templateName, Map<String, Object> params)` - 生成SQL
- `getTemplateNames()` - 获取所有模板名称
- `removeTemplate(String name)` - 移除模板

### SqlGenerator
SQL生成器工具类，提供便捷的构建器方法。

**条件构建器：**
```java
SqlGenerator.conditions()
    .eq("field", "value")      // 等于
    .ne("field", "value")      // 不等于
    .gt("field", 100)          // 大于
    .ge("field", 100)          // 大于等于
    .lt("field", 100)          // 小于
    .le("field", 100)          // 小于等于
    .like("field", "%value%")  // LIKE查询
    .in("field", Arrays.asList(1, 2, 3))  // IN查询
    .build();
```

**更新构建器：**
```java
SqlGenerator.updates()
    .set("name", "新名称")
    .set("age", 25)
    .set("updated_at", "NOW()")
    .build();
```

**聚合构建器：**
```java
SqlGenerator.aggregations()
    .count("*", "total_count")
    .sum("amount", "total_amount")
    .avg("score", "avg_score")
    .max("price", "max_price")
    .min("price", "min_price")
    .build();
```

### SqlTemplateLoader
模板加载器，支持从YAML配置文件加载模板定义。

```java
SqlTemplateLoader loader = new SqlTemplateLoader();
loader.loadTemplatesFromResource(engine, "sql-templates.yaml");
```

## 内置模板

系统提供以下内置模板：

| 模板名称 | 描述 | 用途 |
|---------|------|------|
| `user_query` | 通用用户查询模板 | 基础查询操作 |
| `page_query` | 分页查询模板 | 分页数据获取 |
| `aggregation_query` | 聚合查询模板 | 统计分析查询 |
| `insert_query` | 插入数据模板 | 数据插入操作 |
| `update_query` | 更新数据模板 | 数据更新操作 |

## 配置文件模板

在 `src/main/resources/sql-templates.yaml` 中定义额外的模板：

```yaml
templates:
  - name: "report_query"
    description: "报表查询模板"
    template: |
      SELECT 
        {{#selectFields}}
        {{field}}{{#alias}} AS {{alias}}{{/alias}}{{#hasNext}},{{/hasNext}}
        {{/selectFields}}
      FROM {{tableName}}
      {{#joins}}
      {{joinType}} {{targetTable}} {{alias}} ON {{condition}}
      {{/joins}}
      WHERE {{#conditions}}{{field}} {{operator}} '{{value}}'{{#hasNext}} AND {{/hasNext}}{{/conditions}}
      {{#groupBy}}GROUP BY {{#.}}{{.}}{{#hasNext}}, {{/hasNext}}{{/.}}{{/groupBy}}
      {{#orderBy}}ORDER BY {{.}}{{/orderBy}}
      {{#limit}}LIMIT {{.}}{{/limit}};

default_params:
  limit: 100
  pageSize: 20
```

## 使用场景

### 1. 动态查询构建
根据前端传入的查询条件动态生成WHERE子句：

```java
// 根据用户输入动态添加查询条件
SqlGenerator.ConditionBuilder conditions = SqlGenerator.conditions();

if (userId != null) {
    conditions.eq("user_id", userId);
}
if (startDate != null) {
    conditions.ge("created_date", startDate);
}
if (status != null) {
    conditions.eq("status", status);
}

params.put("conditions", conditions.build());
```

### 2. 报表SQL生成
根据报表配置动态生成复杂的统计查询：

```java
// 动态添加聚合字段
SqlGenerator.AggregationBuilder aggregations = SqlGenerator.aggregations()
    .count("*", "total_records");

if (needSumAmount) {
    aggregations.sum("amount", "total_amount");
}
if (needAvgScore) {
    aggregations.avg("score", "avg_score");
}

params.put("aggregations", aggregations.build());
```

### 3. 批量操作SQL
动态生成批量插入或更新SQL：

```java
// 批量插入用户数据
List<List<Object>> userData = Arrays.asList(
    Arrays.asList("张三", "zhang@example.com", 25),
    Arrays.asList("李四", "li@example.com", 30)
);

params.put("rows", SqlGenerator.buildInsertRows(userData));
```

## 扩展模板

### 自定义模板
```java
SqlTemplate customTemplate = new SqlTemplate(
    "complex_analytics",
    "SELECT {{dimensions}}, {{metrics}} " +
    "FROM {{tableName}} " +
    "WHERE {{#dateRange}}date BETWEEN '{{startDate}}' AND '{{endDate}}'{{/dateRange}} " +
    "{{#filters}}AND {{field}} {{operator}} {{value}}{{/filters}} " +
    "GROUP BY {{dimensions}} " +
    "ORDER BY {{orderBy}};",
    "复杂分析查询"
);

engine.addTemplate(customTemplate);
```

## 最佳实践

1. **参数验证**：在生成SQL前验证必要参数是否存在
2. **SQL注入防护**：使用参数化查询，避免直接拼接用户输入
3. **模板复用**：将常用的SQL模式抽象为模板
4. **错误处理**：适当处理模板不存在或参数错误的情况
5. **性能优化**：模板会被预编译，避免重复创建引擎实例

## 依赖项

- Java 8+
- Mustache.java 0.9.10
- Jackson YAML 2.15.2
- SLF4J 1.7.36

## 注意事项

- 模板使用Mustache语法，支持条件渲染和循环
- 字符串类型的值会自动添加单引号
- 数组和列表会自动添加hasNext标记用于逗号分隔
- 配置文件必须是有效的YAML格式 