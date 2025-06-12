package com.smzdm.abtest;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AB测试SQL模板引擎
 * 基于Mustache模板引擎实现AB测试分析SQL的动态生成
 * 专门用于AB测试数据分析场景
 */
public class SqlTemplateEngine {
    private static final Logger logger = LoggerFactory.getLogger(SqlTemplateEngine.class);
    
    private final MustacheFactory mustacheFactory;
    private final Map<String, SqlTemplate> templates;
    private final Map<String, Mustache> compiledTemplates;

    public SqlTemplateEngine() {
        this.mustacheFactory = new DefaultMustacheFactory();
        this.templates = new ConcurrentHashMap<>();
        this.compiledTemplates = new ConcurrentHashMap<>();
        initializeDefaultTemplates();
    }

    /**
     * 初始化默认的SQL模板
     * AB测试专用引擎，不包含默认模板，通过配置文件加载
     */
    private void initializeDefaultTemplates() {
        logger.info("AB测试SQL模板引擎已初始化，等待加载配置文件模板");
    }

    /**
     * 添加SQL模板
     */
    public void addTemplate(SqlTemplate template) {
        templates.put(template.getName(), template);
        // 预编译模板
        try {
            StringReader reader = new StringReader(template.getTemplate());
            Mustache compiled = mustacheFactory.compile(reader, template.getName());
            compiledTemplates.put(template.getName(), compiled);
            logger.debug("已添加并编译模板: {}", template.getName());
        } catch (Exception e) {
            logger.error("编译模板失败: {}", template.getName(), e);
        }
    }

    /**
     * 根据模板名称和参数生成SQL
     */
    public String generateSql(String templateName, Map<String, Object> params) {
        Mustache template = compiledTemplates.get(templateName);
        if (template == null) {
            throw new IllegalArgumentException("未找到模板: " + templateName);
        }

        try {
            StringWriter writer = new StringWriter();
            template.execute(writer, params);
            String sql = writer.toString();
            logger.debug("生成SQL: {}", sql);
            return sql;
        } catch (Exception e) {
            logger.error("生成SQL失败", e);
            throw new RuntimeException("生成SQL失败", e);
        }
    }

    /**
     * 获取所有模板名称
     */
    public Set<String> getTemplateNames() {
        return new HashSet<>(templates.keySet());
    }

    /**
     * 获取模板
     */
    public SqlTemplate getTemplate(String name) {
        return templates.get(name);
    }

    /**
     * 移除模板
     */
    public void removeTemplate(String name) {
        templates.remove(name);
        compiledTemplates.remove(name);
        logger.info("已移除模板: {}", name);
    }

    /**
     * 清空所有模板
     */
    public void clearTemplates() {
        templates.clear();
        compiledTemplates.clear();
        logger.info("已清空所有模板");
    }

    /**
     * 获取模板数量
     */
    public int getTemplateCount() {
        return templates.size();
    }
} 