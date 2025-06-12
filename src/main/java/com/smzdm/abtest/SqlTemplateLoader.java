package com.smzdm.abtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * SQL模板加载器
 * 从YAML配置文件加载SQL模板定义
 */
public class SqlTemplateLoader {
    private static final Logger logger = LoggerFactory.getLogger(SqlTemplateLoader.class);
    
    private final ObjectMapper yamlMapper;

    public SqlTemplateLoader() {
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    /**
     * 从类路径资源加载模板配置
     */
    public void loadTemplatesFromResource(SqlTemplateEngine engine, String resourcePath) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                logger.warn("未找到模板配置文件: {}", resourcePath);
                return;
            }
            
            loadTemplatesFromStream(engine, inputStream);
            logger.info("成功从资源文件加载模板: {}", resourcePath);
            
        } catch (Exception e) {
            logger.error("加载模板配置失败: {}", resourcePath, e);
            throw new RuntimeException("加载模板配置失败", e);
        }
    }

    /**
     * 从输入流加载模板配置
     */
    @SuppressWarnings("unchecked")
    public void loadTemplatesFromStream(SqlTemplateEngine engine, InputStream inputStream) {
        try {
            Map<String, Object> config = yamlMapper.readValue(inputStream, Map.class);
            
            // 加载模板定义
            List<Map<String, Object>> templates = (List<Map<String, Object>>) config.get("templates");
            if (templates != null) {
                for (Map<String, Object> templateConfig : templates) {
                    SqlTemplate template = createTemplateFromConfig(templateConfig);
                    engine.addTemplate(template);
                    logger.debug("加载模板: {}", template.getName());
                }
            }
            
            // 加载默认参数
            Map<String, Object> defaultParams = (Map<String, Object>) config.get("default_params");
            if (defaultParams != null) {
                logger.debug("加载默认参数: {}", defaultParams);
                // 这里可以将默认参数存储到引擎中，或者提供给调用方使用
            }
            
        } catch (Exception e) {
            logger.error("解析模板配置失败", e);
            throw new RuntimeException("解析模板配置失败", e);
        }
    }

    /**
     * 从配置Map创建SqlTemplate对象
     */
    private SqlTemplate createTemplateFromConfig(Map<String, Object> config) {
        String name = (String) config.get("name");
        String template = (String) config.get("template");
        String description = (String) config.get("description");
        
        if (name == null || template == null) {
            throw new IllegalArgumentException("模板配置缺少必要字段: name 或 template");
        }
        
        SqlTemplate sqlTemplate = new SqlTemplate(name, template, description);
        
        // 设置默认参数（如果有的话）
        @SuppressWarnings("unchecked")
        Map<String, Object> defaultParams = (Map<String, Object>) config.get("default_params");
        if (defaultParams != null) {
            sqlTemplate.setDefaultParams(defaultParams);
        }
        
        return sqlTemplate;
    }

    /**
     * 验证模板配置的有效性
     */
    @SuppressWarnings("unchecked")
    public boolean validateTemplateConfig(String resourcePath) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                logger.error("模板配置文件不存在: {}", resourcePath);
                return false;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> config = yamlMapper.readValue(inputStream, Map.class);
            
            List<Map<String, Object>> templates = (List<Map<String, Object>>) config.get("templates");
            if (templates == null || templates.isEmpty()) {
                logger.error("配置文件中没有找到模板定义");
                return false;
            }
            
            for (Map<String, Object> templateConfig : templates) {
                String name = (String) templateConfig.get("name");
                String template = (String) templateConfig.get("template");
                
                if (name == null || name.trim().isEmpty()) {
                    logger.error("模板名称不能为空");
                    return false;
                }
                
                if (template == null || template.trim().isEmpty()) {
                    logger.error("模板内容不能为空: {}", name);
                    return false;
                }
            }
            
            logger.info("模板配置验证通过: {}", resourcePath);
            return true;
            
        } catch (Exception e) {
            logger.error("验证模板配置失败: {}", resourcePath, e);
            return false;
        }
    }
} 