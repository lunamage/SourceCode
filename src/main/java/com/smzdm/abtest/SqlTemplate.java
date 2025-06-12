package com.smzdm.abtest;

import java.util.Map;

/**
 * SQL模板类
 * 用于定义SQL模板的基本结构
 */
public class SqlTemplate {
    private String name;
    private String template;
    private String description;
    private Map<String, Object> defaultParams;

    public SqlTemplate() {}

    public SqlTemplate(String name, String template, String description) {
        this.name = name;
        this.template = template;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getDefaultParams() {
        return defaultParams;
    }

    public void setDefaultParams(Map<String, Object> defaultParams) {
        this.defaultParams = defaultParams;
    }

    @Override
    public String toString() {
        return "SqlTemplate{" +
                "name='" + name + '\'' +
                ", template='" + template + '\'' +
                ", description='" + description + '\'' +
                ", defaultParams=" + defaultParams +
                '}';
    }
} 