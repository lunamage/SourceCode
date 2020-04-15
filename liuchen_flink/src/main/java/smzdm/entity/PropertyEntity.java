package com.smzdm.entity;

/**
 * @author: liuchen
 * @date: 2019/4/3
 * @time: 11:17 AM
 * @Description: 文章属性，品类/品牌
 */
public class PropertyEntity {
    private String cate;
    private String brand;

    public String getCate() {
        return cate;
    }

    public void setCate(String cate) {
        this.cate = cate;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }
}
