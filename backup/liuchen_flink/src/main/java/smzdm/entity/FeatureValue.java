package com.smzdm.entity;

/**
 * 特征值实体类，包含展示数、点击数和CTR
 * 
 * @author liuchen
 * @since 1.0
 */
public class FeatureValue {
    private final int imp;    // 展示数
    private final int click;  // 点击数
    private final double ctr; // 点击率

    public FeatureValue(int imp, int click, double ctr) {
        this.imp = imp;
        this.click = click;
        this.ctr = ctr;
    }
    
    public int getImp() {
        return imp;
    }
    
    public int getClick() {
        return click;
    }
    
    public double getCtr() {
        return ctr;
    }
    
    /**
     * 创建FeatureValue实例，自动计算CTR
     * @param imp 展示数
     * @param click 点击数
     * @return FeatureValue实例
     */
    public static FeatureValue create(int imp, int click) {
        double ctrRaw = (imp == 0) ? 0.0 : (double) click / imp;
        double ctr = Math.round(ctrRaw * 100000.0) / 100000.0;
        return new FeatureValue(imp, click, ctr);
    }
    
    @Override
    public String toString() {
        return String.format("FeatureValue{imp=%d, click=%d, ctr=%.5f}", imp, click, ctr);
    }
} 