package smzdm.entity;

/**
 * 计算实体类
 */
public class CalEntity {
    private int imp = 0;   // 展示数
    private int click = 0; // 点击数

    public CalEntity() {}

    public CalEntity(int imp, int click) {
        this.imp = imp;
        this.click = click;
    }

    public int getImp() { return imp; }
    public void setImp(int imp) { this.imp = imp; }
    public int getClick() { return click; }
    public void setClick(int click) { this.click = click; }

    /**
     * 增加展示数
     */
    public void addImp() { this.imp++; }
    
    /**
     * 增加点击数
     */
    public void addClick() { this.click++; }
    
    @Override
    public String toString() {
        return "CalEntity{imp=" + imp + ", click=" + click + '}';
    }
} 