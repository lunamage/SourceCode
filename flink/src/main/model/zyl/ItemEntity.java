package zyl;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * @author: liuchen
 * @date: 2019/1/16
 * @time: 2:30 PM
 * @Description:
 */
public class ItemEntity {
    @JSONField(name = "a")
    private String itemId;

    private String sp;

    @JSONField(name = "sit")
    private String timestamp;

    public String getSp() {
        return sp;
    }

    public void setSp(String sp) {
        this.sp = sp;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
