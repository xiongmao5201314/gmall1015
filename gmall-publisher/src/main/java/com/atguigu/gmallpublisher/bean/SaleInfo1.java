package com.atguigu.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaleInfo1 {
    private Integer total;
    private List<Map<String,Object>> detail;
    private List<Stat> stats = new ArrayList<>();

    public void addStat(Stat stat){
        stats.add(stat);
    }
    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<Map<String, Object>> getDetail() {
        return detail;
    }

    public void setDetail(List<Map<String, Object>> detail) {
        this.detail = detail;
    }

    public List<Stat> getStats() {
        return stats;
    }


}
