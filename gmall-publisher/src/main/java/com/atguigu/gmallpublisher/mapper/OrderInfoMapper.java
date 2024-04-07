package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderInfoMapper {
    Double getTotalAmount(String date);
    List<Map<String,Object>> getHourAmount(String date);
}
