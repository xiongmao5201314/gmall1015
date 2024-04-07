package com.atguigu.gmallpublisher.service;


import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    Long getDau(String date);
    Map<String,Long> getHourDau(String date);
    Double getTotalAmount(String date);
    Map<String,Double> getHourAmount(String date);
    Map<String,Object> getSaleDetailAndAggGroupByField(
            String date,
            String keyWord,
            int startPage,
            int sizePerpage,
            String aggField,
            int aggCount
    ) throws IOException;
}
