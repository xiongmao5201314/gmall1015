package com.atguigu.gmallpublisher.service;
import com.atguigu.gmall.common.ESTUtil2;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderInfoMapper;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {

    @Autowired
    DauMapper dauMapper;
    @Autowired
    OrderInfoMapper orderInfoMapper;
    @Override
    public Long getDau(String date) {
        return dauMapper.getDau(date);
    }

    @Override
    public HashMap<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dauMapper.getHourDau(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map<String, Object> map : hourDauList) {
            String key =(String) map.get("HOUR");
            Long value =(Long) map.get("COUNT");
            result.put(key,value);
        }
        return result;
    }

    @Override
    public Double getTotalAmount(String date) {
       Double result = orderInfoMapper.getTotalAmount(date);
        return result == null ? 0 : result ;
    }

    @Override
    public Map<String, Double> getHourAmount(String date) {
        HashMap<String, Double> result = new HashMap<>();
        List<Map<String, Object>> hourAmountList = orderInfoMapper.getHourAmount(date);
        for (Map<String, Object> hourAmount : hourAmountList) {
            String key = (String) hourAmount.get("HOUR");
            Double value = ((BigDecimal) hourAmount.get("SUM")).doubleValue();
            result.put(key,value);

        }
        return result;
    }

    @Override
    public Map<String,Object> getSaleDetailAndAggGroupByField(String date,
                                                              String keyWord,
                                                              int startPage,
                                                              int sizePerpage,
                                                              String aggField,
                                                              int aggCount) throws IOException {
        HashMap<String, Object> result = new HashMap<>();
        JestClient client = ESTUtil2.getClient();
        String dsl = DSLs.getSaleDetailDSL(date, keyWord, startPage, sizePerpage, aggField, aggCount);
        Search search = new Search.Builder(dsl)
                .addIndex("sale_detail")
                .addType("_doc")
                .build();
        SearchResult searchResult = client.execute(search);
        Integer total = searchResult.getTotal();
        result.put("total",total);
       List<HashMap> detail = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;

            detail.add(source);
        }
        result.put("detail",detail);
        Map<String, Long> aggMap = new HashMap<>();
        List<TermsAggregation.Entry> buckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_" + aggField)
                .getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long count = bucket.getCount();
            aggMap.put(key,count);
        }
        result.put("agg",aggMap);
        return result;
    }
}
