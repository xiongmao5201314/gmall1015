package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.SaleInfo1;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.PublisherService;

import jdk.nashorn.internal.ir.ReturnNode;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    public PublisherService service;
    //http://localhost:8070/realtime-total?date=2020-02-11
    @GetMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date") String date ){
        ArrayList<Map<String, String>> result = new ArrayList<>();
        Map<String, String> map1 = new HashMap<>();
        map1.put("id","dau");
        map1.put("name","新增日活");
        map1.put("value",service.getDau(date).toString());
        result.add(map1);
        Map<String, String> map2 = new HashMap<>();
        map2.put("id","new_mid");
        map2.put("name","新增设备");
        map2.put("value","233");
        result.add(map2);
        Map<String, String> map3 = new HashMap<>();
        map3.put("id","order_amount");
        map3.put("name","新增交易额");
        map3.put("value",service.getTotalAmount(date).toString());
        result.add(map3);
        String s = JSON.toJSONString(result);
        return s;
    };

    //http://localhost:8070/realtime-hour?id=dau&date=2020-02-11
    /*
    {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
       "today":{"12":38,"13":1233,"17":123,"19":688 }}
     */
    @GetMapping("/realtime-hour")
    public String realtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){
        if("dau".equals(id)){
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(yesterday(date));

            Map<String,Map<String,Long>> result = new HashMap<>();
            result.put("today",today);
            result.put("yesterday",yesterday);
            return JSON.toJSONString(result);
        }else if("order_amount".equals(id)){
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(yesterday(date));
            Map<String,Map<String,Double>> result = new HashMap<>();
            result.put("today",today);
            result.put("yesterday",yesterday);
            return JSON.toJSONString(result);
        }
        return "";

    }

    private String yesterday(String date) {
//       return LocalDate.parse(date).plusDays(-1).toString();
       return LocalDate.parse(date).minusDays(1).toString();
    }
    @GetMapping("/sale_datail")
    public String SaleDatail(@RequestParam("date")  String date,
                             @RequestParam("startpage") int startpage,
                             @RequestParam("size") int size,
                             @RequestParam("keyword") String keyword) throws IOException {
        Map<String, Object> user_gender = service.getSaleDetailAndAggGroupByField(date,
                keyword,
                startpage,
                size,
                "user_gender",
                2);
        Map<String, Object> user_age = service.getSaleDetailAndAggGroupByField(date,
                keyword,
                startpage,
                size,
                "user_age",
                100);
        SaleInfo1 saleInfo = new SaleInfo1();
        saleInfo.setTotal((Integer) user_age.get("total"));
        List<Map<String, Object>>  detail = (List<Map<String, Object>>)user_age.get("detail");
        saleInfo.setDetail(detail);
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Map<String, Long> genderAgg = ( Map<String, Long>) user_gender.get("agg");
        for (String key : genderAgg.keySet()) {
            Option option = new Option();
            option.setName(key.replace("F","男").replace("M","女"));
            option.setValue(genderAgg.get(key));
            genderStat.addOptions(option);
        }
        saleInfo.addStat(genderStat);
        Stat ageStat = new Stat();
        ageStat.addOptions(new Option("20岁以下",0L));
        ageStat.addOptions(new Option("20岁到30岁",0L));
        ageStat.addOptions(new Option("30岁以上",0L));
        ageStat.setTitle("用户年龄占比");
        Map<String, Long> ageAgg = (Map<String, Long>) user_age.get("agg");
        for (String key : ageAgg.keySet()) {
            int age = Integer.parseInt(key);
            if(age<20){
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(opt.getValue()+ageAgg.get(key));
            }else if (20<= age && age <=30){
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(opt.getValue()+ageAgg.get(key));
            }else {
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(opt.getValue()+ageAgg.get(key));
            }
        }
        saleInfo.addStat(ageStat);
        return JSON.toJSONString(saleInfo);
    }
}
/*
[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233} ]
 */