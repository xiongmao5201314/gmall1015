package com.atguigu;

public class regex {
    public static void main(String[] args) {
        String s = "今今今今天天我请请你吃吃饭";
//        System.out.println(s.matches("\\w+"));
        System.out.println(s.replaceAll("(.)\\1+", "$1"));
    }
}
