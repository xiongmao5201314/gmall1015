package com.atguigu.gmallpublisher.bean;

public class Option {
    private String name;
    private Long value;

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public Option() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
