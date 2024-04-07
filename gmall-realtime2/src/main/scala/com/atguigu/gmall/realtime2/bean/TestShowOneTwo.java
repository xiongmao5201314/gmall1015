package com.atguigu.gmall.realtime2.bean;



public class TestShowOneTwo {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        ShowNumberTwo st = new ShowNumberTwo();
        Thread one = new Thread(st);
        one.setName("肯德基线程");
        one.start();
        Thread two = new Thread(st);
        two.setName("麦当劳线程");
        two.start();
    }

}
class ShowNumberTwo implements Runnable {

    @Override
    public void run() {

        for (int i = 1; i <= 100; i++) {
            synchronized (this) {
                this.notify();
                System.out.println(Thread.currentThread().getName() + "线程正在打印===================="+i);
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}