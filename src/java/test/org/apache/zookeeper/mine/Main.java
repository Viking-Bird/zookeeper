package org.apache.zookeeper.mine;

/**
 * @author pengwang
 * @date 2020/06/01
 */
public class Main {

    public static void main(String[] args) {
        long currentTime = System.currentTimeMillis();
        System.out.println(400 / 200);
        long interval = (currentTime / 200 + 1) * 2000;
        System.out.println(interval);
    }
}