package com.sankuai.inf.leaf.server.demo;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Description:守护线程测试
 * </p>
 *
 * @author zcy
 * @date 2019/11/20 16:19
 */
public class TestDaemon extends Thread {

    public static final Logger LOGGER = LoggerFactory.getLogger(TestDaemon.class);

    private void scheduledUploadData() {
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "schedule-upload-time");
                thread.setDaemon(false);
                return thread;
            }
        }).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                System.out.println("hello world");
            }
        }, 1L, 3L, TimeUnit.SECONDS);//每3s上报数据

    }

    public static void main(String[] args) {
        TestDaemon testDaemon = new TestDaemon();
        testDaemon.scheduledUploadData();
        System.out.println("main end");
    }
}
