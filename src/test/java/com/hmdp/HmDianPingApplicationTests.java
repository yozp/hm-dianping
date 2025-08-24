package com.hmdp;

import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private IShopService shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

//    void testSaveShop(){
//        shopService.saveShop2Redis(1L,10L);
//    }

    @Test
    void testIdWorker() throws InterruptedException {
        //当CountDownLatch  内部维护的 变量变为0时，就不再阻塞
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            //调用一次countDown ，内部变量就减少1
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        //这里的await是主线程等待上面的线程结束（异步），上面的for里面的不会等待task执行完才跳下一次
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    /**
     * 测试使用Redission的分布式锁
     */
    @Test
    void testRedission() throws Exception {
        //获取锁(可重入)，指定锁的名称
        RLock lock = redissonClient.getLock("anyLock");
        //尝试获取锁，参数分别是：获取锁的最大等待时间(期间会重试)，锁自动释放时间，时间单位
        boolean isLock = lock.tryLock(1, 10, TimeUnit.SECONDS);
        //判断获取锁成功
        if (isLock) {
            try {
                System.out.println("执行业务");
            } finally {
                lock.unlock();
            }
        }
    }

    @Test
    void testHyperLogLog() {
        //准备数据，装用户数据
        String[] users = new String[100];
        //数组角标
        int index = 0;
        for (int i = 1; i <= 1000000; i++) {
            //赋值
            users[index++] = "user_" + i;
            //每1000条发送一次
            if (i % 1000 == 0) {
                index = 0;
                stringRedisTemplate.opsForHyperLogLog().add("hll1", users);
            }
        }
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hll1");
        System.out.println("size = " + size);
    }
}
