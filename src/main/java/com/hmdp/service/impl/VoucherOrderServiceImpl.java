package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.apache.tomcat.util.scan.UrlJar;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.LOCK_ORDER_KEY;

/**
 * 秒杀优惠券服务
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //Spring Data Redis提供的类，用于封装Redis Lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));//设置Lua脚本文件位置
        SECKILL_SCRIPT.setResultType(Long.class);//指定脚本返回结果类型为Long
    }

    //--------------------------------------------------------------------------------------------------------------

    //创建一个用于异步处理的线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
    @PostConstruct//Spring 容器初始化后立即启动消费者线程。
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //初始化一个容量为 1,048,576（1024×1024）的阻塞队列，用于临时存储秒杀订单
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    //代理对象
    private IVoucherOrderService proxy;

    //2）
    // 用于线程池处理的任务
    // 当初始化完毕后，就会去从队列中去拿信息
    private class VoucherOrderHandler implements Runnable {

        //主循环处理新消息
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取队列中的订单信息
                    //XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),// 消费者组g1，消费者实例c1
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),// 每次读1条，阻塞2秒
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())// 从 stream.orders 读取新消息，> 表示新消息
                    );
                    // 2. 跳过空消息
                    if(list==null||list.isEmpty()){
                        continue;
                    }
                    // 3. 消息解析
                    MapRecord<String, Object, Object> record = list.get(0);
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(record.getValue(), new VoucherOrder(), true);
                    // 4.创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5.确认消息已处理完成 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1","g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常！", e);
                    handlePendingList(); // 异常时转去处理失败消息
                }
            }
        }

        //专门处理失败消息
        private void handlePendingList() {
            while(true){
                try{
                    // 1. 从Pending List读取(0表示待处理消息)
                    //XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),// 消费者组 g1，消费者 c1
                            StreamReadOptions.empty().count(1),// 每次读取 1 条消息（不阻塞）
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))//0 表示读取 Pending List 中的消息
                    );
                    // 2. 无异常消息时退出
                    if(list==null||list.isEmpty()){
                        break;
                    }
                    // 3. 消息解析与业务处理（同主流程）
                    MapRecord<String, Object, Object> record = list.get(0);
                    //将Redis中的Hash结构转换为Java对象
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(record.getValue(), new VoucherOrder(), true);
                    createVoucherOrder(voucherOrder);
                    // 4. 成功处理后的ACK
                    stringRedisTemplate.opsForStream().acknowledge("s1","g1",record.getId());
                }catch (Exception e){
                    log.error("处理pendding订单异常！", e);
                    try{
                        Thread.sleep(20);//休眠2毫秒
                    }catch (Exception e2){
                        e2.printStackTrace();
                    }
                }
            }
        }
    }

    //3）
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = UserHolder.getUser().getId();
        // 2.创建锁对象
        RLock redisLock = redissonClient.getLock(LOCK_ORDER_KEY + userId);
        // 3.尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 4.判断是否获得锁成功
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }
        try {
            //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            redisLock.unlock();
        }
    }

    //1）
    /**
     * 基于Redis的Stream结构作为消息队列，实现异步秒杀下单
     * 将耗时比较短的逻辑判断放入到redis中，如秒杀库存、秒杀订单
     * <p>
     * 我们去下单时，是通过lua表达式去原子执行判断逻辑，
     * 如果判断我出来不为0，则要么是库存不足，要么是重复下单，返回错误信息，
     * 如果是0，则把下单的逻辑保存到队列中去，然后异步执行
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //生成订单id
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,// Lua 脚本
                Collections.emptyList(),// KEYS 为空（或无键参数）
                voucherId.toString(), //ARGV[1]
                userId.toString(), //ARGV[2]
                String.valueOf(orderId) //ARGV[3]
        );
        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足！" : "不能重复下单！");
        }

        //这里的代码在lua已经实现过，这里是起到一个兜底作用
        // 3.封装订单对象
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        // 4.放入阻塞队列
        orderTasks.add(voucherOrder);
        // 5.获取代理对象
        proxy = (IVoucherOrderService)AopContext.currentProxy();

        // 3.返回订单id
        return Result.ok(orderId);
    }

    //4）
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //这里的代码在lua已经实现过，这里是起到一个兜底作用
        //一人一单
        Long userId = UserHolder.getUser().getId();
        Long voucherId = voucherOrder.getVoucherId();
        //5.1.用户id和对应的订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        //5.2.判断是否存在
        if (count > 0) {
            //用户存在且购买过
            log.error("用户已经购买过一次！");
            return;
        }
        //6，扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0) //乐观锁防止超卖问题
                .update();
        if (!success) {
            log.error("库存不足！");
        }
        //7.创建订单
        save(voucherOrder);
    }

    //--------------------------------------------------------------------------------------------------------------

//    /**
//     * 基于分布式锁Redission解决秒杀服务
//     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 3.判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束！");
//        }
//        // 4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//
//        //5.一人一单
//        //重点：保证一个线程的拿锁、比锁、删锁操作都在同一事务中，保证原子性操作
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //（已废弃，改为使用Redission）
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        //Redission创建分布式锁
//        RLock lock = redissonClient.getLock(LOCK_ORDER_KEY + userId);
//        //获取锁对象
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            return Result.fail("不允许重复下单！");
//        }
//        //分布式锁
//        try {
//            //获取代理对象，才能使得事务生效！（不能直接使用this）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }

//    /**
//     * 基于阻塞队列实现秒杀优化
//     * 将耗时比较短的逻辑判断放入到redis中，如秒杀库存、秒杀订单
//     * <p>
//     * 我们去下单时，是通过lua表达式去原子执行判断逻辑，
//     * 如果判断我出来不为0，则要么是库存不足，要么是重复下单，返回错误信息，
//     * 如果是0，则把下单的逻辑保存到队列中去，然后异步执行
//     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //生成订单id
//        long orderId = redisIdWorker.nextId("order");
//        // 1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,// Lua 脚本
//                Collections.emptyList(),// KEYS 为空（或无键参数）
//                voucherId.toString(), //ARGV[1]
//                userId.toString(), //ARGV[2]
//                String.valueOf(orderId) //ARGV[3]
//        );
//        // 2.判断结果是否为0
//        int r = result.intValue();
//        if (r != 0) {
//            // 2.1.不为0 ，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足！" : "不能重复下单！");
//        }
//        //TODO 保存阻塞队列
//        // 3.返回订单id
//        return Result.ok(orderId);
//    }
//
//    /**
//     * 确保线程安全
//     */
//    @Override
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        //5.一人一单
//        Long userId = UserHolder.getUser().getId();
//        //5.1.用户id和对应的订单
//        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        //5.2.判断是否存在
//        if (count > 0) {
//            //用户存在且购买过
//            return Result.fail("用户已经购买过一次！");
//        }
//        //6，扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock=stock-1")
//                .eq("voucher_id", voucherId)
//                .gt("stock", 0) //乐观锁防止超卖问题
//                .update();
//        if (!success) {
//            return Result.fail("库存不足！");
//        }
//        //7.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 7.1.订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 7.2.用户id
//        voucherOrder.setUserId(userId);
//        // 7.3.代金券id
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//
//        //返回订单id
//        return Result.ok(orderId);
//    }

}
