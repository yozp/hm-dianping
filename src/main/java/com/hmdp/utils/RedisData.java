package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 用于封装逻辑过期数据
 */
@Data
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}
