package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list;//博客列表
    private Long minTime;//最小时间戳，作为下一次查询的条件
    private Integer offset;//与上一次查询相同的查询个数作为偏移量
}
