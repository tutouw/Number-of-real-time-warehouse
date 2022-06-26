package com.iflytek.bean;

import lombok.Data;

/**
 * @author Aaron
 * @date 2022/6/26 9:54
 */
@Data
public class OrderInfoRefundBean {
    // 订单id
    String id;
    // 省份id
    String province_id;
    // 历史数据
    String old;

}
