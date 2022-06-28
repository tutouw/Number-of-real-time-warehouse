package com.iflytek.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Aaron
 * @date 2022/6/28 12:09
 */

@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 首页独立访客数
    Long homeUvCt;

    // 商品详情页独立访客数
    Long goodDetailUvCt;

    // 时间戳
    Long ts;
}
