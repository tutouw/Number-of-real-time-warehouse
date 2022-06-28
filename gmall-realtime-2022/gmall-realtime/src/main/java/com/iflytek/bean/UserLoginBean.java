package com.iflytek.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Aaron
 * @date 2022/6/28 15:07
 */


@Data
@AllArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    Long ts;
}

