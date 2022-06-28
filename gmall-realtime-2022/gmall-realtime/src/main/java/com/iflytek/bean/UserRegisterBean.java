package com.iflytek.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Aaron
 * @date 2022/6/28 18:36
 */

@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}
