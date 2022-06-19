package com.iflytek.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Aaron
 * @date 2022/6/19 11:05
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
    // 表名                    Phoenix表名       字段（主流：过滤，配置流：建表）   主键          建表扩展字段
    // SourceTable（主键）      SinkTable        SinkColumns                  SinkPk       SinkExtend

    // 表名
    private String sinkTable;

    // Phoenix表名
    private String sourceTable;

    // 字段（主流：过滤，配置流：建表）
    private String sinkPk;

    // 主键
    private String sinkColumns;

    // 建表扩展字段
    private String sinkExtend;
}
