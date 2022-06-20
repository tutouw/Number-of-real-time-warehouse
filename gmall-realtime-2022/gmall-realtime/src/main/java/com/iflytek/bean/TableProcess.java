package com.iflytek.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Aaron
 * @date 2022/6/19 11:05
 */


@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    private String sourceTable;
    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;
}