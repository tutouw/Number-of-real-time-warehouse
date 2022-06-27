package com.iflytek.app.func;

import com.iflytek.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.FunctionHints;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * 自定义分词函数
 *
 * @author Aaron
 * @date 2022/6/26 22:30
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String keyword) {
        List<String> list;
        try {
            list = KeyWordUtil.splitKeyWord(keyword);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
        }
    }
}
