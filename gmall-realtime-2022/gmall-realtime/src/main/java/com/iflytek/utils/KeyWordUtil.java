package com.iflytek.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 分词器工具类
 *
 * @author Aaron
 * @date 2022/6/26 20:36
 */

public class KeyWordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {

        // 创建结果集合用来存放结果数据
        ArrayList<String> result = new ArrayList<>();

        // 创建分词器对象
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword), false);

        // 提取分词
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            String word = next.getLexemeText();
            result.add(word);
            next = ikSegmenter.next();
        }

        return result;
    }

/*    public static void main(String[] args) throws IOException {
        List<String> list = splitKeyWord("科大讯飞大数据Flink实时数仓");

        System.out.println(list);

    }*/
}
