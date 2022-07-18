package com.zht.utils;
/*
 * @Author root
 * @Data  2022/7/18 13:40
 * @Description
 * */
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {
      //创建集合用于存放结果
        ArrayList<String> result = new ArrayList<>();
        StringReader reader = new StringReader(keyword);
        //创建分词器对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader,false);
        //提取分词

        Lexeme next = ikSegmenter.next();
        while (next!=null){
            String word = next.getLexemeText();
            result.add(word);

            next = ikSegmenter.next();
        }
        return  result;
    }

//    public static void main(String[] args) throws IOException {
//        List<String> list = splitKeyWord("这是一个测试的程序");
//        System.out.println(list);
//    }

}
