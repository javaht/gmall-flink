package com.zht.app.func;

import com.zht.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/*
 * @Author root
 * @Data  2022/7/18 14:45
 * @Description
 * */
@FunctionHint(output =@DataTypeHint("Row<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword)  {
        List<String> keyList = null;
        try {
            keyList = KeywordUtil.splitKeyWord(keyword);
            for (String word : keyList) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
           collect(Row.of(keyword));
        }

    }

}
