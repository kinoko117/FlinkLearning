package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WordLen;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lzc
 * @Date 2022/3/12 11:12
 */
public class Flink02_Function_Table {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setString("table.exec.sink.not-null-enforcer", "drop");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        DataStreamSource<String> stream = env.fromElements(
                "hello hello atguigu",
                "hello atguigu",
                "zs lisi wangwu");


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 把流转成一个表(动态表)
        Table table = tEnv.fromDataStream(stream, $("line"));
        table.printSchema();
        tEnv.createTemporaryView("sensor", table);
        // 使用自定义函数
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        /*table
                .joinLateral(call(Split.class, $("line")))
                .select($("line"), $("word"), $("len"))
                .execute()
                .print();*/
        // 1.2 先注册后使用
        tEnv.createTemporaryFunction("split", Split.class);
        table
//                .joinLateral(call("split", $("line")))// 默认是内连接
                .leftOuterJoinLateral(call("split", $("line")))
                .select($("line"), $("word"), $("len"))
                .execute()
                .print();

        // 2. 在sql中使用
        // 先注册

    }
    // Row用来表示制成的表的每行数据的封装 也可以使用POJO
    // Row是一种弱类型, 需要明确的指定字段名和类型
    /*@FunctionHint(output = @DataTypeHint("row<word string, len int>"))
    public static class Split extends TableFunction<Row> {
        public void eval(String line){
            // 数组的长度是几, 制成的表就几行
            String[] words = line.split(" ");
            for (String word : words) {
                //of方法传入几个参数, 就表示一行有几列
                collect(Row.of(word, word.length()));  // 调用一次, 就有一行数据
            }
        }
    }*/
    // POJO是一种强类型, 每个字段的类型和名字都是和POJO中的属性保持了一致. 不用额外的配置
    public static class Split extends TableFunction<WordLen> {
        public void eval(String line){

            // 一些特殊情况, 这个值不生成表
            if (line.contains("zs")) {
                return;
            }

            // 数组的长度是几, 制成的表就几行
            String[] words = line.split(" ");
            for (String word : words) {
                //of方法传入几个参数, 就表示一行有几列
                collect(new WordLen(word, word.length()));  // 调用一次, 就有一行数据
            }
        }
    }

}
/*
 "hello hello atguigu"
                        hello   5
                        hello   5
                        atguigu  7
"hello atguigu"
                        hello 5
                        atguigu 7

....


 */