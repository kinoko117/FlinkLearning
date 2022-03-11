package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink03_Table_Connector_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 直接从文件读取数据
        tEnv
                .connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");  // 创建换一个临时表: 表名 sensor
        // 使用临时表的表明, 得到一个表对象
        Table result = tEnv.from("sensor").select($("id"), $("vc"));

        // 把表写入到文件中
        // 建立一个动态表A与文件关联, 然后把数据写入到动态表A中, 则自动写入到文件中
        tEnv
                .connect(new FileSystem().path("input/a.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("vc", DataTypes.INT()))
                .createTemporaryTable("s1");


//        tEnv.insertInto("s1", result);  // 无效
        result.executeInsert("s1");  // 这个是表的方法

    }
}
