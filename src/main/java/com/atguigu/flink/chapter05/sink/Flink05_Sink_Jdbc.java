package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lzc
 * @Date 2022/3/2 13:56
 */
public class Flink05_Sink_Jdbc {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                });


        stream
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(
                        JdbcSink.sink(
                                "replace into sensor(id, ts, vc)values(?,?,?)",
                                new JdbcStatementBuilder<WaterSensor>() {
                                    // 流中每来一条数据, 回调这个方法
                                    @Override
                                    public void accept(PreparedStatement ps,
                                                       WaterSensor value) throws SQLException {
                                        ps.setString(1, value.getId());
                                        ps.setLong(2, value.getTs());
                                        ps.setInt(3, value.getVc());
                                    }
                                },
                                new JdbcExecutionOptions.Builder() // 执行参数
                                        .withBatchIntervalMs(3000)
                                        .withBatchSize(16 * 1024)
                                        .withMaxRetries(3)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withDriverName("com.mysql.jdbc.Driver")
                                        .withUrl("jdbc:mysql://hadoop162:3306/test")
                                        .withUsername("root")
                                        .withPassword("aaaaaa")
                                        .build()
                        )
                );


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
