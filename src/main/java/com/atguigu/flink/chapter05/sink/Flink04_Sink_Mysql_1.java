package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author lzc
 * @Date 2022/3/2 13:56
 */
public class Flink04_Sink_Mysql_1 {
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
                .addSink(new MysqlSink());


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MysqlSink extends RichSinkFunction<WaterSensor>{

        private Connection conn;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 建立到mysql的连接
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test", "root", "aaaaaa"); //alt+ctrl+enter
//            conn.setAutoCommit(false);

        }

        @Override
        public void close() throws Exception {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        // 在这个方法内实现具体你的写入动作
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            // 通过连接对象得到一个预处理语句 PrepareStatement
            // 第一次插入, 二次应该是更新, key重复
//            String sql = "insert into sensor(id, ts, vc)values(?,?,?)";
//            String sql = "insert into sensor(id, ts, vc)values(?,?,?) on duplicate key update vc=?";
            String sql = "replace into sensor(id, ts, vc)values(?,?,?)";
            PreparedStatement ps = conn.prepareStatement(sql);
            // sql语句中u占位符, 需要给占位符赋值
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            // 执行写入
            ps.execute();
            ps.close();
        }
    }
}
