package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink12_Prodcess_NoKey {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 4L, 30),
                new WaterSensor("sensor_1", 2L, 20),
                new WaterSensor("sensor_1", 5L, 40),
                new WaterSensor("sensor_2", 4L, 100),
                new WaterSensor("sensor_2", 5L, 200)
        );

        stream
                .process(new ProcessFunction<WaterSensor, String>() {
                    int sum = 0;  // 其实就是状态  flink的专门状态管理可以解决这些问题
                    // 流中每来一个元素, 就触发一次这个方法
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        sum += value.getVc();
                        out.collect("水位和: " + sum);

                    }
                })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*



 */