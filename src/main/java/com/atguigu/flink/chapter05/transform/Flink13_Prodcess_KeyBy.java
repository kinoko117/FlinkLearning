package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink13_Prodcess_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);

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
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //int sum = 0; // 每个并行度一个对象, 每个并行度一个sum
                    HashMap<String, Integer> idToVcSum = new HashMap<>();

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        //sum += value.getVc();

                        //out.collect(ctx.getCurrentKey() + " 水位和: " + sum);

                        Integer sum = idToVcSum.getOrDefault(ctx.getCurrentKey(), 0);
                        sum += value.getVc();
                        idToVcSum.put(ctx.getCurrentKey(), sum);
                        out.collect(ctx.getCurrentKey() + " 水位和: " + sum);

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