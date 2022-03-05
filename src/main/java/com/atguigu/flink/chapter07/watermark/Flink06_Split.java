package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lzc
 * @Date 2022/3/5 10:00
 */
public class Flink06_Split {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> main = env
                .socketTextStream("hadoop162", 9999)
                .map(value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor ws,
                                               Context ctx,
                                               Collector<WaterSensor> out) throws Exception {
                        // sensor1 放入主流   sensor2 放一个侧输出流  其他放入一个侧输出流
                        if ("sensor_1".equals(ws.getId())) {
                            out.collect(ws);
                        }else if("sensor_2".equals(ws.getId())){
                            ctx.output(new OutputTag<WaterSensor>("s2"){}, ws);
                        }else{
                            ctx.output(new OutputTag<WaterSensor>("other"){}, ws);
                        }
                    }
                });

        main.print("s1");
        main.getSideOutput(new OutputTag<WaterSensor>("s2"){}).print("s2");
        main.getSideOutput(new OutputTag<WaterSensor>("other"){}).print("other");



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*

测输出流的第二个作用:
 分流
 */
