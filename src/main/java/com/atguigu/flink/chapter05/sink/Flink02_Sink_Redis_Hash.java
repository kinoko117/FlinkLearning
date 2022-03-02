package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author lzc
 * @Date 2022/3/2 13:56
 */
public class Flink02_Sink_Redis_Hash {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 4L, 30),
                new WaterSensor("sensor_1", 2L, 20),
                new WaterSensor("sensor_1", 5L, 40),
                new WaterSensor("sensor_2", 4L, 100),
                new WaterSensor("sensor_2", 5L, 200),
                new WaterSensor("中文", 5L, 200)
        );

        FlinkJedisPoolConfig jedisConf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop162")
                .setPort(6379)
                .setMaxTotal(100)
                .setMaxIdle(10)
                .setTimeout(10 * 1000)
                .setDatabase(0)
                .build();

        stream
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(new RedisSink<>(jedisConf, new RedisMapper<WaterSensor>() {

                    // 返回命令描述符

                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        // 第二个参数只对hash和zset有效, 其他数据结构忽略
                        return new RedisCommandDescription(RedisCommand.HSET, "传感器");
                    }

                    // 数据要写入到redis.
                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        return data.getId();  // sensor id作为字符串的key
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        return JSON.toJSONString(data);
                    }
                }));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
redis 数据结构:

string


list

set

hash
 key            field    value
 a               1        ....
                 2        ....

zset

 */