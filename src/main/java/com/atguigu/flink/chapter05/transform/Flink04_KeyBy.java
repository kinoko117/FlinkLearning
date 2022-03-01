package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink04_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
        stream
            /*    .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                })
                .print();*/
            .keyBy(new KeySelector<Integer, Integer>() {
                @Override
                public Integer getKey(Integer value) throws Exception {
                    return value % 2;
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
new KeyGroupStreamPartitioner<>(
                                keySelector,
                                StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM))  // 128



KeyGroupRangeAssignment
        .assignKeyToParallelOperator(
                // 奇数/偶数, 128, 2
                key, maxParallelism, numberOfChannels);



computeOperatorIndexForKeyGroup(
                // 128, 2, [0,127]
                maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));

                        // key, 128
                        assignToKeyGroup(key, maxParallelism)

                            // hashCode,  128
                            computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
                                MathUtils.murmurHash(keyHash) % maxParallelism;  // [0,127]
                                // 双重hash

        [0,127] * 2 / 128    = [0,254]/128     0, 1
return keyGroupId * parallelism / maxParallelism;

>=64  1
<64   0


 */