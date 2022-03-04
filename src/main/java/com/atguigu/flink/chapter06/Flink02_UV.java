package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author lzc
 * @Date 2022/3/4 8:56
 */
public class Flink02_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4])
                    );
                })
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    HashSet<Long> userIds = new HashSet<>();

                    @Override
                    public void processElement(UserBehavior ub,
                                               Context ctx,
                                               Collector<Long> out) throws Exception {
                        /*if ("pv".equals(ctx.getCurrentKey())) {
                            int pre = userIds.size();
                            userIds.add(ub.getUserId());
                            int post = userIds.size();

                            // 当uv增长的时候才需要向外输出
                            if (post > pre) {

                                out.collect((long) userIds.size());
                            }
                        }*/

                        if ("pv".equals(ctx.getCurrentKey())) {

                            // 返回值是true表示这次是新增, 否则就是一个旧元素
                            if (userIds.add(ub.getUserId())) {

                                out.collect((long) userIds.size());
                            }


                        }
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
