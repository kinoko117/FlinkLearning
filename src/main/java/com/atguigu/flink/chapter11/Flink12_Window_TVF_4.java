package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink12_Window_TVF_4 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 先创建一个流
        DataStream<Person> stream = env
                .fromElements(
                    new Person("a1", "b1", "c1", "d1", 1000L, 10),
                    new Person("a2", "b2", "c2", "d2", 2000L, 20),
                    new Person("a3", "b3", "c3", "d3", 3000L, 30),
                    new Person("a4", "b4", "c4", "d4", 4000L, 40),
                    new Person("a5", "b5", "c5", "d5", 5000L, 50),
                    new Person("a6", "b6", "c6", "d6", 6000L, 60),
                    new Person("a7", "b7", "c7", "d7", 7000L, 70),
                    new Person("a8", "b8", "c8", "d8", 8000L, 80)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Person>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws, ts) -> ws.getTs())

                );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table table = tEnv.fromDataStream(stream, $("a"), $("b"), $("c"), $("d"), $("ts").rowtime(), $("score"));
        tEnv.createTemporaryView("person", table);


        /*tEnv
                .sqlQuery("select" +
                        " a,b,c,d, window_start, window_end, " +
                        " sum(score) vc_sum " +
                        "from table( tumble(table person, descriptor(ts), interval '5' second) )" +
                        "group by window_start, window_end, grouping sets( " +
                        " (a,b,c,d), (a,b,c), (a,b), (a), ())" +
                        "")
                .execute()
                .print();*/

        /*tEnv
                .sqlQuery("select" +
                        " a,b,c,d, window_start, window_end, " +
                        " sum(score) vc_sum " +
                        "from table( tumble(table person, descriptor(ts), interval '5' second) )" +
                        "group by window_start, window_end, rollup(a,b,c,d)")
                .execute()
                .print();*/

                tEnv
                .sqlQuery("select" +
                        " a,b,c,d, window_start, window_end, " +
                        " sum(score) vc_sum " +
                        "from table( tumble(table person, descriptor(ts), interval '5' second) )" +
                        "group by window_start, window_end, cube(a,b,c,d)")
                .execute()
                .print();  // 1 + 4 + 6 + 4 + 1  (a,b,c,d) (a,b,c)(a,b,d)(a,c,d)


    }
}
/*
分组集  group sets

------
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------+
| op |                              a |                              b |                              c |                              d |            window_start |              window_end |      vc_sum |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------+
| +I |                             a1 |                             b1 |                             c1 |                             d1 | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          10 |
| +I |                             a1 |                             b1 |                             c1 |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          10 |
| +I |                             a1 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          10 |
| +I |                             a2 |                             b2 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          20 |
| +I |                             a4 |                             b4 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          40 |
| +I |                             a4 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          40 |
| +I |                             a2 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          20 |
| +I |                         (NULL) |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |         100 |
| +I |                             a3 |                             b3 |                             c3 |                             d3 | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          30 |
| +I |                             a3 |                             b3 |                             c3 |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          30 |
| +I |                             a1 |                             b1 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          10 |
| +I |                             a2 |                             b2 |                             c2 |                             d2 | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          20 |
| +I |                             a3 |                             b3 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          30 |
| +I |                             a3 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          30 |
| +I |                             a2 |                             b2 |                             c2 |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          20 |
| +I |                             a4 |                             b4 |                             c4 |                             d4 | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          40 |
| +I |                             a4 |                             b4 |                             c4 |                         (NULL) | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |          40 |
| +I |                             a5 |                             b5 |                             c5 |                             d5 | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          50 |
| +I |                             a8 |                             b8 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          80 |
| +I |                             a8 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          80 |
| +I |                             a5 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          50 |
| +I |                         (NULL) |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |         260 |
| +I |                             a6 |                             b6 |                             c6 |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          60 |
| +I |                             a6 |                             b6 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          60 |
| +I |                             a7 |                             b7 |                             c7 |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          70 |
| +I |                             a7 |                             b7 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          70 |
| +I |                             a8 |                             b8 |                             c8 |                             d8 | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          80 |
| +I |                             a8 |                             b8 |                             c8 |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          80 |
| +I |                             a5 |                             b5 |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          50 |
| +I |                             a6 |                             b6 |                             c6 |                             d6 | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          60 |
| +I |                             a7 |                             b7 |                             c7 |                             d7 | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          70 |
| +I |                             a7 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          70 |
| +I |                             a5 |                             b5 |                             c5 |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          50 |
| +I |                             a6 |                         (NULL) |                         (NULL) |                         (NULL) | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |          60 |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+-------------------------+-------------------------+-------------+
34 rows in set

Process finished with exit code 0




 */