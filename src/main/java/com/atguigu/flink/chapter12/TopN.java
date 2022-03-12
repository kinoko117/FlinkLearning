package com.atguigu.flink.chapter12;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/3/12 15:24
 */
public class TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        /*
            private Long userId;
            private Long itemId;
            private Integer categoryId;
            private String behavior;
            private Long timestamp;
         */
        // 1. 读取数据 从文件中
        tEnv.executeSql("create table ub(" +
                            " userId bigint, " +
                            " itemId bigint, " +
                            " categoryId int, " +
                            " behavior string, " +
                            " ts bigint , " +
                            " et as to_timestamp_ltz(ts, 0), " +
                            " watermark for et as et - interval '3' second " +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/UserBehavior.csv', " +
                            " 'format' = 'csv' " +
                            ")");
        
        
        // 2. 过滤  开窗聚合 统计每个商品的点击量
        /*tEnv
            .sqlQuery("select" +
                          " itemId, " +
                          " hop_start(et, interval '1' hour, interval '2' hour) stt, " +
                          " hop_end(et, interval '1' hour, interval '2' hour) edt, " +
                          " count(*) ct " +
                          "from ub " +
                          "where behavior='pv' " +
                          "group by itemId, hop(et, interval '1' hour, interval '2' hour)")
            .execute()
            .print();*/
        
        Table t1 = tEnv.sqlQuery("select" +
                                     " itemId, " +
                                     " window_start stt, " +
                                     " window_end edt, " +
                                     " count(*) ct " +
                                     "from  table( " +
                                     " hop( table ub, descriptor(et), interval '1' hour, interval '2' hour) " +
                                     ")" +
                                     "where behavior='pv' " +
                                     "group by window_start, window_end, itemId");
        tEnv.createTemporaryView("t1", t1);
        
        // 3. 使用 over 窗口, 对同一个窗口内的元素安装点击量降序排列, 分配名次
        // rank  row_number dense_rank flink 目前只支持 row_number
        Table t2 = tEnv.sqlQuery("select " +
                                        " stt, edt, itemId, ct, " +
                                        " row_number() over(partition by edt order by ct desc) rn " +
                                        "from t1");
        tEnv.createTemporaryView("t2", t2);
        
        // 4. 过滤出来名次小于等于3的
        Table t3 = tEnv.sqlQuery("select " +
                                        " edt w_end, " +
                                        " itemId item_id, " +
                                        " ct item_count, " +
                                        " rn rk " +
                                        " from t2 where rn<=3");
    
    
        // 5. 写入到Mysql中
        // 5.1 在flink建动态表与mysql中的表关联
        tEnv.executeSql("CREATE TABLE `hot_item` (" +
                            "  `w_end` timestamp," +
                            "  `item_id` bigint," +
                            "  `item_count` bigint," +
                            "  `rk` bigint," +
                            "  PRIMARY KEY (`w_end`,`rk`)not enforced" +
                            ")with(" +
                            "   'connector' = 'jdbc', " +
                            "   'url' = 'jdbc:mysql://hadoop162:3306/flink_sql'," +
                            "   'table-name' = 'hot_item', " +
                            "   'username' = 'root', " +
                            "   'password' = 'aaaaaa'" +
                            ")");
        
        // 5.2 把结果写入到动态表, 则自动会写入到mysql
        t3.executeInsert("hot_item");
        
        
    }
}
