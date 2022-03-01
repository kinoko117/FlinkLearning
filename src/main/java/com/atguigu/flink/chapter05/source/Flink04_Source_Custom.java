package com.atguigu.flink.chapter05.source;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 13:58
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> stream = env.addSource(new SocketSource("hadoop162", 9999));
        stream.print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class SocketSource implements SourceFunction<WaterSensor> {
        
        private String host;
        private int port;
        private boolean isCancel;
    
        public SocketSource(String host, int port) {
            this.host = host;
            this.port = port;
        }
        
        // source的核心方法
        // 从socket读取数据
        /*
        public class WaterSensor {
            //pojo  : 字段 + setter+getter+构造器   用来封装数据
            
            private String id;
            private Long ts;
            private Integer vc;
}
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // 读取数据, 然后对数据进行封装, 把封装后的数据写入到流中
            Socket socket = new Socket(host, port);
            InputStream is = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    
            String line = reader.readLine();
            while (!isCancel && line != null) {
                //...
                String[] data = line.split(",");
                ctx.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));
        
                line = reader.readLine();
            }
    
        }
        
        // 外界可以通过调用这个方法实现停止source
        @Override
        public void cancel() {
            //System.exit(0);
            isCancel = true;
        }
    }
}
