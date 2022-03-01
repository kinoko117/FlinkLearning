import org.apache.flink.util.MathUtils;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 15:44
 */
public class Test1 {
    public static void main(String[] args) {
        System.out.println(MathUtils.murmurHash("奇数".hashCode()) % 128);
        System.out.println(MathUtils.murmurHash("偶数".hashCode()) % 128);
        
        System.out.println(MathUtils.murmurHash(Integer.valueOf(0).hashCode()) % 128);
        System.out.println(MathUtils.murmurHash(Integer.valueOf(1).hashCode()) % 128);
        
    }
}
