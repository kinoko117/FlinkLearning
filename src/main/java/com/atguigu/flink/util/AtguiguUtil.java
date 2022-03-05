package com.atguigu.flink.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/5 10:17
 */
public class AtguiguUtil {
    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }
        return list;
    }
}
