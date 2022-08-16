package org.fdh.day01;

import scala.Int;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestFunction {

    public static void main(String[] args) {
        /**
         * 方式1
         */
//        Integer add1 = new AddInterface<Integer>() {
//            @Override
//            public Integer add(Integer a, Integer b) {
//                return a + b;
//            }
//        }.add(1, 2);
//        System.out.println(add1);
//        //方式2、实现接口
//
        //实现3，函数式接口，拉姆达表达式方式
        AddInterface<Integer> addImpl = (Integer a, Integer b) -> {
            return a + b;
        };
        Integer add = addImpl.add(1, 2);
        System.out.println(add);

        //总结，无论哪种实现方式都需要实现改方法的内部逻辑

//        Java Stream API是面向集合的操作，比如循环一个列表。我们可以理解为在单个JVM之上进行的各类数据操作。
//        Spark和Flink的API是面向数据集或数据流的操作。这些操作分布在大数据集群的多个节点上，并行地分布式执行。
        List<String> strings = Arrays.asList("abc", "", "bc", "12345", "efg", "abcd", "", "jkl");
        List<Integer> lengths = strings.stream().filter(string -> !string.isEmpty()).map(s -> s.length()).collect(Collectors.toList());
        lengths.forEach((s) -> System.out.println(s));
    }
}
