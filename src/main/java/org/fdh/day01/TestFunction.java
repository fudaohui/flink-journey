package org.fdh.day01;

public class TestFunction {

    public static void main(String[] args) {
        AddInterface<Integer> addImpl = (Integer a, Integer b) -> {
            return a + b;
        };
        Integer add = addImpl.add(1, 2);
        System.out.println(add);
    }
}
