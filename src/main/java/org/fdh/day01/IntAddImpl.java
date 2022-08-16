package org.fdh.day01;

public class IntAddImpl implements AddInterface<Integer>{
    @Override
    public Integer add(Integer a, Integer b) {
        return a+b;
    }
}
