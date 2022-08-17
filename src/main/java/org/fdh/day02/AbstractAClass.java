package org.fdh.day02;

/**
 * 1、抽象类可以没有抽象方法，但是有抽象方法则必须是抽象类。
 * 2、抽象类实现接口的好处，可以不用实现接口的所有方法，普通类实现接口必须实现所有的方法
 *
 */
public abstract class AbstractAClass implements Ainterface {
    @Override
    public void aa1() {
        System.out.println("aa");
    }
}