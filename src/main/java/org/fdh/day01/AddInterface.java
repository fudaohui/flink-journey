package org.fdh.day01;


/**
 * 函数式借口
 *
 * @param <T>
 */
@FunctionalInterface
public interface AddInterface<T> {

    T add(T a, T b);
}
