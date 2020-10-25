//package com.xueersi.kafkaproducerservice;
//
//import com.google.common.util.concurrent.AtomicDouble;
//import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
//import com.googlecode.concurrentlinkedhashmap.EvictionListener;
//import com.googlecode.concurrentlinkedhashmap.Weighers;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.locks.ReentrantLock;
//
//public class Test {
//    public static void main(String[] args) {
//        //test001();
//        test002();
//        ReentrantLock lock = null;
//        AtomicDouble atomicDouble =new AtomicDouble();
//    }
//
//    private static void test001() {
//        EvictionListener<String, String> listener = new EvictionListener<String, String>() {
//            @Override
//            public void onEviction(String key, String value) {
//                System.out.println("Evicted key=" + key + ", value=" + value);
//            }
//        };
//        ConcurrentMap<String, String> cache = new ConcurrentLinkedHashMap.Builder<String, String>().maximumWeightedCapacity(10).listener(listener).build();
//        for (int i = 0; i < 150; i++) {
//            int j = 0;
//            j = j + i;
//            cache.put(String.valueOf(j), "nihao" + i);
//        }
//        for (Map.Entry<String, String> entry : cache.entrySet()) {
//            String key = entry.getKey();
//            String value = entry.getValue();
//            System.out.println(key + "====" + value);
//        }
//        System.out.println("1," + cache.get("1"));
//        cache.remove("141");
//        System.out.println("141," + cache.get("141"));
//    }
//
//    /**
//     * ConcurrentLinkedHashMap 是google团队提供的一个容器。它有什么用呢？其实它本身是对
//     * ConcurrentHashMap的封装，可以用来实现一个基于LRU策略的缓存。详细介绍可以参见
//     * http://code.google.com/p/concurrentlinkedhashmap
//     */
//    private static void test002() {
//        ConcurrentLinkedHashMap<Integer, Integer> map = new ConcurrentLinkedHashMap.Builder<Integer, Integer>()
//                .maximumWeightedCapacity(2).weigher(Weighers.singleton())
//                .build();
//        map.put(1, 1);
//        map.put(2, 2);
//        map.put(3, 3);
//        System.out.println(map.get(1));// null 已经失效了
//        System.out.println(map.get(2));
//    }
//
//
//}