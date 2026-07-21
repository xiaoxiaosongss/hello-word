package com.demo;

public final class RocketMqConfig {
    public static final String NAMESRV_ADDR = System.getenv().getOrDefault("NAMESRV_ADDR", "127.0.0.1:9876");
    public static final String TOPIC_BASIC = "demo_basic_topic";
    public static final String TOPIC_ORDERED = "demo_ordered_topic";
    public static final String TOPIC_DELAY = "demo_delay_topic";
    public static final String TOPIC_TRANSACTION = "demo_transaction_topic";
    public static final String TOPIC_FILTER = "demo_filter_topic";

    private RocketMqConfig() {
    }
}
