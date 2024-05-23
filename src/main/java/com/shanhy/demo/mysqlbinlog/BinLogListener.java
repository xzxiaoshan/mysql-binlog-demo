package com.shanhy.demo.mysqlbinlog;

/**
 * BinLogListener监听器
 *
 * @author binlog
 */
@FunctionalInterface
public interface BinLogListener {

    /**
     * onEvent
     *
     * @param binLogItem binLogItem
     */
    void onEvent(BinLogItem binLogItem);
}

