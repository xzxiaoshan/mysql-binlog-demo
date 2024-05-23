package com.shanhy.demo.mysqlbinlog;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 乐游监听器
 * SpringBoot启动成功后的执行业务线程操作
 * CommandLineRunner去实现此操作
 * 在有多个可被执行的业务时，通过使用 @Order 注解，设置各个线程的启动顺序（value值由小到大表示启动顺序）。
 * 多个实现CommandLineRunner接口的类必须要设置启动顺序，不让程序启动会报错！
 *
 * @author binlog
 **/
@Slf4j
@Component
@Order(value = 1)
public class BinLogRunner implements CommandLineRunner, DisposableBean {

    private final BinLogProperties binLogProperties;

    private MySQLBinLogHandler mysqlBinLogHandler;
    private final SaveBinlogFilenameAndPositionListener saveBinlogFilenameAndPositionListener = new SaveBinlogFilenameAndPositionListener();

    public BinLogRunner(BinLogProperties binLogProperties) {
        this.binLogProperties = binLogProperties;
    }

    // 该变量用来观察执行顺序和记录累计处理次数
    private final AtomicInteger num = new AtomicInteger(1);

    @Override
    public void run(String... args) throws Exception {

        // 初始化监听器
        this.mysqlBinLogHandler = new MySQLBinLogHandler(binLogProperties);

        // 获取table集合
        List<String> tableList = binLogProperties.getTableList();
        if (tableList == null || tableList.isEmpty()) {
            return;
        }
        // 注册监听
        tableList.forEach(table -> {
            log.info("注册监听, tableName={}.{}", binLogProperties.getDatabaseName(), table);
            try {
                // 这里将所有表使用同一个处理器处理，实际项目中可能不同的表使用不同的处理
                mysqlBinLogHandler.regListener(binLogProperties.getDatabaseName(), table, binLogItem -> {
                    // 这里对最终封装好的BinLogItem对象进行处理
                    // 如果你得数据并发量不是足够的大，在单线程能处理过来的情况下尽量不要选择多线程，
                    // 否则你要充分考虑好多线程的执行顺序以及服务宕机后position不准确而导致重启后数据丢失问题，
                    // 可以考虑将数据以Table为单元发送到MQ中处理，单个Table对应一个单线程处理程序防止执行顺序带来的数据问题。
                    log.info("监听逻辑处理");
                    // 代码略，这里进行监听逻辑的处理
                    // log.info("BinLogItem={}", binLogItem);
                    log.info("TableName={}", binLogItem.getDbTable());
                    log.info("EventType={}", binLogItem.getEventType().name());
                    log.info("{}. {} >>> {}", num.getAndIncrement(), binLogItem.getBefore().get("name"), binLogItem.getAfter().get("name"));

                    // 在所有逻辑都处理完成后，更新存储position为nextPosition
                    if(saveBinlogFilenameAndPositionListener != null)
                        saveBinlogFilenameAndPositionListener.saveNextBinlogPosition(binLogItem.getNextPosition());
                });
            } catch (Exception e) {
                log.error("BinLog监听异常：" + e);
            }
        });
        // 多线程消费
        mysqlBinLogHandler.parse();
    }

    @Override
    public void destroy() throws Exception {
        mysqlBinLogHandler.stop();
    }
}

