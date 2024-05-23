package com.shanhy.demo.mysqlbinlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import lombok.Data;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.github.shyiko.mysql.binlog.event.EventType.isDelete;
import static com.github.shyiko.mysql.binlog.event.EventType.isWrite;

/**
 * binlog对象
 *
 * @author binlog
 * @date 2024-05-22 10:55:23
 */
@Data
@ToString
public class BinLogItem implements Serializable {

    /**
     * serialVersionUID
     */
    @Serial
    private static final long serialVersionUID = 5503152746318421290L;

    /**
     * dbTable
     */
    private String dbTable;
    /**
     * eventType
     */
    private EventType eventType;
    /**
     * timestamp
     */
    private Long timestamp = null;
    /**
     * serverId
     */
    private Long serverId = null;
    /**
     * position
     */
    private Long position = null;
    /**
     * nextPosition
     */
    private Long nextPosition = null;
    /**
     * 存储字段-之前的值之后的值
     */
    private Map<String, Serializable> before;
    /**
     * after
     */
    private Map<String, Serializable> after;
    /**
     * 存储字段--类型
     */
    private Map<String, TableColumn> columns;

    /**
     * BinLogItem
     *
     * @param dbTableName dbTableName
     * @param eventType   eventType
     * @param columnMap   columMap
     */
    public BinLogItem(String dbTableName, Event event, EventType eventType, Map<String, TableColumn> columnMap) {
        this.eventType = eventType;
        this.columns = columnMap;
        this.dbTable = dbTableName;
        this.before = new HashMap<>();
        this.after = new HashMap<>();
        if (event.getHeader() instanceof EventHeaderV4) {
            EventHeaderV4 eventHeader = event.getHeader();
            this.serverId = eventHeader.getServerId();
            this.timestamp = eventHeader.getTimestamp();
            this.position = eventHeader.getPosition();
            this.nextPosition = eventHeader.getNextPosition();
        }
    }

    /**
     * 新增或者删除操作数据格式化
     *
     * @param dbTableName dbTableName
     * @param row         row
     * @param columnMap   columnMap
     * @param event       event
     * @param eventType   eventType
     * @return BinLogItem
     */
    public static BinLogItem itemFromInsertOrDeleted(String dbTableName, Serializable[] row, Map<String, TableColumn> columnMap,
                                                     Event event, EventType eventType) {
        if (null == row || null == columnMap) {
            return null;
        }
        if (row.length != columnMap.size()) {
            return null;
        }
        // 初始化Item
        BinLogItem item = new BinLogItem(dbTableName, event, eventType, columnMap);

        Map<String, Serializable> beOrAf = new HashMap<>();

        columnMap.forEach((key, tableColumn) -> beOrAf.put(key, row[tableColumn.getOrdinalPosition() - 1]));

        // 写操作放after，删操作放before
        if (isWrite(eventType)) {
            item.after = beOrAf;
        }
        if (isDelete(eventType)) {
            item.before = beOrAf;
        }
        return item;
    }

    /**
     * 更新操作数据格式化
     *
     * @param dbTableName dbTableName
     * @param mapEntry    mapEntry
     * @param columnMap   columnMap
     * @param event       event
     * @param eventType   eventType
     * @return BinLogItem
     */
    public static BinLogItem itemFromUpdate(String dbTableName, Map.Entry<Serializable[], Serializable[]> mapEntry,
                                            Map<String, TableColumn> columnMap,
                                            Event event,
                                            EventType eventType) {
        if (null == mapEntry || null == columnMap) {
            return null;
        }
        // 初始化Item
        BinLogItem item = new BinLogItem(dbTableName, event, eventType, columnMap);

        Map<String, Serializable> be = new HashMap<>();
        Map<String, Serializable> af = new HashMap<>();

        columnMap.forEach((key, tableColumn) -> {
            be.put(key, mapEntry.getKey()[tableColumn.getOrdinalPosition() - 1]);
            af.put(key, mapEntry.getValue()[tableColumn.getOrdinalPosition() - 1]);
        });

        item.before = be;
        item.after = af;
        return item;
    }

}

