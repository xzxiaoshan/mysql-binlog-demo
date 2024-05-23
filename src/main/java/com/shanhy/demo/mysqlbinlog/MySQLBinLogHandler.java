package com.shanhy.demo.mysqlbinlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.shyiko.mysql.binlog.event.EventType.isDelete;
import static com.github.shyiko.mysql.binlog.event.EventType.isUpdate;
import static com.github.shyiko.mysql.binlog.event.EventType.isWrite;

/**
 * Binlog处理器
 *
 * @author binlog
 * @date 2024-05-22 09:44:58
 */
@Slf4j
public class MySQLBinLogHandler implements BinaryLogClient.EventListener {

    /**
     * parseClient
     */
    private final BinaryLogClient parseClient;

    /**
     * saveBinlogFilenameAndPositionListener
     */
    private final SaveBinlogFilenameAndPositionListener saveBinlogFilenameAndPositionListener;

    /**
     * binLogProperties
     */
    private final BinLogProperties binLogProperties;

    /**
     * 存放每张数据表对应的listener
     */
    private final MultiValueMap<String, BinLogListener> listeners;

    /**
     * listenerTableColumnMap
     */
    private Map<String, Map<String, TableColumn>> listenerTableColumnMap;

    /**
     * TABLE_MAP 事件：当某个表的行事件（如 WRITE_ROWS, UPDATE_ROWS, DELETE_ROWS）被记录到 binlog 中时，
     * MySQL 会首先生成一个 TABLE_MAP 事件。这些事件描述了 tableId 和对应表的元数据（如表名、数据库名、列类型等）。
     * 因为 TABLE_MAP 事件和后续的 增删改事件是两次不同的事件，当我们需要再后面事件中获取到对应的表信息时，需要在先收到的 TABLE_MAP 事件中提前将表的Id和表信息关联起来
     */
    private final Map<Long, String> tableIdMapping;

    /**
     * 监听器初始化
     *
     * @param binLogProperties binLogProperties
     */
    public MySQLBinLogHandler(BinLogProperties binLogProperties, SaveBinlogFilenameAndPositionListener saveBinlogFilenameAndPositionListener) {
        this.binLogProperties = binLogProperties;
        this.saveBinlogFilenameAndPositionListener = saveBinlogFilenameAndPositionListener;
        this.parseClient = getBinaryLogClient(binLogProperties);
        this.listeners = new LinkedMultiValueMap<>();
        this.tableIdMapping = new ConcurrentHashMap<>();
        this.initBinlogFilenameAndPosition();
        this.initTableColumns();
        log.info("初始化配置完成：{}", binLogProperties);
    }

    /**
     * initBinlogFilenameAndPosition
     */
    private void initBinlogFilenameAndPosition() {
        if (this.saveBinlogFilenameAndPositionListener != null && this.saveBinlogFilenameAndPositionListener.getBinlogNextPositionString() != null) {
            String[] arr = this.saveBinlogFilenameAndPositionListener.getBinlogNextPositionString().split("/");
            this.parseClient.setBinlogFilename(arr[0]);
            this.parseClient.setBinlogPosition(Long.parseLong(arr[1]));
        }
    }

    /**
     * MySQLBinLogListener
     *
     * @param binLogProperties binLogProperties
     */
    public MySQLBinLogHandler(BinLogProperties binLogProperties) {
        this(binLogProperties, null);
    }

    /**
     * 初始化TableColumns
     */
    public void initTableColumns() {
        this.listenerTableColumnMap = BinLogUtils.getTableColumns(binLogProperties);
    }

    /**
     * getBinaryLogClient
     *
     * @param binLogProperties binLogProperties
     * @return BinaryLogClient
     */
    private BinaryLogClient getBinaryLogClient(BinLogProperties binLogProperties) {
        BinaryLogClient client = new BinaryLogClient(binLogProperties.getHost(), binLogProperties.getPort(),
                binLogProperties.getUsername(), binLogProperties.getPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();
        //序列化设置，按需配置
        eventDeserializer.setCompatibilityMode(
//                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );
        client.setEventDeserializer(eventDeserializer);
        return client;
    }

    /**
     * 监听事件处理
     */
    @Override
    public void onEvent(Event event) {
        EventType eventType = event.getHeader().getEventType();
        if (eventType == EventType.TABLE_MAP) {
            TableMapEventData tableData = event.getData();
            String db = tableData.getDatabase();
            String table = tableData.getTable();
            String dbTable = BinLogUtils.getDbTable(db, table);
            if (listenerTableColumnMap.containsKey(dbTable)) {
                tableIdMapping.put(tableData.getTableId(), dbTable);
            }
        } else if (isUpdate(eventType)) {
            onUpdateEvent(event, eventType);
        } else if (isWrite(eventType) || isDelete(eventType)) {
            onInsertOrDeleteEvent(event, eventType);
        }
    }

    /**
     * 新增或者删除事件处理
     *
     * @param event     event
     * @param eventType eventType
     */
    private void onInsertOrDeleteEvent(Event event, EventType eventType) {
        List<Serializable[]> rows;
        long tableId;
        if (isWrite(eventType)) {
            WriteRowsEventData data = event.getData();
            rows = data.getRows();
            tableId = data.getTableId();
        } else {
            DeleteRowsEventData data = event.getData();
            tableId = data.getTableId();
            rows = data.getRows();
        }
        String dbTableName = tableIdMapping.get(tableId);
        if (dbTableName != null) {
            for (Serializable[] row : rows) {
                this.processBinLogItem(BinLogItem.itemFromInsertOrDeleted(dbTableName, row,
                        listenerTableColumnMap.get(dbTableName), event, eventType));
            }
        }
    }

    /**
     * 修改数据事件处理
     *
     * @param event     event
     * @param eventType eventType
     */
    private void onUpdateEvent(Event event, EventType eventType) {
        UpdateRowsEventData data = event.getData();
        String dbTableName = tableIdMapping.get(data.getTableId());
        if (dbTableName != null) {
            for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                this.processBinLogItem(BinLogItem.itemFromUpdate(dbTableName, row,
                        listenerTableColumnMap.get(dbTableName), event, eventType));
            }
        }
    }

    /**
     * 处理封装后的BinLogItem数据
     *
     * @param binLogItem binLogItem
     */
    public void processBinLogItem(BinLogItem binLogItem) {
        String dbTable = binLogItem.getDbTable();
        listeners.get(dbTable).forEach(binLogListener -> binLogListener.onEvent(binLogItem));
    }

    /**
     * 注册监听
     *
     * @param db       数据库
     * @param table    操作表
     * @param listener 监听器
     */
    public void regListener(String db, String table, BinLogListener listener) {
        // 保存当前注册的listener
        listeners.add(BinLogUtils.getDbTable(db, table), listener);
    }

    /**
     * 理论上单线程独立处理，为每一个表独立一个线程处理
     *
     * @throws IOException IOException
     */
    public void parse() throws IOException {
        parseClient.registerEventListener(this);
        if(this.saveBinlogFilenameAndPositionListener != null) {
            parseClient.registerEventListener(this.saveBinlogFilenameAndPositionListener);
        }
        parseClient.connect();
    }

    /**
     * 停止服务
     */
    public void stop() throws IOException {
        if (parseClient != null && parseClient.isConnected()) {
            parseClient.disconnect();
        }
    }

}
