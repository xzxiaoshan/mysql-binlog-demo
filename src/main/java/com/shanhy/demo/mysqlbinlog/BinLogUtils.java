package com.shanhy.demo.mysqlbinlog;

import com.github.shyiko.mysql.binlog.event.EventType;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.shyiko.mysql.binlog.event.EventType.isDelete;
import static com.github.shyiko.mysql.binlog.event.EventType.isUpdate;
import static com.github.shyiko.mysql.binlog.event.EventType.isWrite;

/**
 * 监听工具
 *
 * @author binlog
 */
@Slf4j
public class BinLogUtils {

    /**
     * BinLogUtils
     */
    private BinLogUtils() {
        // no implementation
    }

    /**
     * 拼接dbTable
     *
     * @param db    db
     * @param table table
     * @return String
     */
    public static String getDbTable(String db, String table) {
        return db + "." + table;
    }

    /**
     * 获取columns集合
     *
     * @param binLogProperties binLogProperties
     * @return Map
     */
    public static Map<String, Map<String, TableColumn>> getTableColumns(BinLogProperties binLogProperties) {
        String dbName = binLogProperties.getDatabaseName();
        List<String> tbNameList = binLogProperties.getTableList();
        if (tbNameList == null || tbNameList.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Class.forName(binLogProperties.getDriverClassName());
            // 保存当前注册的表的colum信息
            Connection connection = DriverManager.getConnection("jdbc:mysql://" + binLogProperties.getHost() + ":" + binLogProperties.getPort() + "/INFORMATION_SCHEMA",
                    binLogProperties.getUsername(), binLogProperties.getPassword());
            StringBuilder tbParamStr = new StringBuilder("(");
            tbNameList.forEach(tbParam -> tbParamStr.append("?,"));
            tbParamStr.replace(tbParamStr.length() - 1, tbParamStr.length(), ")");
            // 执行sql
            String preSql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? and TABLE_NAME in " + tbParamStr;
            PreparedStatement ps = connection.prepareStatement(preSql);
            ps.setString(1, dbName);
            for (int i = 0; i < tbNameList.size(); i++) {
                ps.setString(i + 2, tbNameList.get(i));
            }
            ResultSet rs = ps.executeQuery();
            Map<String, Map<String, TableColumn>> resultMap = new ConcurrentHashMap<>();
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                int ordinalPosition = rs.getInt("ORDINAL_POSITION");
                String dataType = rs.getString("DATA_TYPE");
                if (columnName != null && ordinalPosition >= 1) { // 位置从1开始
                    Map<String, TableColumn> tableColumnMap = resultMap.computeIfAbsent(BinLogUtils.getDbTable(schema, tableName),
                            key -> new HashMap<>());
                    tableColumnMap.put(columnName, new TableColumn(schema, tableName, ordinalPosition, columnName, dataType));
                }
            }
            ps.close();
            rs.close();
            return resultMap;
        } catch (SQLException | ClassNotFoundException e) {
            log.error("load db conf error, schema={}, tables={} ", dbName, String.join(",", tbNameList.toArray(new String[0])), e);
        }
        return Collections.emptyMap();
    }

    /**
     * 根据DBTable获取table
     *
     * @param dbTable dbTable
     * @return java.lang.String
     */
    public static String getTable(String dbTable) {
        if (dbTable == null || dbTable.isEmpty()) {
            return "";
        }
        String[] split = dbTable.split("\\.");
        if (split.length == 2) {
            return split[1];
        }
        return "";
    }

    /**
     * 根据操作类型获取对应集合
     *
     * @param binLogItem binLogItem
     * @return Map<String, Serializable>
     */
    public static Map<String, Serializable> getOptMap(BinLogItem binLogItem) {
        // 获取操作类型
        EventType eventType = binLogItem.getEventType();
        if (isWrite(eventType) || isUpdate(eventType)) {
            return binLogItem.getAfter();
        }
        if (isDelete(eventType)) {
            return binLogItem.getBefore();
        }
        return Collections.emptyMap();
    }

    /**
     * 获取操作类型
     *
     * @param binLogItem binLogItem
     * @return OptType
     */
    public static Integer getOptType(BinLogItem binLogItem) {
        // 获取操作类型
        EventType eventType = binLogItem.getEventType();
        if (isWrite(eventType)) {
            return 1;
        }
        if (isUpdate(eventType)) {
            return 2;
        }
        if (isDelete(eventType)) {
            return 3;
        }
        return null;
    }

}

