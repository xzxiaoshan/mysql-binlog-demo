package com.shanhy.demo.mysqlbinlog;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * 字段属性对象
 *
 * @author binlog
 */
@Data
public class TableColumn implements Serializable {

    /**
     * serialVersionUID
     */
    @Serial
    private static final long serialVersionUID = 1274951031683996339L;

    /**
     * ordinalPosition
     */
    private int ordinalPosition;
    /**
     * columnName
     */
    private String columnName; // 列名
    /**
     * dataType
     */
    private String dataType; // 类型
    /**
     * schema
     */
    private String schema; // 数据库
    /**
     * tableName
     */
    private String tableName; // 表

    /**
     * TableColumn
     *
     * @param schema          schema
     * @param tableName       tableName
     * @param ordinalPosition ordinalPosition
     * @param columnName      columnName
     * @param dataType        dataType
     */
    public TableColumn(String schema, String tableName, int ordinalPosition, String columnName, String dataType) {
        this.schema = schema;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.ordinalPosition = ordinalPosition;
    }

}

