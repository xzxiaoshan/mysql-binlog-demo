package com.shanhy.demo.mysqlbinlog;

import lombok.Data;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * BinLog 配置类
 *
 * @author 单红宇
 * @date 2024/5/20 17:17
 */
@Data
@Component
@ConfigurationProperties(prefix = "binlog.datasource")
@ToString(exclude = "password")
public class BinLogProperties {

    /**
     * host
     */
    private String host;

    /**
     * port
     */
    private int port;

    /**
     * username
     */
    private String username;

    /**
     * password
     */
    private String password;

    /**
     * databaseName
     */
    private String databaseName;

    /**
     * driverClassName
     */
    private String driverClassName;

    /**
     * tableList
     */
    private List<String> tableList;

}
