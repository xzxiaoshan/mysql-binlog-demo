package com.shanhy.demo.mysqlbinlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

/**
 * 但由于binlog服务下线，重新启动后，默认又开始在最新position处进行监听。会丢失一部门binlog的事件。
 * 所以每次事件均需要记录当前的position位置。重新建立client端时，使用记录的position位置初始化BinaryLogClient。
 * 单实例可以使用记录到本地文件的方式存储，如果需要考虑多实例分布式问题则需要考虑存储到redis等共享存储中。
 * 注意：因为事件属于高频操作，所以建议使用redis或者MQ这种写入速度较快的存储方式。
 *
 * @author 单红宇
 * @date 2024/5/23 14:37
 */
public class SaveBinlogFilenameAndPositionListener implements BinaryLogClient.EventListener {

    @Override
    public void onEvent(Event event) {
        EventHeader eventHeader = event.getHeader();
        EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            RotateEventData rotateEventData = (RotateEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
            this.saveBinlogFilenameAndNextPosition(rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
        }
    }

    /**
     * 保存文件名和下一个position
     *
     * @param binlogFilename binlogFilename
     * @param nextPosition   nextPosition
     */
    public void saveBinlogFilenameAndNextPosition(String binlogFilename, long nextPosition) {
        // no implementation
    }

    /**
     * 保存下一个position
     *
     * @param nextPosition nextPosition
     */
    public void saveNextBinlogPosition(long nextPosition) {
        // no implementation
    }

    /**
     * 返回 binlogFilename/nextPosition 这样的格式表示具体binlog文件和position位置的字符串
     *
     * @return String
     */
    public String getBinlogNextPositionString() {
        // 从持久化存储处读取binlogFileName和nextPosition，以 binlogFilename/nextPosition 这样的格式返回（示例：mysql-bin.000557/270069874）
        return null;
    }

}
