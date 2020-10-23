package com.xueersi.kafkaserviceapi.Enum;

/**
 * @author wudi
 * @version 1.0 2020/08/31
 */
public enum ReturnCodeEnum {
    /**
     * 发送成功
     */
    SEND_SUCCESS(0, "send success"),
    /**
     * 批量发送成功
     */
    BATCH_SEND_SUCCESS(0, "batch send success"),
    /**
     * 业务暂停成功
     */
    BIZ_STOP_SUCCESS(0, "biz stop success"),
    /**
     * 业务恢复成功
     */
    BIZ_RESUME_SUCCESS(0, "biz resume success"),
    /**
     * 查看业务状态成功
     */
    PEEK_BIZ_STATUS_SUCCESS(0, "view biz status success"),
    /**
     * 清空本地数据库
     */
    CLEAR_DB_SUCCESS(0, "clear db success"),

    /**
     * topic不存在
     */
    TOPIC_NOT_EXIST(10005, "topic not exist"),
    /**
     * 发送失败
     */
    SEND_FAILED(10006, "send failed"),
    /**
     * 批量发送失败
     */
    BATCH_SEND_FAILED(10007, "send failed msg list"),
    /**
     * message 延时级别有误
     */
    DELAY_LEVEL_ERROR(10011, "delay level error");


    private Integer code;
    private String returnMsg;

    ReturnCodeEnum(int code, String returnMsg) {
        this.code = code;
        this.returnMsg = returnMsg;
    }

    public int getCode() {
        return code;
    }

    public String getReturnMsg() {
        return returnMsg;
    }
}
