package bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderInfo {
    // 编号
    Long id;
    // 地区
    Long province_id;
    // 订单状态
    String order_status;
    // 用户id
    Long user_id;
    // 总金额
    BigDecimal total_amount;
    // 促销金额
    BigDecimal activity_reduce_amount;
    // 优惠券
    BigDecimal coupon_reduce_amount;
    // 原价金额
    BigDecimal original_total_amount;
    // 运费
    BigDecimal feight_fee;
    // 失效时间
    String expire_time;
    // 创建时间
    String create_time;  //yyyy-MM-dd HH:mm:ss
    // 操作时间
    String operate_time;
    // 把字段create_time处理得到下面三个
    String create_date;
    String create_hour;
    Long create_ts;
}