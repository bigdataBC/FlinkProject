package bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderDetail {
    // 编号
    Long id;
    // 订单编号
    Long order_id;
    Long sku_id;
    // 购买价格(下单时sku价格）
    BigDecimal order_price;
    // 购买个数
    Long sku_num;
    String sku_name;
    // 创建时间
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}