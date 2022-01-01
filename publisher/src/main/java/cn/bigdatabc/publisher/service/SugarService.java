package cn.bigdatabc.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author liufengting
 * @date 2022/1/1
 */
public interface SugarService {
    BigDecimal getGmv(int date);

    Map getGmvByTm(int date, int limit);
}
