package com.zht.gmall.publisher.service;

import com.zht.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.zht.gmall.publisher.bean.TradeProvinceOrderCt;
import com.zht.gmall.publisher.bean.TradeStats;

import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}
