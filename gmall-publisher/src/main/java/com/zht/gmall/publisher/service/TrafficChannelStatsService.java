package com.zht.gmall.publisher.service;

import com.zht.gmall.publisher.bean.*;

import java.util.List;

public interface TrafficChannelStatsService {
    List<TrafficUvCt> getUvCt(Integer date);

    List<TrafficSvCt> getSvCt(Integer date);

    List<TrafficPvPerSession> getPvPerSession(Integer date);

    List<TrafficDurPerSession> getDurPerSession(Integer date);

    List<TrafficUjRate> getUjRate(Integer date);
}
