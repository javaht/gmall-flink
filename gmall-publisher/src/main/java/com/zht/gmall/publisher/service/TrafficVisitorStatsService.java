package com.zht.gmall.publisher.service;

import com.zht.gmall.publisher.bean.TrafficVisitorStatsPerHour;
import com.zht.gmall.publisher.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TrafficVisitorStatsService {
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);

    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}
