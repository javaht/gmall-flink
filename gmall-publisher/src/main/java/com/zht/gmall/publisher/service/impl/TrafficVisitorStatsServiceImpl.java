package com.zht.gmall.publisher.service.impl;

import com.zht.gmall.publisher.bean.TrafficVisitorStatsPerHour;
import com.zht.gmall.publisher.bean.TrafficVisitorTypeStats;
import com.zht.gmall.publisher.mapper.TrafficVisitorStatsMapper;
import com.zht.gmall.publisher.service.TrafficVisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficVisitorStatsServiceImpl implements TrafficVisitorStatsService {

    @Autowired
    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

    @Override
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorTypeStats(date);
    }

    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }
}
