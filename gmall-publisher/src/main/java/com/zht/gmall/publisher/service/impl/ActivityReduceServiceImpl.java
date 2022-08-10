package com.zht.gmall.publisher.service.impl;

import com.zht.gmall.publisher.bean.ActivityReduceStats;
import com.zht.gmall.publisher.mapper.ActivityStatsMapper;
import com.zht.gmall.publisher.service.ActivityReduceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ActivityReduceServiceImpl implements ActivityReduceService {

    @Autowired
    private ActivityStatsMapper activityStatsMapper;

    @Override
    public List<ActivityReduceStats> getActivityStats(Integer date) {
        return activityStatsMapper.selectActivityStats(date);
    }
}
