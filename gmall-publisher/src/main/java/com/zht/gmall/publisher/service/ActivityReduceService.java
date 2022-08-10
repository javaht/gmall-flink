package com.zht.gmall.publisher.service;

import com.zht.gmall.publisher.bean.ActivityReduceStats;

import java.util.List;

public interface ActivityReduceService {
    List<ActivityReduceStats> getActivityStats(Integer date);
}
