package com.zht.gmall.publisher.service.impl;

import com.zht.gmall.publisher.bean.TrafficKeywords;
import com.zht.gmall.publisher.mapper.TrafficKeywordsMapper;
import com.zht.gmall.publisher.service.TrafficKeywordsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficKeywordsServiceImpl implements TrafficKeywordsService {

    @Autowired
    TrafficKeywordsMapper trafficKeywordsMapper;

    @Override
    public List<TrafficKeywords> getKeywords(Integer date) {
        return trafficKeywordsMapper.selectKeywords(date);
    }
}
