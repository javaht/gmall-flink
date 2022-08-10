package com.zht.gmall.publisher.service;

import com.zht.gmall.publisher.bean.UserChangeCtPerType;
import com.zht.gmall.publisher.bean.UserPageCt;
import com.zht.gmall.publisher.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserPageCt> getUvByPage(Integer date);

    List<UserChangeCtPerType> getUserChangeCt(Integer date);

    List<UserTradeCt> getTradeUserCt(Integer date);
}
