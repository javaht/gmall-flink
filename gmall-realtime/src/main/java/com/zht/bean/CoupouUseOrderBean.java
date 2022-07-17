package com.zht.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CoupouUseOrderBean {
    String id;
    String coupou_id;
    String user_id;
    String order_id;
    String date_id;
    String using_time;
    String old;
    String ts;
}
