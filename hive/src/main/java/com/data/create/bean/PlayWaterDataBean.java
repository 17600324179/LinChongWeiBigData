package com.data.create.bean;

import lombok.*;

/**
 * @Description: 播放原始流水起播数据
 * @Author: chongweiLin
 * @CreateDate: 2020-06-05 10:35
 **/

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PlayWaterDataBean {
    private String imp_date; // 日期
    private String time_stamp; //时间戳
    private String guid; //设备ID
    private String start_type;  // 启动类型 广告启动（1）、app启动（2）、type页启动（3）
    private String pt; //产线

    @Override
    public String toString() {
        return imp_date + '\t' + time_stamp + '\t' + guid + '\t' + start_type + '\t' + pt;
    }
}
