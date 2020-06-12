package com.data.create.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @Description: guid黑名单表的数据
 * @Author: chongweiLin
 * @CreateDate: 2020-06-08 09:55
 **/
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class GuidBlackBean {
    private String imp_date;
    private String guid;

    @Override
    public String toString() {
        return imp_date + "\t" + guid;
    }
}
