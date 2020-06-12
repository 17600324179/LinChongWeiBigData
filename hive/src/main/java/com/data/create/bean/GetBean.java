package com.data.create.bean;


import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

/**
 * @Description:
 * @Author: chongweiLin
 * @CreateDate: 2020-06-05 14:59
 **/
public class GetBean {
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static Random random = new Random();
    private static HashMap<Integer, String> ptHashMap = new HashMap<Integer, String>();

    static {
        ptHashMap.put(0, "TCL");
        ptHashMap.put(1, "SNMAPP");
        ptHashMap.put(2, "SWCO");
        ptHashMap.put(3, "TVMORE");
        ptHashMap.put(4, "KK");
        ptHashMap.put(5, "MI");
    }

    ;

    public static GuidBlackBean getGuidBlackDataBean() {
        String imp_date = dateFormat.format(System.currentTimeMillis());
        String guid = UUID.randomUUID().toString();
        return new GuidBlackBean(imp_date, guid);
    }

    /**
     * @return ods层启动流水数据对象（日期、时间戳、设备id、启动类型、产线）
     */
    public static PlayWaterDataBean getPlayWaterDataBean() {
        long time_stamp = System.currentTimeMillis();
        String imp_date = dateFormat.format(time_stamp);
        String guid = UUID.randomUUID().toString();
        String start_type = String.valueOf(random.nextInt(3));
        String pt = ptHashMap.get(random.nextInt(6));
        return new PlayWaterDataBean(imp_date, String.valueOf(time_stamp), guid, start_type, pt);
    }

    /**
     * @return ods层H5启动流水数据对象（日期、时间戳、设备id、启动类型、产线）
     */
    public static PlayWaterDataBean getH5PlayWaterDataBean() {
        long time_stamp = System.currentTimeMillis();
        String imp_date = dateFormat.format(time_stamp);
        String guid = UUID.randomUUID().toString();
        String start_type = String.valueOf(random.nextInt(3));
        String pt = ptHashMap.get(random.nextInt(6));
        return new PlayWaterDataBean(imp_date, String.valueOf(time_stamp), guid, start_type, "H5");
    }

    /**
     * @return ods层H5启动流水数据对象（日期、时间戳、设备id、启动类型、产线）
     */
    public static PlayWaterDataBean getLinuxPlayWaterDataBean() {
        long time_stamp = System.currentTimeMillis();
        String imp_date = dateFormat.format(time_stamp);
        String guid = UUID.randomUUID().toString();
        String start_type = String.valueOf(random.nextInt(3));
        String pt = ptHashMap.get(random.nextInt(6));
        return new PlayWaterDataBean(imp_date, String.valueOf(time_stamp), guid, start_type, "Linux");
    }
}
