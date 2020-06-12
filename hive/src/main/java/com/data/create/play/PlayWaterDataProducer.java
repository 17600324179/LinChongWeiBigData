package com.data.create.play;

import com.data.create.bean.GetBean;
import com.data.create.util.WriterDisk;

import java.io.IOException;
import java.io.PrintStream;

/**
 * @Description:
 * @Author: chongweiLin
 * @CreateDate: 2020-06-05 10:32
 **/
public class PlayWaterDataProducer {
    public static void main(String[] args) throws IOException {
        ptWaterData(10000000);
        h5WaterData(1000000);
        linuxWaterData(100000);

    }

    /**
     * pt产生的数据
     *
     * @throws IOException
     */
    private static void ptWaterData(long count) throws IOException {
        PrintStream printStream = WriterDisk.getPrintStream("C:\\Users\\Administrator\\Desktop\\result\\pt_play_water_");
        for (int i = 0; i < count; i++) {
            printStream.println(GetBean.getPlayWaterDataBean().toString());
        }
        WriterDisk.closeStream(printStream);
    }

    /**
     * H5产生的数据
     *
     * @throws IOException
     */
    private static void h5WaterData(long count) throws IOException {
        PrintStream printStream = WriterDisk.getPrintStream("C:\\Users\\Administrator\\Desktop\\result\\h5_play_water_");
        for (int i = 0; i < count; i++) {
            printStream.println(GetBean.getH5PlayWaterDataBean().toString());
        }
        WriterDisk.closeStream(printStream);
    }

    /**
     * Linux产生的数据
     *
     * @throws IOException
     */
    private static void linuxWaterData(long count) throws IOException {
        PrintStream printStream = WriterDisk.getPrintStream("C:\\Users\\Administrator\\Desktop\\result\\linux_play_water_");
        for (int i = 0; i < count; i++) {
            printStream.println(GetBean.getLinuxPlayWaterDataBean().toString());
        }
        WriterDisk.closeStream(printStream);
    }
}
