package com.data.create.payflow;

import com.data.create.bean.GetBean;
import com.data.create.util.WriterDisk;

import java.io.IOException;
import java.io.PrintStream;

/**
 * @Description:
 * @Author: chongweiLin
 * @CreateDate: 2020-06-08 09:59
 **/
public class GuidBlackDataProducer {
    public static void main(String[] args) throws IOException {
        int count = 100000;
        PrintStream printStream = WriterDisk.getPrintStream("C:\\Users\\Administrator\\Desktop\\result\\guid_black_");
        for (int i = 0; i < count; i++) {
            printStream.println(GetBean.getGuidBlackDataBean().toString());
        }
        WriterDisk.closeStream(printStream);
    }
}
