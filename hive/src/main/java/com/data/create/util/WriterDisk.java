package com.data.create.util;

import com.data.create.bean.GetBean;

import java.io.*;

/**
 * @Description:
 * @Author: chongweiLin
 * @CreateDate: 2020-06-05 16:10
 **/
public class WriterDisk {
    public static PrintStream getPrintStream(String path) throws FileNotFoundException {
        long timeStamp = System.currentTimeMillis();
        File file = new File(path  + GetBean.dateFormat.format(timeStamp) + "_" + timeStamp + ".txt");
        if (file.exists() == false) {
            file.getParentFile().mkdirs();
        }
        // 创建基于文件的输出流
        PrintStream printStream = new PrintStream(new FileOutputStream(file));

        return printStream;
    }

    public static void closeStream(PrintStream printStream) throws IOException {
        if (printStream != null) {
            printStream.close();
        }
    }
}
