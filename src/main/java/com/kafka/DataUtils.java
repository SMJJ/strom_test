package com.kafka;


import java.text.SimpleDateFormat;
import java.util.Date;

public class DataUtils {

    public static String getTime(String time) throws Exception{
        String s = time.substring(1,time.length()-1);
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }

//    public static void main(String[] args) throws Exception {
//        System.out.println(DataUtils.getTime("2019-12-23 23:00:00"));
//    }

}
