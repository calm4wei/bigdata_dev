package com.zqykj.bigdata.alert.util;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by weifeng on 2017/6/9.
 */
public class DateUtils {

    public static boolean compare(long time, Integer monthAmount) {
        Date date = new Date(time);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, monthAmount);
        return date.after(calendar.getTime());
    }
}
