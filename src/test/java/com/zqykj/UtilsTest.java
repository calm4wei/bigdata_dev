package com.zqykj;

import com.zqykj.bigdata.alert.util.DateUtils;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by weifeng on 2017/6/9.
 */
public class UtilsTest {

    @Test
    public void compareDate() {
        long time = 1494159294737l;
        System.out.println(DateUtils.compare(time, -1));
    }

    @Test
    public void getBeforeTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(1496837694737l));
        calendar.add(Calendar.MONTH, -1);
        System.out.println(calendar.getTime().getTime());
    }


}
