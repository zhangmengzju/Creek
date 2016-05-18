package com.creek.streaming.framework.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtils {
    private static final Logger LOGGER            = LoggerFactory.getLogger(DateUtils.class);
    private static final int    TIME_15_MIN_IN_MS = 15 * 60 * 1000;

    public static long DateToLong(String format, String dateStr, String tZoneStr)
            throws ParseException {
        TimeZone tZone = TimeZone.getTimeZone(tZoneStr);

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(tZone);

        Date dt = sdf.parse(dateStr);

        return dt.getTime();
    }

    @SuppressWarnings("deprecation")
    public static String conventTimeToLocaleString(long time, String timeZone) {
        // 中国时间    timeZone="Asia/Shanghai"   
        // 美国时间    timeZone="America/Los_Angeles"
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
        return new Date(time).toLocaleString();
    }

    public static String conventTimeToString(long time, String tZone, String format) {//
        // 中国时间    timeZone="Asia/Shanghai"   
        // 美国时间    timeZone="America/Los_Angeles"
        //        TimeZone.setDefault();
        //        return new Date(time).toString();
        Date date = new Date(time);
        SimpleDateFormat sdf = new SimpleDateFormat(format);//"yyyy-MM-dd HH:mm:ss"
        sdf.setTimeZone(TimeZone.getTimeZone(tZone));

        String dateText = sdf.format(date);
        return dateText;
    }

    public static long getTimeLongByCountry(String country, String timeStr) throws ParseException {
        LOGGER.debug(String.format("[country:%s][timeStr:%s]", country, timeStr));
        if (country.equals("cn")) {
            return DateUtils.DateToLong("yyyy-MM-dd HH:mm:ss", timeStr, "Asia/Shanghai");
        } else if (country.equals("us")) {
            return DateUtils.DateToLong("yyyy-MM-dd HH:mm:ss", timeStr, "America/Los_Angeles");
        }
        return 0L;
    }

    public static String getTimeIn15MinStringByCountry(String country, long timeLong) {
        if (country.toLowerCase().equals("cn")) {
            return DateUtils.conventTimeToString(
                    (timeLong / TIME_15_MIN_IN_MS * TIME_15_MIN_IN_MS), "Asia/Shanghai",
                    "yyyy-MM-dd HH:mm:ss");//转成5min粒度的时刻
        } else if (country.toLowerCase().equals("us")) {
            return DateUtils.conventTimeToString(
                    (timeLong / TIME_15_MIN_IN_MS * TIME_15_MIN_IN_MS), "America/Los_Angeles",
                    "yyyy-MM-dd HH:mm:ss");//转成5min粒度的时刻
        } else {
            LOGGER.warn(String.format("[ warn ][DateUtils getTimeString country: %s]", country));
            return null;
        }
    }

    public static int compareDate() {
        String inputDate1 = "2011-05-14 23:30:00";
        String inputDate2 = "2011-05-14 23:35:00";
        Date date1 = null;
        Date date2 = null;

        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            date1 = inputFormat.parse(inputDate1);
            date2 = inputFormat.parse(inputDate2);
        } catch (ParseException e) {
            LOGGER.warn("[DateUtils][compareDate error][" + date1 + "][" + date2 + "]");
        }

        return date1.compareTo(date2);
    }

    
    public static Date stringToDate(String ts) throws ParseException {
        //ts = "2011-05-14 23:30:00";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = formatter.parse(ts);
        return date;
    }
    

    public static String dateToString(Date date){ 
        SimpleDateFormat formatter = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss"); 
        String ts = formatter.format(date);     
        return ts; 
    } 

    public static String getTs2DayBack(String ts) throws ParseException{
        Date curDate = stringToDate(ts); 
 
        Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.HOUR, -48);
         
        String ts2DayBack = dateToString(cal.getTime());
        return ts2DayBack;
    }
    
    public static void main(String[] args) throws ParseException {
        String str = getTs2DayBack("2011-03-01 23:30:00");
        System.out.println("[str]"+str);
        //        TimeZone timeZoneSH = TimeZone.getTimeZone("Asia/Shanghai");
        //        TimeZone timeZoneLA = TimeZone.getTimeZone("America/Los_Angeles");
        //
        //        String format = "yyyy-MM-dd HH:mm:ss";
        //        String dateStr = "2015-08-26 01:38:46";
        //
        //        long timeLongSH = DateToLong(format, dateStr, timeZoneSH);
        //        System.out.println("[Long timeLongSH]" + timeLongSH);
        //
        //        long timeLongLA = DateToLong(format, dateStr, timeZoneLA);
        //        System.out.println("[Long timeLongSH]" + timeLongLA);

        //        String timeLocaleString = conventTimeToLocaleString(timeLong, "Asia/Shanghai");
        //        System.out.println("[LocaleString]" + timeLocaleString);
        //
        //        String timeString = conventTimeToString(timeLong, "America/Los_Angeles");
        //        System.out.println("[String]" + timeString);

        //        converter();
        //        compareDate();

        //        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
        //        Date currentDate = calendar.getTime();
        //        System.out.println("[curTime Long]" + currentDate);
        //
        //        Calendar calendar1 = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        //        Date currentDate1 = calendar1.getTime();
        //        System.out.println("[curTime Long]" + currentDate1);      

        //                Date date = new Date();
        //                System.out.println("[date]" + date.toString());

        //        long val = 1346524199000l;
        //        Date date = new Date(val);
        //        SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //        String dateText = df2.format(date);
        //        System.out.println(dateText);

        //        String strCN = getTimeIn15MinStringByCountry("cn", 1445410380000L);
        //        System.out.println("[strCN]" + strCN);
        //
        //        String strUS = getTimeIn15MinStringByCountry("us", 1445410380000L);
        //        System.out.println("[strUS]" + strUS);

    }

}
