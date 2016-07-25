/**
* Project Name:test-java
* Date:2015年11月3日上午10:21:16
* Copyright (c) 2015, jingma@iflytek.com All Rights Reserved.
*/
/**
* Project Name:test-java
* Date:2015年11月3日上午10:21:16
* Copyright (c) 2015, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.util.StringUtil;

/**
 * 时间工具集 <br/>
 * date: 2015年11月3日 上午10:21:16 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class DateUtil extends DateUtils{
    /**
    * 日志
    */
    private static Log log = LogFactory.getLog(DateUtil.class);
	/**
	 * 长日期格式
	 */
	public static final String DATE_FORMATTER_L = "yyyy-MM-dd HH:mm:ss";
	/**
	 * 短日期格式
	 */
	public static final String DATE_FORMATTER_S = "yyyy-MM-dd";
	/**
	 * 日期格式(14位)
	 */
	public static final String DATE_FORMATTER14 = "yyyyMMddHHmmss";
	/**
	 * 日期格式(8位)
	 */
	public static final String DATE_FORMATTER8 = "yyyyMMdd";
	/**
	* 常见时间格式化器:yyyyMMddHHmmss
	*/
	public static final SimpleDateFormat sdf14 = new SimpleDateFormat(DATE_FORMATTER14);
    /**
    * 常见时间格式化器:yyyy-MM-dd HH:mm:ss
    */
    public static final SimpleDateFormat sdf19 = new SimpleDateFormat(DATE_FORMATTER_L);

	/**
	* 解析大部分常见日期格式 <br/>
	* @author jingma@iflytek.com
	* @param dateStr 要解析的字符串
	* @return 时间对象，解析失败则为空
	*/
	public static Date parseDate(String dateStr){
		if(StringUtil.isEmpty(dateStr)){
			return null;
		}
		String parse = dateStr;
		DateFormat format = null;
		parse = parse.replaceFirst("^(18|19|20|21){1}[0-9]{2}([^0-9]?)", "yyyy$2");
		parse = parse.replaceFirst("^[0-9]{2}([^0-9]?)", "yy$1");
		parse = parse.replaceFirst("([^0-9]?)(1{1}[0-2]{1}|0?[1-9]{1})([^0-9]?)", "$1MM$3");
		parse = parse.replaceFirst("([^0-9]?)(3{1}[0-1]{1}|[0-2]?[0-9]{1})([^0-9]?)", "$1dd$3");
		parse = parse.replaceFirst("([^0-9]?)(2[0-3]{1}|[0-1]?[0-9]{1})([^0-9]?)", "$1HH$3");
		parse = parse.replaceFirst("([^0-9]?)[0-5]?[0-9]{1}([^0-9]?)", "$1mm$2");
		parse = parse.replaceFirst("([^0-9]?)[0-5]?[0-9]{1}([^0-9]?)", "$1ss$2");
		parse = parse.replaceFirst("([^0-9]?)[0-9]{1,3}([^0-9]*)", "$1SSS$2");
		try {
			format = new SimpleDateFormat(parse);
			//设置为严格验证时间格式，默认是非严格的，1月32不会报错，会解析为2月1号。
			format.setLenient(false);
			Date date = format.parse(dateStr);
			log.debug(String.format("原始字符串：%s,判断格式：%s,解析结果：%s", dateStr,parse,sdf19.format(date)));
			return date;
		} catch (Exception e) {
		    log.error(String.format("日期解析出错：%s-->%s",parse,dateStr));
			log.debug(null, e);
		}
		return null;
	}
    
    /**
     * 将时间对象转换为14位长度的时间字符串
     * @param value 需要转换的参数
     * @return 返回转换后的数值
     */
    public static String dateToStr14(Object value) { 
        if (value == null) {
            return null; 
        } else if (value instanceof java.util.Date){ 
            return sdf14.format((Date) value);
        } else if(value instanceof java.sql.Date){
            java.sql.Date sqlDate = (java.sql.Date)value;
            return sdf14.format(new Date(sqlDate.getTime()));
        } else {
            Date val = parseDate(value.toString());
            if(val==null){
                return null;
            }
            return sdf14.format(val); 
        } 
    }
    /**
     * 返回日期所在的季度
     * @param date
     * @return
     */
    public static int getSeasons(Date date) {
        int m = getMonth(date);
        if (m <= 0) {
            return 0;
        } else if (m < 4)
            return 1;
        else if (m < 7)
            return 2;
        else if (m < 10)
            return 3;
        else if (m < 13)
            return 4;
        else
            return 0;
    }
    /**
     * 给定日期所在的季度，并返回该季度的第一天日期,如果指定日期错误，返回null
     * @param date
     * @return
     */
    public static Date getNowSeasonsFirstDay(Date date) {
        int m = getSeasons(date);
        if (m > 0) {
            if (m == 1) {
                return parseDate(getYear(date) + "-01-01");
            } else if (m == 2) {
                return parseDate(getYear(date) + "-04-01");
            } else if (m == 3) {
                return parseDate(getYear(date) + "-07-01");
            } else {
                return parseDate(getYear(date) + "-10-01");
            }
        }
        return null;
    }

    /**
     * 得到某年的最后一天的日期
     * @param year
     * @return
     */
    public static Date getYearLastDay(String year) {
        if (year == null || "".equals(year))
            return null;
        Date nd = parseDate(year + "-01-01");
        return addDays(addYears(nd, 1), -1);
    }

    /**
     * 得到下个月的第一天
     * @param year
     * @param month
     * @return
     */
    public static Date getNextMonthFirstDay(String year, String month) {
        if (year == null || "".equals(year) || month == null
                || "".equals(month))
            return null;
        Date nd = parseDate(year + "-" + month + "-01");
        return addMonths(nd, 1);
    }

    /**
     * 得到某年月的最后一天的日期
     * @param year
     * @param month
     * @return
     */
    public static Date getMonthLastDay(String year, String month) {
        if (year == null || "".equals(year) || month == null
                || "".equals(month))
            return null;
        Date nd = parseDate(year + "-" + month + "-01");
        return addDays(addMonths(nd, 1), -1);
    }

    /**
     * 计算两个日期相差的月数
     * @param st 起始日期
     * @param end 结束日期
     * @return
     */
    public static int compareMonth(Date st, Date end) {
        int y = Math.abs((getYear(end) < 0 ? 0 : getYear(end))
                - (getYear(st) < 0 ? 0 : getYear(st)));
        int m = 0;
        if (y > 0) {
            y--;
            m = Math.abs(12 - getMonth(st) + getMonth(end));
        } else {
            m = Math.abs(getMonth(end) - getMonth(st));
        }
        return (y * 12) + m;
    }

    /**
     * 计算两个日期相差的毫秒数
     * @param start 启始时间
     * @param end 结束时间
     * @return
     */
    public static long compare(Date start, Date end) {
        if (start != null && end != null) {
            return end.getTime() - start.getTime();
        }
        return 0l;
    }

    /**
     * 判断给的日期，是否是当前的前一天以及更早的日期，若是，返回true，否则返回false
     * @param date
     * @return
     */
    public static boolean compareDate(Date date) {
        if (date != null) {
            return date.before(parseDate(doFormatDate(new Date(), DATE_FORMATTER_S)));
        }
        return false;
    }

    /**
     * 自定义格式化日期输出
     * @param date
     * @param format
     * @return
     */
    public static String doFormatDate(Date date, String format) {
        if (date == null)
            return null;
        return (new SimpleDateFormat(format)).format(date);
    }

    /**
     * 获取当前的年,如果是-1，则表示错误
     * @return
     */
    public static int getYear() {
        return getYear(new Date());
    }

    /**
     * 获取指定日期的年,如果是-1，则表示错误
     * @param date
     * @return
     */
    public static int getYear(Date date) {
        if (date == null)
            return -1;
        return DateToCalendar(date).get(Calendar.YEAR);
    }

    /**
     * 获取当前月，如果返回"0"，则表示错误
     * @return
     */
    public static int getMonth() {
        return getMonth(new Date());
    }

    /**
     * 获取当前月，如果返回"0"，则表示错误
     * @param date
     * @return
     */
    public static int getMonth(Date date) {
        if (date == null)
            return 0;
        return DateToCalendar(date).get(Calendar.MONTH) + 1;
    }

    /**
     * 获取当天日,如果返回"0",表示该日期无效或为null
     * @return
     */
    public static int getDay() {
        return getDay(new Date());
    }

    /**
     * 取一个日期的日,如果返回"0",表示该日期无效或为null
     * @param da
     * @return
     */
    public static int getDay(Date da) {
        if (da == null)
            return 0;
        return DateToCalendar(da).get(Calendar.DATE);
    }

    /**
     * 将java.util.Date类型的日期格式转换成java.util.Calendar格式的日期
     * @param dd
     * @return
     */
    public static Calendar DateToCalendar(Date dd) {
        Calendar cc = Calendar.getInstance();
        cc.setTime(dd);
        return cc;
    }

    /**
     * 将一个长整型数据转为日期
     * @param datenum
     * @return
     */
    public static Date longToDate(long datenum) {
        Calendar cc = Calendar.getInstance();
        cc.setTimeInMillis(datenum);
        return cc.getTime();
    }

    /**
     * 将一个长整型数据转为日期格式的字符串
     * @param datenum
     * @return
     */
    public static String longToDateString(long datenum) {
        return doFormatDate(longToDate(datenum), DATE_FORMATTER_L);
    }

    /**
     * 得到给定日期的前一个周日的日期
     * @param date
     * @return
     */
    public static Date getUpWeekDay(Date date) {
        if (date == null) {
            return null;
        } else {
            Calendar cc = Calendar.getInstance();
            cc.setTime(date);
            int week = cc.get(Calendar.DAY_OF_WEEK);
            return DateUtils.addDays(date, (1 - week));
        }
    }

    /**
     * 得到给定日期所在周的周一日期
     * @param date
     * @return
     */
    public static Date getMonday(Date date) {
        if (date == null) {
            return null;
        } else {
            Calendar cc = Calendar.getInstance();
            cc.setTime(date);
            int week = cc.get(Calendar.DAY_OF_WEEK);
            return DateUtils.addDays(date, (2 - week));
        }
    }

    /**
     * 得到指定日期所在的周（1-7），惹指定的日期不存在，则返回“-1”
     * @param date
     * @return -1 or 1-7
     */
    public static int getWeek(Date date) {
        if (date == null) {
            return -1;
        } else {
            Calendar cc = Calendar.getInstance();
            cc.setTime(date);
            int week = cc.get(Calendar.DAY_OF_WEEK);
            if (week == 1) {
                week = 7;
            } else {
                week--;
            }
            return week;
        }
    }

    /**
     * 将DATE转为数据库的Timestamp类型
     * @param dt
     * @return
     */
    public static Timestamp dateToTime(Date dt) {
        if (dt == null)
            return null;
        return new Timestamp(dt.getTime());
    }

    /**
     * method 将字符串类型的日期转换为一个timestamp（时间戳记java.sql.Timestamp）
     * @param dateString 需要转换为timestamp的字符串
     * @return dataTime timestamp
     */
    @SuppressWarnings("static-access")
    public static java.sql.Timestamp string2Time(String dateString)
            throws java.text.ParseException {
        DateFormat dateFormat;
        dateFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss.SSS",
                Locale.ENGLISH);// 设定格式
        // dateFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss",
        // Locale.ENGLISH);
        dateFormat.setLenient(false);
        // 我做这块的时候下边的不对，后来我把dateFormat后边加上了.getDateInstance()就好了
        // java.util.Date timeDate = dateFormat.parse(dateString);//util类型
        java.util.Date ywybirt = dateFormat.getDateInstance().parse(dateString);// util类型
        java.sql.Timestamp dateTime = new java.sql.Timestamp(ywybirt.getTime());// Timestamp类型,timeDate.getTime()返回一个long型
        return dateTime;
    }

    /**
     * 将日期格式转为java.sql.Date
     * 
     * @param de
     * @return
     */
    public static java.sql.Date dateToSqlDate(Date de) {
        return new java.sql.Date(de.getTime());
    }

    /**
     * 返回指定格式的日期字符串
     * @param format 格式字符串
     * @return
     */
    public static String getDateTimeStr(String format){
    	return doFormatDate(new Date(), format);
    }

    /**
     * 返回指定格式的日期字符串
     * @param format 格式字符串
     * @param jiaTian 加几天
     * @return
     */
    public static String getDateTimeStr(String format,int jiaTian){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, jiaTian);
        return doFormatDate(cal.getTime(), format);
    }
    /**
     * 返回yyyy-MM-dd HH:mm:ss格式的日期字符串
     * @return
     */
    public static String getDateTimeStr(){
    	return getDateTimeStr(DATE_FORMATTER_L);
    }
    /**
     * 以公安部的日期格式返回当前系统时间
     * 
     * @return
     */
    public static String getGabDate() {
        return getDateTimeStr(DATE_FORMATTER14);
    }
}
