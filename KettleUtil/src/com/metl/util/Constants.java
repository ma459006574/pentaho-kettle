/**
* Project Name:eova
* Date:2016年5月24日下午7:18:51
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;


/**
 * 常量 <br/>
 * date: 2016年5月24日 下午7:18:51 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public interface Constants {
	/**
	* metl数据源
	*/
	String DATASOURCE_METL = "metl";
    /**
    * 空字符串
    */
    String NULL_STR = "";

    /**
    * 缓存-字典
    */
    String CACHE_DICT = "dict";
    
	/**
	* 逻辑判断-是
	*/
	String WHETHER_TRUE="1";
	/**
	* 逻辑判断-否
	*/
	String WHETHER_FALSE="0";
	
	/**
	* 连接状态-未知
	*/
	String LINK_STATUS_UNKNOW="0";
	/**
	* 连接状态-成功
	*/
	String LINK_STATUS_SUCCESS="1";
	/**
	* 连接状态-失败
	*/
	String LINK_STATUS_FAILED="2";
	
    /**
    * 数据对象类型-表
    */
    String DO_TYPE_TABLE = "table";
    /**
    * 数据对象类型-视图
    */
    String DO_TYPE_VIEW = "view";
    /**
    * 数据对象类型-txt文本
    */
    String DO_TYPE_TXT = "txt";
    /**
    * 数据对象类型-csv文本
    */
    String DO_TYPE_CSV = "csv";
    /**
    * 数据对象类型-03版EXCEL
    */
    String DO_TYPE_XLS = "xls";
    /**
    * 数据对象类型-07版EXCEL
    */
    String DO_TYPE_XLSX = "xlsx";

    /**
    * 字段业务类型-姓名
    */
    String FB_TYPE_NAME = "name";
    /**
    * 字段业务类型-身份证号
    */
    String FB_TYPE_IDCARD = "idcard";
    /**
    * 字段业务类型-时间
    */
    String FB_TYPE_DATE = "date";
    /**
    * 字段业务类型-IP地址
    */
    String FB_TYPE_IP_ARRDRESS = "ip_address";
    /**
    * 字段业务类型-字典
    */
    String FB_TYPE_DICT = "dictionary";
    /**
    * 字段业务类型-是否判断
    */
    String FB_TYPE_WHETHER = "whether";
    /**
    * 字段业务类型-一般代码
    */
    String FB_TYPE_GENERAL_CODE = "general_code";
    /**
    * 字段业务类型-数字
    */
    String FB_TYPE_NUMBER = "number";
    /**
    * 字段业务类型-短字符串
    */
    String FB_TYPE_SHORT_STRING = "short_string";
    /**
    * 字段业务类型-长字符串
    */
    String FB_TYPE_LONG_STRING = "long_string";
    /**
    * 字段业务类型-图片
    */
    String FB_TYPE_IMAGE = "image";
    /**
    * 字段业务类型-颜色
    */
    String FB_TYPE_COLOR = "color";
    /**
    * 字段业务类型-图标
    */
    String FB_TYPE_ICON = "icon";
    /**
    * 字段业务类型-密码
    */
    String FB_TYPE_PASSWORD = "password";
    /**
    * 字段业务类型-文件
    */
    String FB_TYPE_FILE = "file";
    
    /**
    * 字段数据类型-字符串
    */
    String FD_TYPE_STRING = "string";
    /**
    * 字段数据类型-数字
    */
    String FD_TYPE_NUMBER = "number";
    /**
    * 字段数据类型-时间
    */
    String FD_TYPE_DATE = "date";
    /**
    * 字段数据类型-CLOB
    */
    String FD_TYPE_CLOB = "clob";
    /**
    * 字段数据类型-BLOB
    */
    String FD_TYPE_BLOB = "blob";
    
    /**
    * 字典类别-数据库类型
    */
    String DICT_CATEGORY_DATABASE_TYPE = "DATABASE_TYPE";
    /**
    * 字典类别-数据库
    */
    String DICT_CATEGORY_DATABASE = "DATABASE";
    /**
    * 字典类别-星期
    */
    String DICT_CATEGORY_WEEK_DAY = "WEEK_DAY";
    /**
    * 字典类别-一般配置
    */
    String DICT_CATEGORY_GENERAL_CONFIG = "GENERAL_CONFIG";
    
    /**
    * 定时类别-不需要定时
    */
    String SCHEDULER_TYPE_NOT_TIMING = "not_timing";
    /**
    * 定时类别-时间间隔
    */
    String SCHEDULER_TYPE_TIME_INTERVAL = "time_interval";
    /**
    * 定时类别-天
    */
    String SCHEDULER_TYPE_DAY = "day";
    /**
    * 定时类别-周
    */
    String SCHEDULER_TYPE_WEEK = "week";
    /**
    * 定时类别-月
    */
    String SCHEDULER_TYPE_MONTH = "month";

    /**
    * 数据账单状态-待审核
    */
    String DATA_BILL_STATUS_WAIT_EXAMINE = "wait_examine";
    /**
    * 数据账单状态-审核通过
    */
    String DATA_BILL_STATUS_EXAMINE_PASS = "examine_pass";
    /**
    * 数据账单状态-审核未通过
    */
    String DATA_BILL_STATUS_EXAMINE_NOT_PASS = "examine_not_pass";
    /**
    * 数据账单状态-入库中
    */
    String DATA_BILL_STATUS_CURRENT_INPUT = "current_input";
    /**
    * 数据账单状态-入库失败
    */
    String DATA_BILL_STATUS_INPUT_FAILED = "input_failed";
    /**
    * 数据账单状态-入库成功
    */
    String DATA_BILL_STATUS_INPUT_SUCCESS = "input_success";
}
