/**
* Project Name:eova
* Date:2016年5月24日下午7:18:51
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 常量 <br/>
 * date: 2016年5月24日 下午7:18:51 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public interface Constants {
    /**
    * eova数据源
    */
    String DATASOURCE_EOVA = "eova";
    /**
    * main数据源
    */
    String DATASOURCE_MAIN = "main";
	/**
	* metl数据源
	*/
	String DATASOURCE_METL = "metl";
    /**
    * metl临时数据源
    */
    String DATASOURCE_TEMP = "metl_temp";
    /**
    * kettle资源库数据源
    */
    String DATASOURCE_KETTLE = "kettle";
    /**
    * 禁止页面编辑的数据源
    */
    List<String> DISABLE_EDIT_DS = new ArrayList<String>(
            Arrays.asList(DATASOURCE_METL,
                    DATASOURCE_MAIN,DATASOURCE_EOVA));
    
    /**
    * 空字符串
    */
    String NULL_STR = "";
    /**
    * 反斜杠
    */
    String FXG = "/";
    /**
    * 单引号
    */
    String DYH = "'";
    /**
    * 百分号
    */
    String BFH = "%";
    /**
    * 双竖线
    */
    String SSX = "||";
    /**
    * 特殊连接符，一般不会在常规字符串中出现
    */
    String TSLJF1 = "TSLJF1";
    /**
    * 特殊连接符，一般不会在常规字符串中出现
    */
    String TSLJF2 = "TSLJF2";
    /**
    * 临时对象前缀
    */
    String TEMP_ = "TEMP_";
    /**
    * 临时表变量名称
    */
    String TEMP_TABLE = "TEMP_TABLE";

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
    * 字典类别-字段验证规则
    */
    String DICT_CATEGORY_FIELD_VALIDATE_RULE = "FIELD_VALIDATE_RULE";
    /**
    * 字典类别-字段转换规则
    */
    String DICT_CATEGORY_FIELD_TRANSITION_RULE = "FIELD_TRANSITION_RULE";
    /**
    * 字典类别-字段默认值规则
    */
    String DICT_CATEGORY_DEFAULT_RULE = "DEFAULT_RULE";
    
    /**
    * 定时类别-不需要定时
    */
    String SCHEDULER_TYPE_NOT_TIMING = "0";
    /**
    * 定时类别-时间间隔
    */
    String SCHEDULER_TYPE_TIME_INTERVAL = "1";
    /**
    * 定时类别-天
    */
    String SCHEDULER_TYPE_DAY = "2";
    /**
    * 定时类别-周
    */
    String SCHEDULER_TYPE_WEEK = "3";
    /**
    * 定时类别-月
    */
    String SCHEDULER_TYPE_MONTH = "4";

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
    
    /**
    * 成败-未知
    */
    String SUCCESS_FAILED_NUKNOW = "unknow";
    /**
    * 成败-成功
    */
    String SUCCESS_FAILED_SUCCESS = "success";
    /**
    * 成败-失败
    */
    String SUCCESS_FAILED_FAILED = "failed";

    /**
    * 数据类型-字符串
    */
    String DATA_TYPE_STRING = "string";
    /**
    * 数据类型-数字
    */
    String DATA_TYPE_NUMBER = "number";
    /**
    * 数据类型-时间
    */
    String DATA_TYPE_DATE = "date";
    /**
    * 数据类型-CLOB
    */
    String DATA_TYPE_CLOB = "clob";
    /**
    * 数据类型-BLOB
    */
    String DATA_TYPE_BLOB = "blob";

    /**
    * 数据库类型-ORACLE
    */
    String DS_TYPE_ORACLE = "oracle";
    /**
    * 数据库类型-PostgreSQL
    */
    String DS_TYPE_POSTGRESQL = "PostgreSQL";
    /**
    * 数据库类型-greenplum
    */
    String DS_TYPE_GREENPLUM = "greenplum";
    /**
    * 数据库类型-mysql
    */
    String DS_TYPE_MYSQL = "mysql";

    /**
    * kettle模板-任务根目录
    */
    String KETTLE_TP_ROOT_DIR = "/metl";
    /**
    * kettle模板-统一字典任务目录
    */
    String KETTLE_TP_UNIFY_DICT = KETTLE_TP_ROOT_DIR+"/unify_dict";
    /**
    * kettle模板-生成数据账单的JOB名称
    */
    String KETTLE_TP_GDB_NAME = "gdb_unify_dict";
    /**
    * kettle模板-执行数据账单的JOB名称
    */
    String KETTLE_TP_EDB_NAME = "edb_unify_dict";
    /**
    * kettle模板-执行数据账单的JOB-入临时对象的转换名称
    */
    String KETTLE_TP_EDB_TEMP_NAME = "入临时对象";
    /**
    * kettle模板-执行数据账单的JOB-入临时对象-取来源数据
    */
    String KETTLE_TP_EDB_TEMP_IN_NAME = "取来源数据";
    /**
    * kettle模板-执行数据账单的JOB-入目标对象的转换名称
    */
    String KETTLE_TP_EDB_TARGET_NAME = "入目标对象";
    /**
    * kettle模板-执行数据账单的JOB-入目标对象-取临时数据
    */
    String KETTLE_TP_EDB_TARGET_IN_NAME = "取临时数据";
    /**
    * kettle模板-执行数据账单的JOB名前缀
    */
    String KETTLE_TP_EDB_ = "edb_";
    /**
    * kettle模板-生成数据账单的JOB名前缀
    */
    String KETTLE_TP_GDB_ = "gdb_";

    /**
    * kettle参数名-数据账单主键
    */
    String KETTLE_PARAM_DATA_BILL_OID = "DATA_BILL_OID";
    /**
    * kettle参数名-kettle日志主键
    */
    String KETTLE_PARAM_KETTLE_LOG_OID = "KETTLE_LOG_OID";
    /**
    * kettle参数名-数据任务代码
    */
    String KETTLE_PARAM_DATA_TASK_OCODE = "DATA_TASK_OCODE";

    /**
    * 默认值-当前时间
    */
    String DEFAULT_VAL_CURRENT_DATE = "current_date";
    /**
    * 默认值-批次标记
    */
    String DEFAULT_VAL_BATCH = "batch";
    /**
    * 默认值-MD5去重
    */
    String DEFAULT_VAL_MD5_RR = "md5_rr";
    /**
    * 默认值-验证信息
    */
    String DEFAULT_VAL_VALIDATE_INFO = "validate_info";
    /**
    * 默认值-临时库表达式
    */
    String DEFAULT_VAL_TEMP_EXP = "temp_exp";
    /**
    * 默认值-目标库表达式
    */
    String DEFAULT_VAL_TARGET_EXP = "target_exp";
    /**
    * 默认值-类扩展
    */
    String DEFAULT_VAL_CLASS_EXPAND = "class_expand";
    /**
    * 默认值中不需要执行默认方法赋值的类型，这些默认值采用其他方式处理
    */
    List<String> DEFAULT_VAL_NOT_METHOD = new ArrayList<String>(
            Arrays.asList(DEFAULT_VAL_MD5_RR,DEFAULT_VAL_VALIDATE_INFO,
                    DEFAULT_VAL_TEMP_EXP));

    /**
    * 字段转换规则-时间转换
    */
    String FIELD_TRANSITION_RULE_DATE_TRANSITION = "date_transition";
    /**
    * 字段转换规则-身份证转换
    */
    String FIELD_TRANSITION_RULE_IDCORD_TRANSITION = "idcord_transition";
    /**
    * 字段转换规则-移除空白
    */
    String FIELD_TRANSITION_RULE_TRIM_BLANK= "trim_blank";
    /**
    * 字段转换规则-字典翻译
    */
    String FIELD_TRANSITION_RULE_DICT_TRANSITION = "dict_transition";
    /**
    * 字段转换规则-临时库表达式
    */
    String FIELD_TRANSITION_RULE_TEMP_EXP = "temp_exp";
    /**
    * 字段转换规则-目标库表达式
    */
    String FIELD_TRANSITION_RULE_TARGET_EXP = "target_exp";
    /**
    * 字段转换规则-类扩展
    */
    String FIELD_TRANSITION_RULE_CLASS_EXPAND = "class_expand";
    /**
    * 字段转换规则中不需要执行默认方法赋值的类型，这些转换规则采用其他方式处理
    */
    List<String> FIELD_TRANSITION_RULE_NOT_METHOD = new ArrayList<String>(
            Arrays.asList(FIELD_TRANSITION_RULE_TEMP_EXP));

    /**
    * 字段验证规则-时间验证
    */
    String FIELD_VALIDATE_RULE_DATE_VALIDATE = "date_validate";
    /**
    * 字段验证规则-身份证验证
    */
    String FIELD_VALIDATE_RULE_IDCORD_VALIDATE = "idcord_validate";
    /**
    * 字段验证规则-临时库表达式
    */
    String FIELD_VALIDATE_RULE_TEMP_EXP = "temp_exp";
    /**
    * 字段验证规则-目标库表达式
    */
    String FIELD_VALIDATE_RULE_TARGET_EXP = "target_exp";
    /**
    * 字段验证规则-类扩展
    */
    String FIELD_VALIDATE_RULE_CLASS_EXPAND = "class_expand";
    /**
    * 字段验证规则中不需要执行默认方法赋值的类型，这些验证规则采用其他方式处理
    */
    List<String> FIELD_VALIDATE_RULE_NOT_METHOD = new ArrayList<String>(
            Arrays.asList(FIELD_VALIDATE_RULE_TEMP_EXP));

    /**
    * JOB生成状态-未生成
    */
    String JOB_STATUS_NOT_GENERATE = "not_generate";
    /**
    * JOB生成状态-已生成
    */
    String JOB_STATUS_GENERATED = "generated";
    /**
    * JOB生成状态-待重生
    */
    String JOB_STATUS_WAIT_REPEAT_GENERATE = "wait_repeat_generate";

    /**
    * 字段名-对象主键
    */
    String FIELD_OID = "oid";
    /**
    * 字段名-对象名称
    */
    String FIELD_OCODE = "ocode";
    /**
    * 字段名-对象名称
    */
    String FIELD_ONAME = "oname";
    /**
    * 字段名-扩展信息
    */
    String FIELD_EXPAND = "expand";
    /**
    * 字段名-扩展信息-解析为JSON对象
    */
    String FIELD_EXPAND_OBJ = "expand_obj";
    /**
    * 字段名-默认值
    */
    String FIELD_DEFAULT_VALUE = "default_value";
    /**
    * 字段名-验证器
    */
    String FIELD_VALIDATOR = "validator";
    /**
    * 字段名-转换器
    */
    String FIELD_TRANSITION = "transition";
    /**
    * 字段名-字典id
    */
    String FIELD_ID = "id";
    /**
    * 字段名-字典中文
    */
    String FIELD_CN = "cn";
}
