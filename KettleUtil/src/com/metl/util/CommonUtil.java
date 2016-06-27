/**
* Project Name:KettleUtil
* Date:2016年6月21日上午12:10:20
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import org.pentaho.di.job.entries.eval.JobEntryEval;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 一般工具类 <br/>
 * date: 2016年6月21日 上午12:10:20 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class CommonUtil {
    /**
    * 获取参数 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @param key 参数名称
    * @return 值
    */
    public static String getProp(JobEntryEval jee, String key){
        return jee.environmentSubstitute("${"+key+"}");
    }
    /**
    * 获取参数并解析为JSON对象 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @param key 参数名称
    * @return 值
    */
    public static JSONObject getPropJSONObject(JobEntryEval jee, String key){
        return JSON.parseObject(getProp(jee, key));
    }
}
