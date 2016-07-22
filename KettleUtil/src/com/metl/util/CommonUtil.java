/**
* Project Name:KettleUtil
* Date:2016年6月21日上午12:10:20
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.pentaho.di.trans.step.StepInterface;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;

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
    * @param vs 
    * @param key 参数名称
    * @return 值
    */
    public static String getProp(VariableSpace vs, String key){
        String value = vs.environmentSubstitute("${"+key+"}");
        if(value.startsWith("${")){
            return "";
        }else{
            return value;
        }
    }
    /**
    * 获取参数并解析为JSON对象 <br/>
    * @author jingma@iflytek.com
    * @param vs 
    * @param key 参数名称
    * @return 值
    */
    public static JSONObject getPropJSONObject(VariableSpace vs, String key){
        return JSON.parseObject(getProp(vs, key));
    }
    /**
    * 获取根job <br/>
    * @author jingma@iflytek.com
    * @param rootjob 
    * @return
    */
    public static Job getRootJob(Job rootjob) {
        while(rootjob!=null&&rootjob.getParentJob()!=null){
            rootjob = rootjob.getParentJob();
        }
        return rootjob;
    }
    /**
    * 获取根job <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @return
    */
    public static Job getRootJob(JobEntryEval jee) {
        Job rootjob = jee.getParentJob();
        return getRootJob(rootjob);
    }
    /**
    * 获取根job <br/>
    * @author jingma@iflytek.com
    * @param si 
    * @return
    */
    public static Job getRootJob(StepInterface si) {
        Job rootjob = si.getTrans().getParentJob();
        return getRootJob(rootjob);
    }
    /**
    * 获取根job的id <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @return
    */
    public static String getRootJobId(JobEntryEval jee) {
        return getRootJob(jee).getObjectId().getId();
    }
    /**
    * 获取根job的id <br/>
    * @author jingma@iflytek.com
    * @param si 
    * @return
    */
    public static String getRootJobId(StepInterface si) {
        Job rootjob = getRootJob(si);
        if(rootjob!=null){
            return rootjob.getObjectId().getId();
        }else{
            return null;
        }
    }
    /**
    * 获取根job的名称 <br/>
    * @author jingma@iflytek.com
    * @param si 
    * @return
    */
    public static String getRootJobName(StepInterface si) {
        Job rootjob = getRootJob(si);
        if(rootjob!=null){
            return rootjob.getObjectName();
        }else{
            return null;
        }
    }

    /**
    * metl中的数据库类型转换为kettle中的数据库类型 <br/>
    * @author jingma@iflytek.com
    * @param metlDs
    * @return
    */
    public static String metlDsToKettleDs(String metlDs) {
        if(Constants.DS_TYPE_ORACLE.equals(metlDs)){
            return "Oracle";
        }else if(Constants.DS_TYPE_MYSQL.equals(metlDs)){
            return "mysql";
        }
        return null;
    }
}
