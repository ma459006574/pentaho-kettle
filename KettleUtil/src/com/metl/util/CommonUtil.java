/**
* Project Name:KettleUtil
* Date:2016年6月21日上午12:10:20
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import org.pentaho.di.job.Job;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.steps.scriptvalues_mod.ScriptValuesMod;

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
        String value = jee.environmentSubstitute("${"+key+"}");
        if(value.startsWith("${")){
            return "";
        }else{
            return value;
        }
    }
    /**
    * 获取参数 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @param key 参数名称
    * @return 值
    */
    public static String getProp(BaseStep jee, String key){
        String value = jee.environmentSubstitute("${"+key+"}");
        if(value.startsWith("${")){
            return "";
        }else{
            return value;
        }
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
    /**
    * 获取根job <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @return
    */
    public static Job getRootJob(JobEntryEval jee) {
        Job rootjob = jee.getParentJob();
        while(rootjob.getParentJob()!=null){
            rootjob = rootjob.getParentJob();
        }
        return rootjob;
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
    * 获取根job <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @return
    */
    public static Job getRootJob(ScriptValuesMod jee) {
        Job rootjob = jee.getTrans().getParentJob();
        while(rootjob!=null&&rootjob.getParentJob()!=null){
            rootjob = rootjob.getParentJob();
        }
        return rootjob;
    }
    /**
    * 获取根job的id <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @return
    */
    public static String getRootJobId(ScriptValuesMod jee) {
        Job rootjob = getRootJob(jee);
        if(rootjob!=null){
            return rootjob.getObjectId().getId();
        }else{
            return null;
        }
    }
    /**
    * 获取根job的名称 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @return
    */
    public static String getRootJobName(ScriptValuesMod jee) {
        Job rootjob = getRootJob(jee);
        if(rootjob!=null){
            return rootjob.getObjectName();
        }else{
            return null;
        }
    }
}
