/**
* Project Name:KettleUtil
* Date:2016年7月12日下午3:24:29
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.rule;

import com.alibaba.fastjson.JSONObject;
import com.metl.util.CommonUtil;
import com.metl.util.DateUtil;
import com.metl.utilrun.ExecuteDataEtlRule;

/**
 * 字段默认值规则 <br/>
 * date: 2016年7月12日 下午3:24:29 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class FieldDefaultValue {
    /**
    * 执行数据对象定义的验证转换默认值等规则 
    */
    private ExecuteDataEtlRule eder;
    /**
    * 当前处理的这行数据
    */
    private Object[] outputRow;
    
    /**
    * 获取批次标记的默认值 <br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public String batch(JSONObject field){
        //从变量中获取批次标记
        String batch = CommonUtil.getProp(eder.getKu(), "BATCH");
        return batch;
    }
    /**
    * 获取当前时间 <br/>
    * 默认返回公安部格式字符串，后期支持可通过扩展字段指定格式。
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public Object currentDate(JSONObject field){
        return DateUtil.getGabDate();
    }
    /**
    * 获取验证信息 <br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public Object validateInfo(JSONObject field){
        //暂时还没验证
        return null;
    }
    /**
    * 目标库表达式<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public String targetExp(JSONObject field){
        //TODO jingma:后期支持
        return null;
    }
    /**
    * 目标库方法调用<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public String targetFunction(JSONObject field){
        //TODO jingma:后期支持
        return null;
    }
    /**
    * 自行搭建开发环境<br/>
    * 继承类：com.metl.rule.FieldDefaultValue，通过java开发自己的默认值器<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public String classExpand(JSONObject field){
        //TODO jingma:后期支持
        //将反射结果存入map，避免重复反射，提高性能
        return null;
    }

    /**
     * @return eder 
     */
    public ExecuteDataEtlRule getEder() {
        return eder;
    }

    /**
     * @param eder the eder to set
     */
    public void setEder(ExecuteDataEtlRule eder) {
        this.eder = eder;
    }
    /**
     * @return outputRow 
     */
    public Object[] getOutputRow() {
        return outputRow;
    }
    /**
     * @param outputRow the outputRow to set
     */
    public void setOutputRow(Object[] outputRow) {
        this.outputRow = outputRow;
    }
    
}
