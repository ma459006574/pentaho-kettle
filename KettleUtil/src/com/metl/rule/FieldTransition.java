/**
* Project Name:KettleUtil
* Date:2016年7月12日下午3:24:29
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.rule;

import java.util.Date;

import net.oschina.mytuils.DateUtil;
import net.oschina.mytuils.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.utilrun.ExecuteDataEtlRule;

/**
 * 字段转换规则 <br/>
 * date: 2016年7月12日 下午3:24:29 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class FieldTransition {
    /**
    * 执行数据对象定义的验证转换默认值等规则 
    */
    private ExecuteDataEtlRule eder;
    /**
    * 当前处理的这行数据
    */
    private Object[] outputRow;
    
    /**
    * 时间转换<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 转换结果
    */
    public Object dateTransition(JSONObject field){
        Object result = null;
        Object value = outputRow[eder.getFieldIndex(field.getString(Constants.FIELD_OCODE))];
        if(value==null){
            return result;
        }
        //这些信息都应该初始化时处理好，提高性能
        JSONObject expand = JSON.parseObject(field.getString(Constants.FIELD_EXPAND ));
        String format = null;
        try {
            //不一定有该项配置
            format = expand.getJSONObject(Constants.FIELD_TRANSITION).getString(
                    Constants.FIELD_TRANSITION_RULE_DATE_TRANSITION);
        } catch (NullPointerException e) {
            eder.getKu().logDetailed("获取转换时间格式失败", e);
        }
        if(StringUtil.isNotBlank(format)){
            //要求转换为指定时间格式
            Date val = DateUtil.parseDate(value.toString());
            if(val == null){
                //没有成功解析为时间的则保留原始值
                result = value;
            }else if("date".equals(format)){
                //指定的格式是：date时则返回dete对象
                result = val;
            }else{
                result = DateUtil.doFormatDate(val,format);
            }
        }else{
            //默认转换为14位字符串时间
            String val = DateUtil.dateToStr14(value);
            if(val == null){
                //没有成功解析为时间的则保留原始值
                result = value;
            }else{
                result = val;
            }
        }
        return result;
    }
    /**
    * 身份证转换<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 转换结果
    */
    public String idcordTransition(JSONObject field){
        //TODO jingma:后续开发
        String result = null;
        Object value = outputRow[eder.getFieldIndex(field.getString(Constants.FIELD_OCODE))];
        if(value==null){
            return result;
        }
        return result;
    }
    /**
    * 移除字符串空白部分<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 转换结果
    */
    public Object trimBlank(JSONObject field){
        Object result = outputRow[eder.getFieldIndex(field.getString(Constants.FIELD_OCODE))];
        if(result==null){
            return result;
        }
        return result.toString().trim();
    }
    /**
    * 字典翻译<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 转换结果
    */
    public Object dictTransition(JSONObject field){
        Object result = outputRow[eder.getFieldIndex(field.getString(Constants.FIELD_OCODE))];
        if(result==null){
            return result;
        }
        return result;
    }
    /**
    * 目标库表达式<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public Object targetExp(JSONObject field){
        //TODO jingma:后期支持
        //需要替换表达式中的变量，传入表达式需要的数据
        return null;
    }
    /**
    * 自行搭建开发环境<br/>
    * 继承类：com.metl.rule.FieldDefaultValue，通过java开发自己的默认值器<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public Object classExpand(JSONObject field){
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
