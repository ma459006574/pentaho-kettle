/**
* Project Name:KettleUtil
* Date:2016年7月12日下午3:24:29
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.rule;

import java.text.ParseException;

import net.oschina.mytuils.DateUtil;
import net.oschina.mytuils.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.utilrun.ExecuteDataEtlRule;

/**
 * 字段验证规则 <br/>
 * date: 2016年7月12日 下午3:24:29 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class FieldValidate {
    /**
    * 执行数据对象定义的验证转换默认值等规则 
    */
    private ExecuteDataEtlRule eder;
    /**
    * 当前处理的这行数据
    */
    private Object[] outputRow;
    
    /**
    * 时间格式验证 <br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 验证结果
    */
    public String dateValidate(JSONObject field){
        //验证结果
        String result = null;
        Object value = outputRow[eder.getFieldIndex(field.getString(Constants.FIELD_OCODE))];
        if(value==null){
            return result;
        }
        //这些信息都应该初始化时处理好，提高性能
        JSONObject expand = JSON.parseObject(field.getString(Constants.FIELD_EXPAND ));
        String format = null;
        try {
            //不一定有该项配置
            format = expand.getJSONObject(Constants.FIELD_VALIDATOR).getString(
                    Constants.FIELD_VALIDATE_RULE_DATE_VALIDATE);
        } catch (NullPointerException e) {
            eder.getKu().logDetailed("获取验证时间格式失败", e);
        }
        if(StringUtil.isNotBlank(format)){
            //要求验证为指定时间格式
            try {
                DateUtil.parseDate(value.toString(), new String[]{format});
            } catch (ParseException e) {
                result = value+"无法按指定格式解析："+format;
            }
        }else{
            //只要能正确解析为时间就通过
            if(DateUtil.dateToStr14(value)==null){
                result = value+"无法解析为时间";
            }
        }
        return result;
    }
    /**
    * 身份证格式验证 <br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 验证结果
    */
    public String idcordValidate(JSONObject field){
        //TODO jingma:后续开发
        //验证结果
        String result = null;
        return result;
    }
    /**
    * 目标库表达式<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public String targetExp(JSONObject field){
        //TODO jingma:后期支持
        //验证结果
        String result = null;
        //需要替换表达式中的变量，传入表达式需要的数据
        return result;
    }
    /**
    * 自行搭建开发环境<br/>
    * 继承类：com.metl.rule.FieldDefaultValue，通过java开发自己的默认值器<br/>
    * @author jingma@iflytek.com
    * @param field 要处理的字段在metl库中的信息
    * @return 得到的默认值
    */
    public String classExpand(JSONObject field){
        //验证结果
        String result = null;
        //TODO jingma:后期支持
        //将反射结果存入map，避免重复反射，提高性能
        return result;
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
