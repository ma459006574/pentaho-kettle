/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.utilrun;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson.JSONObject;
import com.metl.kettleutil.KettleUtilRunBase;
import com.metl.rule.FieldDefaultValue;
import com.metl.util.StringUtil;

/**
 * 执行数据对象定义的验证转换默认值等规则 <br/>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteDataEtlRule extends KettleUtilRunBase{
    /**
    * 需要设置默认值的字段
    */
    private List<JSONObject> defaultValueFields = new ArrayList<JSONObject>();
    /**
    * 规则与方法的映射
    */
    private Map<String,Method> ruleMap = new HashMap<String, Method>();
    /**
    * 字段默认值规则
    */
    private FieldDefaultValue fdv = new FieldDefaultValue();
    /**
    * 开始获取并执行数据账单中的任务 <br/>
    * @author jingma@iflytek.com
    * @return 
    * @throws KettleException 
    */
    public boolean run() throws KettleException{
        Object[] r = ku.getRow(); // get row, blocks when needed!
        if (r == null) // no more input to be expected...
        {
            ku.setOutputDone();
            return false;
        }
        if (ku.first) {
            ku.first = false;
            data.outputRowMeta = (RowMetaInterface) ku.getInputRowMeta().clone();
            getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
        }
        Object[] outputRow = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );
        
        setDefaultValue(outputRow);
        
        ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        return true;
    }

    /**
    * 设置默认值 <br/>
    * @author jingma@iflytek.com
    * @param outputRow
    */
    public void setDefaultValue(Object[] outputRow) {
        fdv.setEder(this);
        for(JSONObject dvf:defaultValueFields){
            try {
                //执行默认值方法并将结果赋值到记录中
                outputRow[getFieldIndex(dvf.getString("ocode"))] = 
                        ruleMap.get(dvf.getString("default_value")).invoke(fdv, dvf);
            } catch (Exception e) {
                ku.logError(dvf+"执行默认值规则失败", e);
            }
        }
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //查询有默认值的字段
        List<JSONObject> defFields = metldb.findList(
                "select * from metl_data_field df where df.data_object=? and df.default_value is not null", 
                configInfo.getString("targetObj"));
        defaultValueFields.clear();
        for(JSONObject def:defFields){
            //没有直接映射关系
            if(r.searchValueMeta(def.getString("ocode").toUpperCase())==null){
                defaultValueFields.add(def);
                try {
                    String defRule = StringUtil.underlineTohump(def.getString("default_value"));
                    Method method = FieldDefaultValue.class.
                            getMethod(defRule,JSONObject.class);
                    ruleMap.put(def.getString("default_value"), method);
                } catch (Exception e) {
                    ku.logError(def+"获取默认值规则的方法失败", e);
                }
                addField(r,def.getString("ocode").toUpperCase(),
                        dataTypeToKettleType(def.getString("data_type")),ValueMeta.TRIM_TYPE_NONE,origin);
            }
        }
    }
}
