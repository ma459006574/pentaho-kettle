/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.utilrun;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson.JSONObject;
import com.metl.kettleutil.KettleUtilRunBase;
import com.metl.util.CommonUtil;

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
        for(JSONObject dvf:defaultValueFields){
            
        }
        String batch = CommonUtil.getProp(ku, "BATCH");
        outputRow[getFieldIndex("BATCH")] = batch;
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //查询有默认值的字段
        List<JSONObject> defFields = metldb.findList("select * from metl_data_field df where df.data_object=? and df.default_value is not null", configInfo.getString("targetObj"));
        defaultValueFields.clear();
        for(JSONObject def:defFields){
            //没有直接映射关系
            if(r.searchValueMeta(def.getString("ocode").toUpperCase())==null){
                defaultValueFields.add(def);
                addField(r,def.getString("ocode").toUpperCase(),
                        dataTypeToKettleType(def.getString("data_type")),ValueMeta.TRIM_TYPE_NONE,origin);
            }
        }
    }
}
