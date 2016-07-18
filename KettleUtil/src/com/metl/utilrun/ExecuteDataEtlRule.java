/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.utilrun;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;
import com.metl.kettleutil.KettleUtilRunBase;
import com.metl.rule.FieldDefaultValue;
import com.metl.util.CommonUtil;
import com.metl.util.DateUtil;
import com.metl.util.MD5Util;
import com.metl.util.StringUtil;

/**
 * 执行数据对象定义的验证转换默认值等规则 <br/>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteDataEtlRule extends KettleUtilRunBase{
    /**
    * 去重字段拼接符
    */
    private String rrFieldSpellSymbol = "-";
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
    * 数据账单
    */
    private JSONObject dataBill;
    /**
    * 数据任务
    */
    private JSONObject dataTask;
    /**
    * 目标对象
    */
    private JSONObject targetObj;
    /**
    * 目标对象字段
    */
    private Map<String, JSONObject> toFieldMap;
    /**
    * 去重字段
    */
    private JSONObject rrField;
    /**
    * 去重字段下标顺序列表，-1表示该字段没有在输出字段中
    */
    private List<Integer> rrFieldIndexList = new ArrayList<Integer>();
    /**
    * 去重字段下标与字段的映射
    */
    private Map<Integer,JSONObject> rrFieldIndexMap = new HashMap<Integer, JSONObject>();
    /**
    * 是否需要做去重处理
    */
    private boolean isRr = false;
    /**
    * 在统一字典中的默认配置
    */
    private JSONObject etlRuleConfig;
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
        init();
        Object[] outputRow = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );
        //设置默认值
        setDefaultValue(outputRow);
        //执行转换规则
        executeTransition(outputRow);
        //去重规则处理
        disposeRepeatRule(outputRow);
        //校验数据
        validateData(outputRow);
        
        ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        return true;
    }

    /**
    * 初始化操作 <br/>
    * @author jingma@iflytek.com
    */
    public void init() {
        if (!ku.first) {
            return;
        }
        ku.first = false;
        data.outputRowMeta = (RowMetaInterface) ku.getInputRowMeta().clone();
        dataBill = CommonUtil.getPropJSONObject(ku, "DATA_BILL");
        dataTask = metldb.findOne("select * from metl_data_task dt where dt.ocode=?", 
                dataBill.getString("source_task"));
        targetObj = metldb.findOne("select * from metl_data_object do where do.ocode=?", 
                dataBill.getString("target_obj"));
        toFieldMap = metldb.findMap("oid","select * from metl_data_field df where df.data_object=?", 
                dataBill.getString("target_obj"));
        etlRuleConfig = JSON.parseObject(metldb.findOne("select expand from metl_unify_dict d where d.ocode=? and d.dict_category=?", 
                "ETL_RULE_CONFIG",Constants.DICT_CATEGORY_GENERAL_CONFIG).
                getString("expand"));
        //获取默认配置
        if(etlRuleConfig.containsKey("rrFieldSpellSymbol")){
            rrFieldSpellSymbol = etlRuleConfig.getJSONObject("rrFieldSpellSymbol").
                    getString("value");
        }
        //可以配置去重字段拼接符
        if(configInfo.containsKey("rrFieldSpellSymbol")){
            rrFieldSpellSymbol = configInfo.getString("rrFieldSpellSymbol");
        }
        
        clearTarget();
        rrInit();
        getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
    }

    /**
    * 去重操作初始化操作 <br/>
    * @author jingma@iflytek.com
    */
    public void rrInit() {
        //该任务配置不需要执行去重操作
        if(Constants.WHETHER_FALSE.equals(dataTask.getString("is_rr"))){
            return;
        }
        //去重字段
        rrField = toFieldMap.get(targetObj.getString("rr_field"));
        //去重字段的去重编号大于0则不需要采用生成md5的方式去重，但若要去重必须在数据任务管理处映射该去重字段
        if(rrField.getIntValue("rr_no")>0){
            return;
        }
        //已经有映射关系了，则不再生成MD5
        if(data.outputRowMeta.searchValueMeta(rrField.getString("oname"))!=null){
            return;
        }
        //采用组合去重字段生成md5的方式去重，必须在目标数据对象的字段配置好去重编号
        Map<Integer,JSONObject> rrMap = new HashMap<Integer, JSONObject>();
        for(JSONObject toField:toFieldMap.values()){
            if(toField.getIntValue("rr_no")>0){
                rrMap.put(toField.getInteger("rr_no"), toField);
            }
        }
        //去重编号不能重复，需要在前端控制好
        ArrayList<Integer> rrNoList = new ArrayList<Integer>(rrMap.keySet());
        Collections.sort(rrNoList);
        int index = -10;
        for(Integer rrNo:rrNoList){
            index = getFieldIndex(rrMap.get(rrNo).getString("ocode").toUpperCase());
            rrFieldIndexMap.put(index, rrMap.get(rrNo));
            rrFieldIndexList.add(index);
        }
        isRr = true;
    }

    /**
    * 执行清空目标表操作 <br/>
    * @author jingma@iflytek.com
    */
    private void clearTarget() {
        //该任务配置不需要执行清空目标表操作
        if(!Constants.WHETHER_TRUE.equals(dataTask.getString("is_clear_tt"))){
            return;
        }
        //如果目标对象不是表对象则跳过
        if(!Constants.DO_TYPE_TABLE.equals(targetObj.getString("do_type"))){
            return;
        }
        Db targetDb = Db.getDb(ku, targetObj.getString("database"));
        //换用delete方式，还有机会还原，一般使用该功能时，数据量都不应该很大
        //String deleteSql = "truncate table "+targetObj.getString("real_name");
        String deleteSql = "delete from "+targetObj.getString("real_name");
        ku.logError("清空目标表："+deleteSql);
        targetDb.execute(deleteSql);
    }

    /**
    * 执行校验操作 <br/>
    * @author jingma@iflytek.com
    * @param outputRow
    */
    private void validateData(Object[] outputRow) {
        //该任务配置不需要执行校验操作
        if(Constants.WHETHER_FALSE.equals(dataTask.getString("is_validate"))){
            return;
        }
    }

    /**
    * 处理去重规则 <br/>
    * @author jingma@iflytek.com
    * @param outputRow
    */
    private void disposeRepeatRule(Object[] outputRow) {
        if(!isRr){
            return;
        }
        StringBuffer tempStr = new StringBuffer();
        for(Integer rrFieldIndex:rrFieldIndexList){
            if(rrFieldIndex>-1){
                //数据类型是date
                if(Constants.DATA_TYPE_DATE.equals(
                        rrFieldIndexMap.get(rrFieldIndex).getString("data_type"))){
                    tempStr.append(DateUtil.processDate(outputRow[rrFieldIndex]));
                }else{
                    tempStr.append(outputRow[rrFieldIndex]);
                }
            }
            tempStr.append(rrFieldSpellSymbol);
        }
        outputRow[getFieldIndex(rrField.getString("ocode").toUpperCase())]
                = MD5Util.encode(tempStr.toString());
    }

    /**
    * 执行转换规则 <br/>
    * @author jingma@iflytek.com
    * @param outputRow
    */
    private void executeTransition(Object[] outputRow) {
        if(Constants.WHETHER_FALSE.equals(dataTask.getString("is_transition"))){
            return;
        }
        ku.logBasic("执行转换规则");
    }

    /**
    * 设置默认值 <br/>
    * @author jingma@iflytek.com
    * @param outputRow
    */
    private void setDefaultValue(Object[] outputRow) {
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
    
    /**
    * 主要就是新增了默认字段
    * @see com.metl.kettleutil.KettleUtilRunBase#getFields(org.pentaho.di.core.row.RowMetaInterface, java.lang.String, org.pentaho.di.core.row.RowMetaInterface[], org.pentaho.di.trans.step.StepMeta, org.pentaho.di.core.variables.VariableSpace)
    */
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //查询有默认值的字段
        List<JSONObject> defFields = metldb.findList(
                "select * from metl_data_field df where df.data_object=? and df.default_value is not null", 
                CommonUtil.getPropJSONObject(space, "DATA_BILL").getString("target_obj"));
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
                        dataTypeToKettleType(def.getString("data_type")),ValueMeta.TRIM_TYPE_NONE,
                        origin,def.getString("oname"),def.getIntValue("field_length"));
            }
        }
    }
}
