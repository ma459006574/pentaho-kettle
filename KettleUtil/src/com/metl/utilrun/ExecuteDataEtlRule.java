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
    * 需要调用默认值方法设置默认值的字段
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
        //创建输出记录
        Object[] outputRow = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );
        //设置默认值
        setDefaultValue(outputRow);
        //执行转换规则
        executeTransition(outputRow);
        //去重规则处理
        disposeRepeatRule(outputRow);
        //校验数据
        validateData(outputRow);
        //将该记录设置到下一步骤的读取序列中
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
        //克隆输入记录的元数据
        data.outputRowMeta = (RowMetaInterface) ku.getInputRowMeta().clone();
        dataBill = CommonUtil.getPropJSONObject(ku, "DATA_BILL");
        dataTask = metldb.findOne("select * from metl_data_task dt where dt.ocode=?", 
                dataBill.getString("source_task"));
        targetObj = metldb.findOne("select * from metl_data_object do where do.ocode=?", 
                dataBill.getString("target_obj"));
        toFieldMap = metldb.findMap(Constants.FIELD_OID,"select * from metl_data_field df where df.data_object=?", 
                dataBill.getString("target_obj"));
        etlRuleConfig = metldb.findGeneralConfig("etl_rule_config");
        //获取默认配置
        if(etlRuleConfig.containsKey("rrFieldSpellSymbol")){
            rrFieldSpellSymbol = etlRuleConfig.getJSONObject("rrFieldSpellSymbol").
                    getString("value");
        }
        //可以配置去重字段拼接符
        if(configInfo.containsKey("rrFieldSpellSymbol")){
            rrFieldSpellSymbol = configInfo.getString("rrFieldSpellSymbol");
        }
        //将当前对象注入到默认值器中
        fdv.setEder(this);
        
        clearTarget();
        rrInit();
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
        rrField = metldb.findOne("select * from metl_data_field df where df.data_object=? and df.default_value=?", 
                dataBill.getString("target_obj"),Constants.DEFAULT_VAL_MD5_RR);
        //没有配置MD5字段
        if(rrField==null){
            return;
        }
        //去重字段的去重编号大于0则不需要采用生成md5的方式去重，但若要去重必须在数据任务管理处映射该去重字段
        if(rrField.getIntValue("rr_no")>0){
            return;
        }
        //已经有映射关系了，则不再生成MD5
        if(data.outputRowMeta.searchValueMeta(rrField.getString("ocode").
                toUpperCase())!=null){
            return;
        }
        //添加默认字段元数据
        getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
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
            index = getFieldIndex(rrMap.get(rrNo).getString(Constants.FIELD_OCODE).toUpperCase());
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
            //若该去重字段不在映射中，则跳过
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
        outputRow[getFieldIndex(rrField.getString(Constants.FIELD_OCODE).toUpperCase())]
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
        fdv.setOutputRow(outputRow);
        for(JSONObject def:defaultValueFields){
            try {
                //执行默认值方法并将结果赋值到记录中
                outputRow[getFieldIndex(def.getString(Constants.FIELD_OCODE))] = 
                        ruleMap.get(def.getString(Constants.FIELD_DEFAULT_VALUE)).invoke(fdv, def);
            } catch (Exception e) {
                ku.logError(def+"执行默认值规则失败", e);
            }
        }
    }
    
    /**
    * 主要就是新增了默认字段
    * @see com.metl.kettleutil.KettleUtilRunBase#getFields(org.pentaho.di.core.row.RowMetaInterface, java.lang.String, org.pentaho.di.core.row.RowMetaInterface[], org.pentaho.di.trans.step.StepMeta, org.pentaho.di.core.variables.VariableSpace)
    */
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        if(StringUtil.isBlank(CommonUtil.getProp(space, "DATA_BILL"))){
            return;
        }
        dataBill = CommonUtil.getPropJSONObject(space, "DATA_BILL");
        //查询目标对象有默认值的字段
        List<JSONObject> defFields = metldb.findList(
                "select * from metl_data_field df where df.data_object=? and df.default_value is not null", 
                dataBill.getString("target_obj"));
        Map<String, JSONObject> tfmMap = metldb.findMap("target_field", 
                "select * from metl_tf_mapping tfm where tfm.data_task=?", 
                dataBill.getString("source_task"));
        defaultValueFields.clear();
        String fieldName = null;
        for(JSONObject def:defFields){
            //若配置类映射关系,直接跳过
            if(tfmMap.containsKey(def.getString(Constants.FIELD_OID))){
                continue;
            }
            fieldName =def.getString(Constants.FIELD_OCODE).toUpperCase();
            //没有直接映射关系，当采用临时库表达式默认值时，就会出现前端没有配置映射关系，但这里有映射关系
            if(r.searchValueMeta(fieldName)==null){
                addField(r,fieldName,
                        dataTypeToKettleType(def.getString("data_type")),
                        ValueMeta.TRIM_TYPE_NONE,
                        origin,def.getString(Constants.FIELD_ONAME),
                        def.getIntValue("field_length"));
                if(Constants.DEFAULT_VAL_NOT_METHOD.contains(
                        def.getString(Constants.FIELD_DEFAULT_VALUE))){
                    continue;
                }
                defaultValueFields.add(def);
                try {
                    //如果该规则对应的方法已经存在则跳过
                    if(ruleMap.containsKey(def.getString(Constants.FIELD_DEFAULT_VALUE))){
                        continue;
                    }
                    String defRule = StringUtil.underlineTohump(
                            def.getString(Constants.FIELD_DEFAULT_VALUE));
                    Method method = FieldDefaultValue.class.
                            getMethod(defRule,JSONObject.class);
                    ruleMap.put(def.getString(Constants.FIELD_DEFAULT_VALUE), method);
                } catch (Exception e) {
                    ku.logError(def+"获取默认值规则的方法失败", e);
                }
            }
        }
    }
}
