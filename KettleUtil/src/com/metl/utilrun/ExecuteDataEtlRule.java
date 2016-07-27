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

import net.oschina.mytuils.DateUtil;
import net.oschina.mytuils.KettleUtils;
import net.oschina.mytuils.MD5Util;
import net.oschina.mytuils.StringUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.metl.kettleutil.util.Dict;
import com.metl.rule.FieldDefaultValue;
import com.metl.rule.FieldTransition;
import com.metl.rule.FieldValidate;

/**
 * 执行数据对象定义的验证转换默认值等规则 <br/>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteDataEtlRule extends KettleUtilRunBase{
    /**
    * 默认规则与方法的映射
    */
    private static Map<String,Method> defaultRuleMap = new HashMap<String, Method>();
    /**
    * 字段验证规则与方法的映射
    */
    private static Map<String,Method> validateRuleMap = new HashMap<String, Method>();
    /**
    * 字段转换规则与方法的映射
    */
    private static Map<String,Method> transitionRuleMap = new HashMap<String, Method>();
    /**
    * 日志
    */
    private static Log log = LogFactory.getLog(ExecuteDataEtlRule.class);
    /**
    * 去重字段拼接符
    */
    private String rrFieldSpellSymbol = "-";
    /**
    * 需要调用默认值方法设置默认值的字段
    */
    private List<JSONObject> defaultValueFields = new ArrayList<JSONObject>();
    /**
    * 需要调用转换方法的字段
    */
    private List<JSONObject> transitionFields = new ArrayList<JSONObject>();
    /**
    * 需要调用验证方法的字段
    */
    private List<JSONObject> validateFields = new ArrayList<JSONObject>();
    /**
    * 字段默认值规则
    */
    private FieldDefaultValue fdv = new FieldDefaultValue();
    /**
    * 字段转换规则
    */
    private FieldTransition ft = new FieldTransition();
    /**
    * 字段验证规则
    */
    private FieldValidate fv = new FieldValidate();
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
    * 默认规则对应的字段：<规则,字段>
    */
    private Map<String,JSONObject> ruleFieldMap = new HashMap<String, JSONObject>();
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
    * 转换中的错误信息
    */
    private JSONObject transitionErrorInfo;
    /**
    * 无效记录数
    */
    private Integer invalidCount = 0;
    static{
    }
    /**
    * 将各种规则的处理方法先反射好 <br/>
    * @author jingma@iflytek.com
    */
    public static void initMethod() {
        List<JSONObject> defaultRules = Dict.dictList(
                Constants.DICT_CATEGORY_DEFAULT_RULE);
        List<JSONObject> transitionRules = Dict.dictList(
                Constants.DICT_CATEGORY_FIELD_TRANSITION_RULE);
        List<JSONObject> validateRules = Dict.dictList(
                Constants.DICT_CATEGORY_FIELD_VALIDATE_RULE);
        String rule = null;
        for(JSONObject def:defaultRules){
            if(Constants.DEFAULT_VAL_NOT_METHOD.contains(
                    def.getString(Constants.FIELD_ID))){
                continue;
            }
            try {
                rule = StringUtil.underlineTohump(
                        def.getString(Constants.FIELD_ID));
                Method method = FieldDefaultValue.class.
                        getMethod(rule,JSONObject.class);
                defaultRuleMap.put(def.getString(Constants.FIELD_ID), method);
            } catch (Exception e) {
                log.error(rule+"反射获取方法失败："+def, e);
            }
        }
        for(JSONObject tr:transitionRules){
            if(Constants.FIELD_TRANSITION_RULE_NOT_METHOD.contains(
                    tr.getString(Constants.FIELD_ID))){
                continue;
            }
            try {
                rule = StringUtil.underlineTohump(
                        tr.getString(Constants.FIELD_ID));
                Method method = FieldTransition.class.
                        getMethod(rule,JSONObject.class);
                transitionRuleMap.put(tr.getString(Constants.FIELD_ID), method);
            } catch (Exception e) {
                log.error(rule+"反射获取方法失败："+tr, e);
            }
        }
        for(JSONObject vr:validateRules){
            if(Constants.FIELD_VALIDATE_RULE_NOT_METHOD.contains(
                    vr.getString(Constants.FIELD_ID))){
                continue;
            }
            try {
                rule = StringUtil.underlineTohump(
                        vr.getString(Constants.FIELD_ID));
                Method method = FieldValidate.class.
                        getMethod(rule,JSONObject.class);
                validateRuleMap.put(vr.getString(Constants.FIELD_ID), method);
            } catch (Exception e) {
                log.error(rule+"反射获取方法失败："+vr, e);
            }
        }
    }
    /**
     * Creates a new instance of ExecuteDataEtlRule.
     */
    public ExecuteDataEtlRule() {
    }
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
            return end();
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
    public boolean end() {
        ku.setOutputDone();
        //设置无效记录数据变量，以备后续记录日志
        setVariableRootJob("INVALID_COUNT", invalidCount.toString());
        return false;
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
        initMethod();
        //克隆输入记录的元数据
        data.outputRowMeta = (RowMetaInterface) ku.getInputRowMeta().clone();
        dataBill = KettleUtils.getPropJSONObject(ku, "DATA_BILL");
        dataTask = metldb.findFirst("select * from metl_data_task dt where dt.ocode=?", 
                dataBill.getString("source_task"));
        targetObj = metldb.findFirst("select * from metl_data_object t where t.ocode=?", 
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
        //添加默认字段元数据
        getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
        //将当前对象注入到各种规则执行器中
        fdv.setEder(this);
        ft.setEder(this);
        fv.setEder(this);
        rrInit();
        initTransition();
        initValidate();
        
        clearTarget();
    }

    /**
    * 初始化验证规则 <br/>
    * @author jingma@iflytek.com
    */
    private void initValidate() {
        //查询具有验证规则的字段,必填、主键、配置了验证规则的
        List<JSONObject> fields = metldb.find(
                "select * from metl_data_field df where df.data_object=? "
                + "and (df.validator is not null or df.is_required=? or df.is_pk=?)", 
                dataBill.getString("target_obj"),Constants.WHETHER_TRUE,
                Constants.WHETHER_TRUE);
        //需要考虑与字段映射、默认值等的关系
        for(JSONObject field:fields){
            //不存在映射关系的字段，若是必填字段或是主键，在必填验证就会失败，不进行后面的验证。
            //否则会进行后面的验证，所有，本系统的验证规则为空时都会通过验证。
            validateFields.add(field);
        }
    }

    /**
    * 初始化转换规则 <br/>
    * @author jingma@iflytek.com
    */
    private void initTransition() {
        //查询具有转换规则的字段
        List<JSONObject> fields = metldb.find(
                "select * from metl_data_field df where df.data_object=? "
                + "and df.transition is not null", 
                dataBill.getString("target_obj"));
        //需要考虑与字段映射、默认值等的关系
        for(JSONObject field:fields){
            //不存在映射关系的字段
            if(getFieldIndex(field.getString(Constants.FIELD_OCODE).toUpperCase())==-1){
                continue;
            }
            transitionFields.add(field);
        }
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
        JSONObject rrField = ruleFieldMap.get(Constants.DEFAULT_VAL_MD5_RR);
        //没有配置MD5字段
        if(rrField==null){
            return;
        }
        //去重字段的去重编号大于0则不需要采用生成md5的方式去重，但若要去重必须在数据任务管理处映射该去重字段
        if(rrField.getIntValue("rr_no")>0){
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
            index = getFieldIndex(rrMap.get(rrNo).getString(Constants.FIELD_OCODE).toUpperCase());
            rrFieldIndexMap.put(index, rrMap.get(rrNo));
            rrFieldIndexList.add(index);
        }
        isRr = true;
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
                //数据类型是date,虽然前面有各种转换规则，但目标对象是用户自己定义的，
                //一切转换都要为目标服务，最终肯定要转换为目标对象中这些字段对应的数据类型
                if(Constants.DATA_TYPE_DATE.equals(
                        rrFieldIndexMap.get(rrFieldIndex).getString("data_type"))){
                    tempStr.append(DateUtil.dateToStr14(outputRow[rrFieldIndex]));
                }else{
                    tempStr.append(outputRow[rrFieldIndex]);
                }
            }
            tempStr.append(rrFieldSpellSymbol);
        }
        outputRow[getFieldIndex(
                ruleFieldMap.get(Constants.DEFAULT_VAL_MD5_RR).
                getString(Constants.FIELD_OCODE))]
                = MD5Util.encode(tempStr.toString());
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
        Db targetDb = Db.use(ku, targetObj.getString("database"));
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
        fv.setOutputRow(outputRow);
        //获取验证字段的下标
        int idx = getFieldIndex(
                ruleFieldMap.get(Constants.DEFAULT_VAL_VALIDATE_INFO).
                getString(Constants.FIELD_OCODE));
        //存最终验证结果
        JSONObject validateResult = new JSONObject();
        if(transitionErrorInfo.size()>0){
            validateResult.put(Constants.FIELD_TRANSITION, transitionErrorInfo);
        }
        JSONObject validateInfo = new JSONObject();
        Object vi = null;
        //已经在临时库产生了验证结果
        if(outputRow[idx] != null){
            //若配置了临时库表达式验证，结果会存入验证字段
            String[] fvs = outputRow[idx].toString().split(Constants.TSLJF1);
            for(String fv:fvs){
                //具体字段验证结果
                String[] vr = fv.split(Constants.TSLJF2);
                if(vr.length==2){
                    //产生了信息
                    validateInfo.put(vr[0], vr[1]);
                }
            }
            
        }
        //若临时库验证没有通过，则不再进行这里的验证了
        if(validateInfo.size()==0){
            for(JSONObject f:validateFields){
                try {
                    //执行默认值方法并将结果赋值到记录中
                    String[] fvs = f.getString(Constants.FIELD_VALIDATOR).split(",");
                    for(String fvr:fvs){
                        if(Constants.FIELD_VALIDATE_RULE_NOT_METHOD.contains(fvr)){
                            continue;
                        }
                        vi = validateRuleMap.get(fvr).invoke(fv, f);
                        if(vi != null){
                            validateInfo.put(f.getString(Constants.FIELD_OCODE), vi);
                            //一个规则失败，则不再继续验证
                            break;
                        }
                    }
                } catch (Exception e) {
                    ku.logError(f+"执行验证规则失败", e);
                    validateInfo.put(f.getString(Constants.FIELD_OCODE), "执行验证"
                    +f.getString(Constants.FIELD_VALIDATOR)+"失败："+e.getMessage());
                }
            }
        }
        //处理验证结果
        if(validateInfo.size()>0){
            validateResult.put(Constants.FIELD_VALIDATOR, validateInfo);
        }
        if(validateResult.size()>0){
            //任务要求执行验证规则，则必须存在验证字段
            outputRow[idx] = validateResult.toJSONString();
            //新增一条无效记录
            invalidCount++;
        }
        
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
        ft.setOutputRow(outputRow);
        transitionErrorInfo = new JSONObject();
        for(JSONObject f:transitionFields){
            try {
                //执行转换方法并将结果赋值到记录中，一个字段可以有多个转换器，按顺序执行
                String[] fts = f.getString(Constants.FIELD_TRANSITION).split(",");
                for(String ftr:fts){
                    //不需要执行转换方法的转换规则
                    if(Constants.FIELD_TRANSITION_RULE_NOT_METHOD.contains(ftr)){
                        continue;
                    }
                    outputRow[getFieldIndex(f.getString(Constants.FIELD_OCODE))] = 
                            transitionRuleMap.get(ftr).invoke(ft, f);
                }
            } catch (Exception e) {
                ku.logError(f+"执行转换规则失败", e);
                transitionErrorInfo.put(f.getString(Constants.FIELD_OCODE), "执行转换"
                +f.getString(Constants.FIELD_TRANSITION)+"失败："+e.getMessage());
            }
        }
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
                        defaultRuleMap.get(def.getString(Constants.FIELD_DEFAULT_VALUE)).
                        invoke(fdv, def);
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
        if(StringUtil.isBlank(KettleUtils.getProp(space, "DATA_BILL"))){
            return;
        }
        dataBill = KettleUtils.getPropJSONObject(space, "DATA_BILL");
        //查询目标对象有默认值的字段
        List<JSONObject> defFields = metldb.find(
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
            if(Constants.DEFAULT_VAL_MD5_RR.equals(def.
                    getString(Constants.FIELD_DEFAULT_VALUE))){
                ruleFieldMap.put(Constants.DEFAULT_VAL_MD5_RR, def);
            }else if(Constants.DEFAULT_VAL_VALIDATE_INFO.equals(def.
                    getString(Constants.FIELD_DEFAULT_VALUE))){
                ruleFieldMap.put(Constants.DEFAULT_VAL_VALIDATE_INFO, def);
            }
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
            }
        }
    }
}
