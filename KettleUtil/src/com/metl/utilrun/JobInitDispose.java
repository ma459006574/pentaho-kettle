/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.utilrun;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.kettleutil.KettleUtilRunBase;
import com.metl.util.CommonUtil;
import com.metl.util.DateUtil;
import com.metl.util.StringUtil;

/**
 * job初始化操作 <br/>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class JobInitDispose extends KettleUtilRunBase{

    public boolean run() throws KettleException{
        //作为步骤起点
        if(ku.first){
            ku.first = false;
        }else{
            return false;
        }
        data.outputRowMeta = new RowMeta();
        getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
        //直接产生一条输出
        Object[] outputRow = new Object[data.outputRowMeta.size()];
        
        //获取数据账单主键
        String dataBillOid = CommonUtil.getProp(ku,Constants.KETTLE_PARAM_DATA_BILL_OID);
        //若数据账单为空则生成随机uuid值
        if(StringUtil.isBlank(dataBillOid)){
            throw new RuntimeException("数据账单主键(DATA_BILL_OID)参数不能为空");
//            dataBill = StringUtil.getUUIDUpperStr();
        }
        JSONObject dataBill = metldb.findOne("select * from metl_data_bill db where db.oid=?", dataBillOid);
        JSONObject addField = null;
        //将数据账单主键作为批次标记
        outputRow[getFieldIndex("BATCH")] = dataBillOid;
        outputRow[getFieldIndex("DATA_BILL")] = dataBill.toJSONString();
        outputRow[getFieldIndex("DATA_TASK")] = dataBill.getString("source_task");
        outputRow[getFieldIndex("JOB_XDLJ")] = dataBill.getString("source_task");
        //分片字段
        if(dataBill.getString("shard_field")!=null){
            addField = metldb.findOne("select * from metl_data_field df where df.oid=?", 
                    dataBill.getString("shard_field"));
            outputRow[getFieldIndex("SHARD_FIELD")] = addField.getString(Constants.FIELD_OCODE);
        }
        outputRow[getFieldIndex("SHARD_START")] = dataBill.getString("shard_start");
        outputRow[getFieldIndex("SHARD_END")] = dataBill.getString("shard_end");
        outputRow[getFieldIndex("JOB_NAME")] = CommonUtil.getRootJobName(ku);
        outputRow[getFieldIndex("ID_JOB")] = CommonUtil.getRootJobId(ku);
        outputRow[getFieldIndex("START_TIME")] = DateUtil.getGabDate();
        //设置临时表变量
        String tempTable = CommonUtil.getProp(ku,Constants.TEMP_TABLE);
        //临时表变量为空
        if(StringUtil.isBlank(tempTable)){
            String sourceTask = dataBill.getString("source_task").toUpperCase();
            if(StringUtil.isNotBlank(sourceTask)){
                //临时表名：TEMP_+来源对象代码
                tempTable = Constants.TEMP_+sourceTask;
            }
        }
        outputRow[getFieldIndex(Constants.TEMP_TABLE)] = tempTable;
        //设置kettle日志主键变量
        String kettleLogOid = CommonUtil.getProp(ku,Constants.KETTLE_PARAM_KETTLE_LOG_OID);
        //kettle日志主键变量为空
        if(StringUtil.isBlank(kettleLogOid)){
            kettleLogOid = StringUtil.getUUIDUpperStr();
        }
        outputRow[getFieldIndex(Constants.KETTLE_PARAM_KETTLE_LOG_OID)] = kettleLogOid;
        
        ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        //结束输出
        ku.setOutputDone();
        return true;
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        addField(r,"JOB_NAME",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"JOB名称");
        addField(r,"START_TIME",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"开始时间");
        addField(r,"ID_JOB",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"JOB主键");
        addField(r,"BATCH",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"批次标记");
        addField(r,Constants.TEMP_TABLE,ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"临时表");
        addField(r,"DATA_BILL",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"数据账单对象");
        addField(r,"DATA_TASK",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"数据任务");
        addField(r,"JOB_XDLJ",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"JOB相对路径");
        addField(r,"SHARD_FIELD",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"分片字段");
        addField(r,"SHARD_START",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"分片开始");
        addField(r,"SHARD_END",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"分片结束");
        addField(r,Constants.KETTLE_PARAM_KETTLE_LOG_OID,ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"kettle日志主键");
    }
}
