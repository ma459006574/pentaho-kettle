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
        String dataBillOid = CommonUtil.getProp(ku,"DATA_BILL_OID");
        //若数据账单为空则生成随机uuid值
        if(StringUtil.isBlank(dataBillOid)){
            throw new RuntimeException("数据账单主键(DATA_BILL_OID)参数不能为空");
//            dataBill = StringUtil.getUUIDUpperStr();
        }
        JSONObject dataBill = metldb.findOne("select * from metl_data_bill db where db.oid=?", dataBillOid);
        //将数据账单主键作为批次标记
        outputRow[getFieldIndex("BATCH")] = dataBillOid;
        outputRow[getFieldIndex("DATA_BILL")] = dataBill.toJSONString();
        outputRow[getFieldIndex("JOB_XDLJ")] = dataBill.getString("source_task");
        outputRow[getFieldIndex("SHARD_START")] = dataBill.getString("shard_start");
        outputRow[getFieldIndex("SHARD_END")] = dataBill.getString("shard_end");
        outputRow[getFieldIndex("JOB_NAME")] = CommonUtil.getRootJobName(ku);
        outputRow[getFieldIndex("ID_JOB")] = CommonUtil.getRootJobId(ku);
        outputRow[getFieldIndex("START_TIME")] = DateUtil.getGabDate();
        //设置临时表变量
        String tempTable = CommonUtil.getProp(ku,"TEMP_TABLE");
        //临时表变量为空
        if(StringUtil.isBlank(tempTable)){
            String sourceObj = dataBill.getString("source_obj").toUpperCase();
            if(StringUtil.isNotBlank(sourceObj)){
                tempTable = "TEMP_"+sourceObj;
            }
        }
        outputRow[getFieldIndex("TEMP_TABLE")] = tempTable;
        
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
        addField(r,"TEMP_TABLE",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"临时表");
        addField(r,"DATA_BILL",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"数据账单对象");
        addField(r,"JOB_XDLJ",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"JOB相对路径");
        addField(r,"SHARD_START",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"数据片开始");
        addField(r,"SHARD_END",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"数据片结束");
    }
}
