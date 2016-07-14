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
        String dataBill = CommonUtil.getProp(ku,"DATA_BILL");
        //若数据账单为空则生成随机uuid值
        if(StringUtil.isBlank(dataBill)){
            dataBill = StringUtil.getUUIDUpperStr();
        }
        //将数据账单主键作为批次标记
        outputRow[getFieldIndex("BATCH")] = dataBill;
        outputRow[getFieldIndex("JOB_NAME")] = CommonUtil.getRootJobName(ku);
        outputRow[getFieldIndex("ID_JOB")] = CommonUtil.getRootJobId(ku);
        outputRow[getFieldIndex("START_TIME")] = DateUtil.getGabDate();
        //设置临时表变量
        String tempTable = CommonUtil.getProp(ku,"TEMP_TABLE");
        //临时表变量为空
        if(StringUtil.isBlank(tempTable)){
            String targetTable = CommonUtil.getProp(ku,"TARGET_TABLE");
            if(StringUtil.isNotBlank(targetTable)){
                tempTable = "TEMP_"+targetTable;
            }
        }
        outputRow[getFieldIndex("TEMP_TABLE")] = tempTable;
        
        ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        //结束输出
        ku.setOutputDone();
        return true;
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        addField(r,"JOB_NAME",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
        addField(r,"START_TIME",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
        addField(r,"ID_JOB",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
        addField(r,"BATCH",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
        addField(r,"TEMP_TABLE",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
    }
}
