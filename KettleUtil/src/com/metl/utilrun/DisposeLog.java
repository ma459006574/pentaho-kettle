/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.utilrun;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

import com.metl.constants.Constants;
import com.metl.kettleutil.KettleUtilRunBase;
import com.metl.util.DateUtil;
import com.metl.util.StringUtil;

/**
 * 处理日志相关操作 <br/>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class DisposeLog extends KettleUtilRunBase{

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
        Object[] outputRow = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size());
        
        outputRow[getFieldIndex("INPUT_COUNT")] = StringUtil.parseLong(getVariavle("INPUT_COUNT"));
        outputRow[getFieldIndex("REPEAT_COUNT")] = StringUtil.parseLong(getVariavle("REPEAT_COUNT"));
        outputRow[getFieldIndex("ADD_COUNT")] = StringUtil.parseLong(getVariavle("ADD_COUNT"));
        outputRow[getFieldIndex("INVALID_COUNT")] = StringUtil.parseLong(getVariavle("INVALID_COUNT"));
        outputRow[getFieldIndex("END_TIME")] = DateUtil.getGabDate();
        String state = Constants.DATA_BILL_STATUS_INPUT_SUCCESS;
        if(!Constants.SUCCESS_FAILED_SUCCESS.equals(outputRow[getFieldIndex("RESULT")])){
            state = Constants.DATA_BILL_STATUS_INPUT_FAILED;
        }
        outputRow[getFieldIndex("STATE")] = state;
        
        ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        return true;
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        addField(r,"INPUT_COUNT",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin,"读取量");
        addField(r,"REPEAT_COUNT",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin,"重复量");
        addField(r,"ADD_COUNT",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin,"新增量");
        addField(r,"INVALID_COUNT",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin,"无效量");
        addField(r,"END_TIME",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"结束时间");
        addField(r,"STATE",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"数据账单运行状态");
    }
}
