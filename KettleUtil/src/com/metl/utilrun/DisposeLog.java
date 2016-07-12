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
import com.metl.util.CommonUtil;
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
        
        outputRow[getFieldIndex("INSERT_NUM")] = StringUtil.parseLong(CommonUtil.getProp(ku, "INSERT_NUM"));
        outputRow[getFieldIndex("UPDATE_NUM")] = StringUtil.parseLong(CommonUtil.getProp(ku, "UPDATE_NUM"));
        outputRow[getFieldIndex("INPUT_NUM")] = StringUtil.parseLong(CommonUtil.getProp(ku, "INPUT_NUM"));
        outputRow[getFieldIndex("DELETE_NUM")] = StringUtil.parseLong(CommonUtil.getProp(ku, "DELETE_NUM"));
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
        addField(r,"INSERT_NUM",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin);
        addField(r,"UPDATE_NUM",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin);
        addField(r,"INPUT_NUM",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin);
        addField(r,"DELETE_NUM",ValueMeta.TYPE_INTEGER,ValueMeta.TRIM_TYPE_NONE,origin);
        addField(r,"END_TIME",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
        addField(r,"STATE",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
    }
}
