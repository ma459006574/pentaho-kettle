/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

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
        
        String batch = CommonUtil.getProp(ku, "BATCH");
        outputRow[getFieldIndex("BATCH")] = batch;
        
        ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        return true;
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        addField(r,"BATCH",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin);
    }
}
