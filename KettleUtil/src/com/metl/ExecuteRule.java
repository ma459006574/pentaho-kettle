/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.TransformClassBase;
import org.pentaho.di.trans.steps.userdefinedjavaclass.TransformClassBase.Fields;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClass;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassData;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;

import com.metl.constants.Constants;
import com.metl.db.Db;
import com.metl.util.CommonUtil;

/**
 * 执行数据对象定义的验证转换默认值等规则 <br/>
 * 运用<pre>
import com.metl.ExecuteRule;
public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
{
    ExecuteRule er = new ExecuteRule(this,parent,smi,sdi);
    return er.run();
}</pre>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteRule {
    /**
    * 用户自定义java类控件
    */
    private UserDefinedJavaClass udjc;
    /**
    * 项目基础数据库操作对象
    */
    private Db metldb;
    private UserDefinedJavaClassMeta meta;
    private UserDefinedJavaClassData data;
    private TransformClassBase tcb;
    /**
     * Creates a new instance of ExecuteRule.
     */
    public ExecuteRule() {
    }
    
    /**
    * Creates a new instance of ExecuteRule.<br/>
    * @param udjc
    * @param smi
    * @param sdi
    */
    public ExecuteRule(TransformClassBase tcb,UserDefinedJavaClass udjc,
            StepMetaInterface smi, StepDataInterface sdi) {
        super();
        this.tcb = tcb;
        this.udjc = udjc;
        this.meta = (UserDefinedJavaClassMeta) smi;
        this.data = (UserDefinedJavaClassData) sdi;
        metldb = Db.getDb(udjc, Constants.DATASOURCE_METL);
    }

    /**
    * 开始获取并执行数据账单中的任务 <br/>
    * @author jingma@iflytek.com
     * @return 
     * @throws KettleException 
    */
    public boolean run() throws KettleException{
        if (udjc.first) {
            udjc.first = false;
        }
        // 获取输入字段
        Object[] rIn = udjc.getRow();
        if (rIn == null) {
            udjc.setOutputDone();
            return false;
        }
        // 获取输出字段
        Object[] rOut = tcb.createOutputRow(rIn, data.outputRowMeta.size());

        String batch = CommonUtil.getProp(udjc, "BATCH");
        tcb.get(Fields.Out, "BATCH").setValue(rOut, batch);

        udjc.putRow(data.outputRowMeta, rOut);
        return true;
    }
    /**
     * @return udjc 
     */
    public UserDefinedJavaClass getUdjc() {
        return udjc;
    }

    /**
     * @param udjc the udjc to set
     */
    public void setUdjc(UserDefinedJavaClass udjc) {
        this.udjc = udjc;
    }

    /**
     * @return metldb 
     */
    public Db getMetldb() {
        return metldb;
    }

    /**
     * @param metldb the metldb to set
     */
    public void setMetldb(Db metldb) {
        this.metldb = metldb;
    }

    /**
     * @return meta 
     */
    public UserDefinedJavaClassMeta getMeta() {
        return meta;
    }

    /**
     * @param meta the meta to set
     */
    public void setMeta(UserDefinedJavaClassMeta meta) {
        this.meta = meta;
    }

    /**
     * @return data 
     */
    public UserDefinedJavaClassData getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(UserDefinedJavaClassData data) {
        this.data = data;
    }

    /**
     * @return tcb 
     */
    public TransformClassBase getTcb() {
        return tcb;
    }

    /**
     * @param tcb the tcb to set
     */
    public void setTcb(TransformClassBase tcb) {
        this.tcb = tcb;
    }

}
