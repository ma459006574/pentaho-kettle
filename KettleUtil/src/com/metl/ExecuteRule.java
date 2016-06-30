/**
* Project Name:KettleUtil
* Date:2016年6月29日下午4:58:19
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl;

import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClass;

import com.metl.util.Constants;
import com.metl.util.Db;

/**
 * 执行数据对象定义的验证转换默认值等规则 <br/>
 * date: 2016年6月29日 下午4:58:19 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteRule {
    /**
    * javascript控件
    */
    private UserDefinedJavaClass udjc;
    /**
    * 项目基础数据库操作对象
    */
    private Db metldb;
    /**
     * Creates a new instance of ExecuteRule.
     */
    public ExecuteRule() {
    }
    
    public ExecuteRule(UserDefinedJavaClass udjc) {
        super();
        this.udjc = udjc;
        metldb = Db.getDb(udjc, Constants.DATASOURCE_METL);
    }

    /**
    * 开始获取并执行数据账单中的任务 <br/>
    * @author jingma@iflytek.com
    */
    public void run(){
        
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

}
