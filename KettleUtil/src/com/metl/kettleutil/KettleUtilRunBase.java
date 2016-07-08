/**
* Project Name:KettleUtil
* Date:2016年7月6日下午4:25:59
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.kettleutil;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metl.util.Constants;
import com.metl.util.Db;

/**
 *  <br/>
 * date: 2016年7月6日 下午4:25:59 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public abstract class KettleUtilRunBase {
    /**
    * kettleUtil控件
    */
    protected KettleUtil ku;
    /**
    * 项目基础数据库操作对象
    */
    protected Db metldb;
    protected KettleUtilMeta meta;
    protected KettleUtilData data;
    /**
    * 配置信息
    */
    protected JSONObject configInfo;

    /**
    * 获取本步骤的输出字段 <br/>
    * @author jingma@iflytek.com
    * @param r
    * @param origin
    * @param info
    * @param nextStep
    * @param space
    */
    public abstract void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space);

    /**
    * 开始处理每一行数据 <br/>
    * @author jingma@iflytek.com
    * @return 
    * @throws KettleException 
    */
    public abstract boolean run() throws KettleException;

    /**
    *  <br/>
    * @author jingma@iflytek.com
    * @param r
    * @param string
    * @param typeInteger
    * @param trimTypeNone
    * @param origin
    */
    @SuppressWarnings("deprecation")
    protected void addField(RowMetaInterface r, String name, int type,
            int trimType, String origin) {
        ValueMetaInterface v = new ValueMeta();
        v.setName(name);
        v.setType(type);
        v.setTrimType(trimType);
        v.setOrigin(origin);
        r.addValueMeta(v);
    }
    /**
    * 获取输出字段在数组中的下标 <br/>
    * @author jingma@iflytek.com
    * @param field 字段名称
    * @return 下标
    */
    public int getFieldIndex(String field){
        return data.outputRowMeta.indexOfValue(field);
    }
    /**
     * @return ku 
     */
    public KettleUtil getKu() {
        return ku;
    }

    /**
     * @param ku the ku to set
     */
    public void setKu(KettleUtil ku) {
        this.ku = ku;
        metldb = Db.getDb(ku, Constants.DATASOURCE_METL);
    }

    /**
     * @return meta 
     */
    public KettleUtilMeta getMeta() {
        return meta;
    }

    /**
     * @param meta the meta to set
     */
    public void setMeta(KettleUtilMeta meta) {
        this.meta = meta;
        //将配置信息解析成josn对象,支持变量
        if(StringUtils.isNotBlank(meta.getConfigInfo())){
            setConfigInfo(JSON.parseObject(ku.environmentSubstitute(meta.getConfigInfo())));
        }
    }

    /**
     * @return data 
     */
    public KettleUtilData getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(KettleUtilData data) {
        this.data = data;
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
     * @return configInfo 
     */
    public JSONObject getConfigInfo() {
        return configInfo;
    }

    /**
     * @param configInfo the configInfo to set
     */
    public void setConfigInfo(JSONObject configInfo) {
        this.configInfo = configInfo;
    }
}
