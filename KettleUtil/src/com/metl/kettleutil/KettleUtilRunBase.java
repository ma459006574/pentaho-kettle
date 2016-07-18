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
import com.metl.constants.Constants;
import com.metl.db.Db;

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
    * 添加字段 <br/>
    * @author jingma@iflytek.com
    * @param r 行
    * @param name 字段名称
    * @param type 类型
    * @param trimType 去除空白规则
    * @param origin 宿主
    * @param comments 描述
    */
    protected void addField(RowMetaInterface r, String name, int type,
            int trimType, String origin, String comments) {
        addField(r, name, type, trimType, origin,comments, 0);
    }
    /**
    * 添加字段 <br/>
    * @author jingma@iflytek.com
    * @param r 行
    * @param name 字段名称
    * @param type 类型
    * @param trimType 去除空白规则
    * @param origin 宿主
    * @param comments 描述
    * @param length 长度
    */
    @SuppressWarnings("deprecation")
    protected void addField(RowMetaInterface r, String name, int type,
            int trimType, String origin, String comments, int length) {
        ValueMetaInterface v = new ValueMeta();
        v.setName(name);
        v.setType(type);
        v.setTrimType(trimType);
        v.setOrigin(origin);
        v.setComments(comments);
        if(length>0){
            v.setLength(length);
        }
        r.addValueMeta(v);
    }

    /**
    * metl数据类型转换为kettle的数据类型 <br/>
    * @author jingma@iflytek.com
    * @param dataType metl数据类型
    * @return kettle的数据类型
    */
    protected int dataTypeToKettleType(String dataType) {
        if(Constants.DATA_TYPE_STRING.equals(dataType)){
            return ValueMeta.TYPE_STRING;
        }else if(Constants.DATA_TYPE_NUMBER.equals(dataType)){
            return ValueMeta.TYPE_INTEGER;
        }else if(Constants.DATA_TYPE_DATE.equals(dataType)){
            return ValueMeta.TYPE_DATE;
        }else if(Constants.DATA_TYPE_BLOB.equals(dataType)){
            return ValueMeta.TYPE_BINARY;
        }else if(Constants.DATA_TYPE_CLOB.equals(dataType)){
            return ValueMeta.TYPE_STRING;
        }
        return 0;
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
     * @param space 
     */
    public void setMeta(KettleUtilMeta meta, VariableSpace space) {
        this.meta = meta;
        //将配置信息解析成josn对象,支持变量
        if(StringUtils.isNotBlank(meta.getConfigInfo())){
            setConfigInfo(JSON.parseObject(space.environmentSubstitute(meta.getConfigInfo())));
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
