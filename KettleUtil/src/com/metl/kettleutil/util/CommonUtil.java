/**
* Project Name:KettleUtil
* Date:2016年6月21日上午12:10:20
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.kettleutil.util;

import net.oschina.mytuils.KettleUtils;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;

/**
 * 一般工具类 <br/>
 * date: 2016年6月21日 上午12:10:20 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class CommonUtil {

    /**
    * metl中的数据库类型转换为kettle中的数据库类型 <br/>
    * @author jingma@iflytek.com
    * @param metlDs
    * @return
    */
    public static String metlDsToKettleDs(String metlDs) {
        if(Constants.DS_TYPE_ORACLE.equals(metlDs)){
            return "Oracle";
        }else if(Constants.DS_TYPE_MYSQL.equals(metlDs)){
            return "mysql";
        }
        return null;
    }
    
    /**
    * 获取或创建指定代码的数据库 <br/>
    * 若不存在则自动根据metl系统配置在kettle中创建该数据库
    * @author jingma@iflytek.com
    * @param dbCode 数据代码
    * @return 
    * @throws KettleException 
    */
    public static DatabaseMeta getOrCreateDB(String dbCode) throws KettleException {
        ObjectId dbId = null;
        Repository repository = KettleUtils.getInstanceRep();
        dbId = repository.getDatabaseID(dbCode);
        if(dbId!=null){
            return repository.loadDatabaseMeta(dbId, null);
        }else{
            JSONObject metlDb = Db.use(Constants.DATASOURCE_METL).
                    findFirst("select * from metl_database db where db.ocode=?", dbCode);
            DatabaseMeta dataMeta = new DatabaseMeta(dbCode, CommonUtil.metlDsToKettleDs(metlDb.getString("type")), 
                    "JNDI", null, dbCode, null, null, null);
            //保存转换时会进行该数据库的保存的
            //repository.saveDatabaseMetaStepAttribute(id_transformation, id_step, dbCode, dataMeta);
            return dataMeta;
        }
    }
}
