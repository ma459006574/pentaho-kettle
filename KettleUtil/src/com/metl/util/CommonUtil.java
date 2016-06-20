/**
* Project Name:KettleUtil
* Date:2016年6月21日上午12:10:20
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import javax.sql.DataSource;

import org.pentaho.di.core.database.util.DatabaseUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 一般工具类 <br/>
 * date: 2016年6月21日 上午12:10:20 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class CommonUtil {
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @param dbCode
    * @return
    */
    public static JdbcTemplate getJdbcTemplate(JobEntryEval jee, String dbCode) {
        try {
            DataSource dataSource = ( new DatabaseUtil() ).getNamedDataSource( dbCode );
            JdbcTemplate jt = new JdbcTemplate(dataSource);
            return jt;
        } catch (KettleException e1) {
            jee.logError("获取数据库失败", e1);
        }
        return null;
    }
}
