/**
* Project Name:KettleUtil
* Date:2016年6月21日下午2:55:40
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.pentaho.di.core.database.util.DatabaseUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.trans.step.BaseStep;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.fastjson.JSONObject;

/**
 * 数据库操作类 <br/>
 * date: 2016年6月21日 下午2:55:40 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class Db {
    /**
    * spring数据库操作工具
    */
    private JdbcTemplate jdbcTemplate;
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma@iflytek.com
    * @param ku 
    * @param dbCode
    * @return
    */
    public static Db getDb(BaseStep ku, String dbCode) {
        try {
            DataSource dataSource = ( new DatabaseUtil() ).getNamedDataSource( dbCode );
            JdbcTemplate jt = new JdbcTemplate(dataSource);
            return new Db(jt);
        } catch (KettleException e) {
            ku.logError("获取数据库失败", e);
        }
        return null;
    }
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @param dbCode
    * @return
    */
    public static Db getDb(JobEntryBase jee, String dbCode) {
        try {
            DataSource dataSource = ( new DatabaseUtil() ).getNamedDataSource( dbCode );
            JdbcTemplate jt = new JdbcTemplate(dataSource);
            return new Db(jt);
        } catch (KettleException e) {
            jee.logError("获取数据库失败", e);
        }
        return null;
    }
    /**
    * 关闭数据库连接即相关预编译语句 <br/>
    * @author jingma@iflytek.com
    * @param jee 
    * @param conn 数据库连接
    * @param preps 预编译语句
    */
    public static void closeConn(JobEntryEval jee,Connection conn,PreparedStatement... preps){
        for(PreparedStatement p:preps){
            if(p != null){
                try {
                    p.close();
                } catch (SQLException e) {
                    jee.logError("关闭预处理游标失败", e);
                }
            }
        }
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                jee.logError("关闭数据库连接失败", e);
            }
        }
    }
    
    public Db(JdbcTemplate jdbcTemplate) {
        super();
        this.jdbcTemplate = jdbcTemplate;
    }
    /**
    * 获取第一个对象 <br/>
    * @author jingma@iflytek.com
    * @param sql
    * @param prarms
    * @return
    */
    @SuppressWarnings("unchecked")
    public List<JSONObject> findList(String sql,Object... prarms){
        return (List<JSONObject>) jdbcTemplate.query(sql, prarms, new ResultSetList());
    }

    /**
    * 获取数据库当前时间-14位字符串 <br/>
    * @author jingma@iflytek.com
    * @return
    */
    public String getCurrentDateStr14(){
        return findOne("select to_char(sysdate,'yyyymmddhh24miss') as current_date from dual").
                getString("current_date");
    }
    /**
    * 执行更新语句 <br/>
    * @author jingma@iflytek.com
    * @param sql
    */
    public void execute(String sql){
        jdbcTemplate.execute(sql);
    }
    /**
    * 获取第一个对象 <br/>
    * @author jingma@iflytek.com
    * @param sql
    * @param prarms
    * @return
    */
    public JSONObject findOne(String sql,Object... prarms){
        return (JSONObject) jdbcTemplate.query(sql, prarms, new ResultSetFast());
    }
    /**
    * 获取数据库连接 <br/>
    * @author jingma@iflytek.com
    * @return
    * @throws SQLException
    */
    public Connection getConn() throws SQLException{
        return jdbcTemplate.getDataSource().getConnection();
    }
    /**
     * @return jdbcTemplate 
     */
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }
    /**
     * @param jdbcTemplate the jdbcTemplate to set
     */
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
