/**
* Project Name:KettleUtil
* Date:2016年6月21日上午12:16:28
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.alibaba.fastjson.JSONObject;

/**
 * 获取第一个对象 <br/>
 * date: 2016年6月21日 上午12:16:28 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ResultSetFast implements ResultSetExtractor {

    /**
     * 
     * @see org.springframework.jdbc.core.ResultSetExtractor#extractData(java.sql.ResultSet)
     */
    @Override
    public Object extractData(ResultSet rs) throws SQLException,
            DataAccessException {
        JSONObject json = new JSONObject();
        if(rs.next()){
            for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
                String colName = rs.getMetaData().getColumnName(i).toLowerCase();
                json.put(colName, rs.getObject(i));
            }
        }
        return json;
    }

}
