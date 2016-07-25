/**
* Project Name:metl
* Date:2016年6月12日下午10:05:29
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;

/**
 * 字典管理 <br/>
 * date: 2016年6月12日 下午10:05:29 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class Dict {

    /**
    * 根据字典类别得到该类别字典的查询SQL <br/>
    * @author jingma@iflytek.com
    * @param dictCategory 字典类别
    * @return 该类别字典的查询SQL
    */
    public static String dictCategoryToSql(String dictCategory) {
        String defaultVal = "select ocode ID,oname CN from metl_unify_dict where dict_category='"
                +dictCategory+"' and is_disable='"+Constants.WHETHER_FALSE
                +"' order by oorder asc;ds=metl";
        String result = dictCategory;
        //如果不是select开头的，则判断为使用同一字典方式。
        if(StringUtil.isNotBlank(dictCategory)&&!dictCategory.toLowerCase().startsWith("select")){
            String expand = Db.use(Constants.DATASOURCE_METL).queryStr(
                    "select expand from metl_unify_dict t "
                    + "where t.dict_category='DICT_CATEGORY' and is_disable=? and t.ocode=?", 
                    Constants.WHETHER_FALSE, dictCategory);
                try {
                    result = JSONObject.parseObject(expand).getString("sql");
                    if(StringUtil.isBlank(result)){
                        result = defaultVal;
                    }
                } catch (Exception e) {
                    //解析中报错时，采用默认查询语句
                    result = defaultVal;
                }
        }
        return result;
    }

    /**
    * 获取字典列表 <br/>
    * @author jingma@iflytek.com
    * @param dictCategory 字典类别
    * @return 字典列表
    */
    public static List<JSONObject> dictList(String dictCategory){
        String expStr = dictCategoryToSql(dictCategory);
        String[] dict = parseDictExp(expStr);
        List<JSONObject> result = Db.use(dict[1]).find(dict[0]);
        return result;
    }
    /**
    * 获取字典值 <br/>
    * @author jingma@iflytek.com
    * @param dictCategory 字典类别
    * @param key 键
    * @return 值
    */
    public static String dictValue(String dictCategory,String key){
        String expStr = dictCategoryToSql(dictCategory);
        String[] dict = parseDictExp(expStr);
        String sql = "select cn from ("+dict[0]+") t where t.id=?";
        String result = Db.use(dict[1]).queryStr(sql, key);
        if(StringUtil.isBlank(result)){
            result = key;
        }
        return result;
    }
    /**
    * 获取字典对象列表 <br/>
    * @author jingma@iflytek.com
    * @param dictCategory 字典类别
    * @return 对象列表
    */
    public static List<JSONObject> dictObjList(String dictCategory){
        List<JSONObject> result = Db.use(Constants.DATASOURCE_METL).find(
                "select * from metl_unify_dict where dict_category=? and is_disable=? order by oorder asc",
                dictCategory,Constants.WHETHER_FALSE);
        return result;
    }
    /**
    * 获取字典对象 <br/>
    * @author jingma@iflytek.com
    * @param dictCategory 字典类别
    * @param key 键
    * @return 对象
    */
    public static JSONObject dictObj(String dictCategory,String key){
        JSONObject result = Db.use(Constants.DATASOURCE_METL).findFirst(
                "select * from metl_unify_dict where dict_category=? and is_disable=? and ocode=?",
                dictCategory,Constants.WHETHER_FALSE,key);
        return result;
    }
    /**
    * 解析字典sql字符串 <br/>
    * 若没有指定数据库则默认使用metl库
    * @author jingma@iflytek.com
    * @param exp
    * @return
    */
    private static String[] parseDictExp(String exp){
        if(StringUtil.isBlank(exp)){
            return null;
        }
        String[] result = new String[2];
        String[] strs = exp.split(";");
        result[0] = strs[0];
        if(strs.length>1){
            result[1] = strs[1].substring(3);
        }else{
            result[1] = Constants.DATASOURCE_METL;
        }
        return result;
    }
}
