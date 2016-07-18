/**
* Project Name:KettleUtil
* Date:2016年7月12日下午3:24:29
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.rule;

import com.alibaba.fastjson.JSONObject;
import com.metl.util.CommonUtil;
import com.metl.util.DateUtil;
import com.metl.utilrun.ExecuteDataEtlRule;

/**
 * 字段默认值规则 <br/>
 * date: 2016年7月12日 下午3:24:29 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class FieldDefaultValue {
    /**
    * 执行数据对象定义的验证转换默认值等规则 
    */
    private ExecuteDataEtlRule eder;
    
    /**
    * 获取批次标记的默认值 <br/>
    * @author jingma@iflytek.com
    * @param field
    * @return
    */
    public String batch(JSONObject field){
        //从变量中获取批次标记
        String batch = CommonUtil.getProp(eder.getKu(), "BATCH");
        return batch;
    }
    /**
    * 获取当前时间 <br/>
    * 默认返回公安部格式字符串，后期支持可通过扩展字段指定格式。
    * @author jingma@iflytek.com
    * @param field
    * @return
    */
    public Object currentDate(JSONObject field){
        return DateUtil.getGabDate();
    }
    /**
    * 获取验证信息 <br/>
    * @author jingma@iflytek.com
    * @param field
    * @return
    */
    public Object validateInfo(JSONObject field){
        //暂时还没验证
        return null;
    }
    /**
    * 通过去重字段生成的md5 <br/>
    * @author jingma@iflytek.com
    * @param field
    * @return
    */
    public Object md5Rr(JSONObject field){
        //不在这里赋值
        return null;
    }

    /**
     * @return eder 
     */
    public ExecuteDataEtlRule getEder() {
        return eder;
    }

    /**
     * @param eder the eder to set
     */
    public void setEder(ExecuteDataEtlRule eder) {
        this.eder = eder;
    }
    
}
