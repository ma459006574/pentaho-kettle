/**
* Project Name:KettleUtil
* Date:2016年6月20日下午12:39:31
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl;

import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.alibaba.fastjson.JSONObject;
import com.metl.util.CommonUtil;
import com.metl.util.FastTResultSet;

/**
 * 生成数据账单 <br/>
 * date: 2016年6月20日 下午12:39:31 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class GenerateDataBill {
    /**
    * 任务编码
    */
    private String taskCode;
    /**
    * javascript控件
    */
    private JobEntryEval jee;
    
    /**
     * Creates a new instance of GenerateDataBill.
     */
    public GenerateDataBill() {
    }
    
    public GenerateDataBill(JobEntryEval jobEntryEval) {
        super();
        this.jee = jobEntryEval;
        this.taskCode = getProp("task_code");
    }

    /**
    * 开始生成数据账单 <br/>
    * @author jingma@iflytek.com
    */
    public void run(){
        jee.logBasic("任务编码："+taskCode);
        String dbCode = "metl";
        JdbcTemplate jt = CommonUtil.getJdbcTemplate(jee,dbCode);
        JSONObject t = (JSONObject) jt.query("select * from metl_data_task t where t.ocode=?", 
                new String[]{taskCode}, new FastTResultSet());
        System.out.println(t.getInteger("oorder"));
        SqlRowSet task = jt.queryForRowSet("select * from metl_data_task t where t.ocode=?", new String[]{taskCode});
        task.next();
        SqlRowSet sourceObj = jt.queryForRowSet("select * from metl_data_object t where t.ocode=?", 
                new String[]{task.getString("source_obj")});
        sourceObj.next();
        String doType = sourceObj.getString("do_type");
        jee.logBasic("来源对象类型："+doType);
        
    }
    /**
    * 获取参数 <br/>
    * @author jingma@iflytek.com
    * @param key 参数名称
    * @return 值
    */
    public String getProp(String key){
        return jee.environmentSubstitute("${"+key+"}");
    }
    /**
     * @return taskCode 
     */
    public String getTaskCode() {
        return taskCode;
    }
    /**
     * @param taskCode the taskCode to set
     */
    public void setTaskCode(String taskCode) {
        this.taskCode = taskCode;
    }

    /**
     * @return jee 
     */
    public JobEntryEval getJee() {
        return jee;
    }

    /**
     * @param jee the jee to set
     */
    public void setJee(JobEntryEval jee) {
        this.jee = jee;
    }
}
