/**
* Project Name:KettleUtil
* Date:2016年6月28日上午11:49:02
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.jobrun;

import java.sql.SQLException;
import java.util.List;

import org.pentaho.di.job.entries.eval.JobEntryEval;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;

/**
 * 执行数据账单中的任务 <br/>
 * 这里会处理数据账单中，状态为审核通过的数据任务。
 * date: 2016年6月28日 上午11:49:02 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteDatabill {
    /**
    * javascript控件
    */
    private JobEntryEval jee;
    /**
    * 项目基础数据库操作对象
    */
    private Db metldb;
    
    /**
     * Creates a new instance of GenerateDataBill.
     */
    public ExecuteDatabill() {
    }
    
    public ExecuteDatabill(JobEntryEval jobEntryEval) {
        super();
        this.jee = jobEntryEval;
        metldb = Db.getDb(jee, Constants.DATASOURCE_METL);
    }

    /**
    * 开始获取并执行数据账单中的任务 <br/>
    * @author jingma@iflytek.com
    */
    public void run(){
        //开始时间
        String start = metldb.getCurrentDateStr14();
        //获取审核通过的数据账单
        String sql = "select * from metl_data_bill db where db.state=?";
        List<JSONObject> list = metldb.findList(sql, Constants.DATA_BILL_STATUS_EXAMINE_PASS);
        for(JSONObject dataBill:list){
            //来源对象
            JSONObject sourceObj = metldb.findOne("select * from metl_data_object do where do.ocode=?", 
                    dataBill.getString("source_obj"));
            //目标对象
            JSONObject targetObj = metldb.findOne("select * from metl_data_object do where do.ocode=?", 
                    dataBill.getString("target_obj"));
            //来源任务
            JSONObject sourceTask = metldb.findOne("select * from metl_data_task dt where dt.ocode=?", 
                    dataBill.getString("source_task"));
            
        }
//        String doType = sourceObj.getString("do_type");
//        jee.logBasic("来源对象类型："+doType);
//        String result = Constants.SUCCESS_FAILED_NUKNOW;
//        try {
//            switch (doType) {
//            case "table":
//                edbTable();
//                break;
//            case "view":
//                edbTable();
//                break;
//            default:
//                break;
//            }
//            result = Constants.SUCCESS_FAILED_SUCCESS;
//        } catch (SQLException e) {
//            jee.logError("创建表的数据账单失败", e);
//            result = Constants.SUCCESS_FAILED_FAILED;
//        }
//        //记录日志到数据库，便于监控。还需要将Kettle本身的运行日志记录到文件中，文件分天存放，每次一个文件
//        String sql = "insert into metl_kettle_log"
//                + "(create_user,job,start_time,end_time,etlflag,result, "
//                + "log_path,data_task)values(?,?,?,?,?,?,?,?)";
//        Connection conn = null;
//        PreparedStatement ps = null;
//        try {
//            conn = metldb.getConn();
//            ps = conn.prepareStatement(sql);
//            ps.setString(1, dataTask.getString("create_user"));
//            ps.setString(2, CommonUtil.getRootJobid(jee));
//            ps.setString(3, start);
//            ps.setString(4, metldb.getCurrentDateStr14());
//            ps.setString(5, null);
//            ps.setString(6, result);
//            ps.setString(7, null);
//            ps.setString(8, dataTask.getString("ocode"));
//            ps.execute();
//        } catch (SQLException e) {
//            jee.logError("插入日志失败", e);
//        } finally{
//            Db.closeConn(jee, conn,ps);
//        }
    }

    /**
    * 执行数据账单任务 <br/>
    * @author jingma@iflytek.com
    * @throws SQLException
    */
    private void edbTable() throws SQLException{
        
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
