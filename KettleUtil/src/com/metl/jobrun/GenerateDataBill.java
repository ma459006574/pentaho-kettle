/**
* Project Name:KettleUtil
* Date:2016年6月20日下午12:39:31
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.jobrun;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.job.entries.eval.JobEntryEval;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;
import com.metl.util.CommonUtil;
import com.metl.util.DateUtil;

/**
 * 生成数据账单 <br/>
 * date: 2016年6月20日 下午12:39:31 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class GenerateDataBill {
    /**
    * 数据任务
    */
    private JSONObject dataTask;
    /**
    * 来源对象
    */
    private JSONObject sourceObj;
    /**
    * javascript控件
    */
    private JobEntryEval jee;
    /**
    * 项目基础数据库操作对象
    */
    private Db metldb;
    /**
    * 抽取标记
    */
    private String etlflag;
    
    /**
     * Creates a new instance of GenerateDataBill.
     */
    public GenerateDataBill() {
    }
    
    public GenerateDataBill(JobEntryEval jobEntryEval) {
        super();
        this.jee = jobEntryEval;
        metldb = Db.use(jee, Constants.DATASOURCE_METL);
        String dataTaskOcode = CommonUtil.getProp(jee,"DATA_TASK_OCODE");
        this.dataTask = metldb.findFirst("select * from metl_data_task dt where dt.ocode=?", 
                dataTaskOcode);
        this.sourceObj = metldb.findFirst("select * from metl_data_object o where o.ocode=?", 
                dataTask.getString("source_obj"));
    }

    /**
    * 开始生成数据账单 <br/>
    * @author jingma@iflytek.com
    */
    public void run(){
        String start = metldb.getCurrentDateStr14();
        String doType = sourceObj.getString("do_type");
        jee.logBasic("来源对象类型："+doType);
        String result = Constants.SUCCESS_FAILED_NUKNOW;
        try {
            switch (doType) {
            case Constants.DO_TYPE_TABLE:
                gdbTable();
                break;
            case Constants.DO_TYPE_VIEW:
                gdbTable();
                break;
            default:
                break;
            }
            result = Constants.SUCCESS_FAILED_SUCCESS;
        } catch (SQLException e) {
            jee.logError("创建表的数据账单失败", e);
            result = Constants.SUCCESS_FAILED_FAILED;
        }
        //记录日志到数据库，便于监控。还需要将Kettle本身的运行日志记录到文件中，文件分天存放，每次一个文件
        String sql = "insert into metl_kettle_log"
                + "(create_user,job,start_time,end_time,etlflag,result, "
                + "log_path,data_task)values(?,?,?,?,?,?,?,?)";
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = metldb.getConn();
            ps = conn.prepareStatement(sql);
            ps.setString(1, dataTask.getString("create_user"));
            ps.setString(2, CommonUtil.getRootJobId(jee));
            ps.setString(3, start);
            ps.setString(4, metldb.getCurrentDateStr14());
            ps.setString(5, etlflag);
            ps.setString(6, result);
            ps.setString(7, null);
            ps.setString(8, dataTask.getString(Constants.FIELD_OCODE));
            ps.execute();
        } catch (SQLException e) {
            jee.logError("插入日志失败", e);
        } finally{
            Db.closeConn(jee, conn,ps);
        }
    }

    /**
    * 生成数据账单-表 <br/>
    * @author jingma@iflytek.com
     * @throws SQLException 
    */
    private void gdbTable() throws SQLException {
        String isAdd = dataTask.getString("is_add");
        //增量，需要在前端做好校验，必须配置好增量字段，修改时也要做相关校验，这些就比较复杂了，
        //这些验证将来需要仔细梳理，系统的做一遍测试
        if(Constants.WHETHER_TRUE.equals(isAdd)){
            String dbCode = sourceObj.getString("database");
            JSONObject addField = metldb.findFirst("select * from metl_data_field t where t.oid=?", 
                    sourceObj.getString("add_field"));
            //增量字段支持的业务类型是：时间、数字
            String dataType = addField.getString("data_type");
            String businessType = addField.getString("business_type");
            //实时从数据库查询抽取标记
            etlflag = metldb.findFirst("select etlflag from metl_data_task t where t.ocode=?", 
                    dataTask.getString(Constants.FIELD_OCODE)).getString("etlflag");
            Db sourcedb = Db.use(jee, dbCode);
            //如果抽取标记为空，则从来源对象获取增量字段最小值
            JSONObject minObj = null;
            JSONObject maxObj = null;
            if(StringUtils.isBlank(etlflag)){
                minObj = sourcedb.findFirst("select min("+addField.getString(Constants.FIELD_OCODE)
                        +") as etlflag from "+sourceObj.getString("real_name"));
                //数据类型是date
                if(Constants.FD_TYPE_DATE.equals(dataType)){
                    etlflag = DateUtil.doFormatDate(minObj.getDate("etlflag"),
                            DateUtil.DATE_FORMATTER14);
                }else{
                    //否则数据类型就是字符串，这些限制需要在前端配置时做好校验
                    etlflag = minObj.getString("etlflag");
                }
            }
            //若抽取标识为空，则结束
            if(StringUtils.isBlank(etlflag)){
                jee.logError(dataTask+"任务来源对象没有数据");
                return;
            }
            //数据类型是date
            if(Constants.FD_TYPE_DATE.equals(dataType)){
                //获取来源对象中增量字段最大值，小于当前时间
                maxObj = sourcedb.findFirst("select max("+addField.getString(Constants.FIELD_OCODE)
                        +") as etlflag from "+sourceObj.getString("real_name")+
                        " t where t."+addField.getString(Constants.FIELD_OCODE)+"<sysdate");
            }else{
                //获取来源对象中增量字段最大值，小于当前时间
                maxObj = sourcedb.findFirst("select max("+addField.getString(Constants.FIELD_OCODE)
                        +") as etlflag from "+sourceObj.getString("real_name")+
                        " t where t."+addField.getString(Constants.FIELD_OCODE)
                        +"<to_char(sysdate,'yyyymmddhh24miss')");
            }
            String tempEtlflag = null;
            //业务类型是时间
            if(Constants.FB_TYPE_DATE.equals(businessType)){
                //首次，对历史数据进行分片处理
                if(minObj != null){
                    //开始
                    Calendar start = Calendar.getInstance();
                    //结束
                    Calendar end = Calendar.getInstance();

                    //数据类型是date
                    if(Constants.FD_TYPE_DATE.equals(dataType)){
                        start.setTime(minObj.getDate("etlflag"));
                        end.setTime(maxObj.getDate("etlflag"));
                    }else{
                        //否则数据类型就是字符串(现在只认14位长度的时间)，这些限制需要在前端配置时做好校验
                        start.setTime(DateUtil.parseDate(minObj.getString("etlflag")));
                        end.setTime(DateUtil.parseDate(maxObj.getString("etlflag")));
                    }
                    //增量，以天为单位
                    Integer addInterval = sourceObj.getInteger("add_interval");
                    //若增量间隔为空，则设置最大值，相当于不分片
                    if(addInterval==null){
                        addInterval = Integer.MAX_VALUE;
                    }
                    while(true){
                        start.add(Calendar.DAY_OF_MONTH, addInterval);
                        tempEtlflag = DateUtil.doFormatDate(start.getTime(),
                                DateUtil.DATE_FORMATTER14);
                        //如果还没有达到最大值
                        if(start.before(end)){
                            //在数据账单中添加记录
                            addDataBillTable(etlflag,tempEtlflag);
                            etlflag = tempEtlflag;
                        }else if(start.equals(end)||start.after(end)){
                            tempEtlflag = DateUtil.doFormatDate(end.getTime(),
                                    DateUtil.DATE_FORMATTER14);
                            //在数据账单中添加记录
                            addDataBillTable(etlflag,tempEtlflag);
                            etlflag = tempEtlflag;
                            break;
                        }
                    }
                }else{
                    //若已经有数据账单生成过了，则直接生成增量账单
                    //数据类型是date
                    if(Constants.FD_TYPE_DATE.equals(dataType)){
                        tempEtlflag = DateUtil.doFormatDate(maxObj.getDate("etlflag"),
                                DateUtil.DATE_FORMATTER14);
                    }else{
                        //否则数据类型就是字符串(现在只认14位长度的时间)，这些限制需要在前端配置时做好校验
                        tempEtlflag = maxObj.getString("etlflag");
                    }
                    if(!etlflag.equals(tempEtlflag)){
                        //在数据账单中添加记录
                        addDataBillTable(etlflag,tempEtlflag);
                    }
                }
            }else{
                //否则就是数字，本系统增量字段业务类型支持时间和数字
                //首次，对历史数据进行分片处理
                if(minObj != null){
                    //开始
                    BigDecimal start = minObj.getBigDecimal("etlflag");
                    //结束
                    BigDecimal end = maxObj.getBigDecimal("etlflag");
                    //增量
                    BigDecimal addInterval = sourceObj.getBigDecimal("add_interval");
                    //若增量间隔为空，则设置为超常规的大间隔，相当于不分片
                    if(addInterval==null){
                        addInterval = BigDecimal.valueOf(99999999999999999l);
                    }
                    while(true){
                        start = start.add(addInterval);
                        tempEtlflag = start.toString();
                        
                        //如果还没有达到最大值
                        if(start.compareTo(end)<0){
                            //在数据账单中添加记录
                            addDataBillTable(etlflag,tempEtlflag);
                            etlflag = tempEtlflag;
                        }else if(start.compareTo(end)>=0){
                            tempEtlflag = end.toString();
                            //在数据账单中添加记录
                            addDataBillTable(etlflag,tempEtlflag);
                            etlflag = tempEtlflag;
                            break;
                        }
                    }
                }else{
                    //若已经有数据账单生成过了，则直接生成增量账单
                    tempEtlflag = maxObj.getString("etlflag");
                    if(!etlflag.equals(tempEtlflag)){
                        //在数据账单中添加记录
                        addDataBillTable(etlflag,tempEtlflag);
                    }
                }
                
            }
            etlflag = tempEtlflag;
            //更新抽取标记
            String sql = "update METL_DATA_TASK t set t.etlflag=? where t.oid=?";
            metldb.update(sql,etlflag,dataTask.getString(Constants.FIELD_OID));
        }else{
            //否则就不是增量
            
        }
    }

    /**
    * 添加数据账单-表 <br/>
    * 处理数据时：<code>start<=add_field<=end</code>但是这样会导致数据账单中数据片数据之和大于总数。
    * @author jingma@iflytek.com
    * @param start 数据片开始
    * @param end 数据片结束
     * @throws SQLException 
    */
    private void addDataBillTable(String start, String end) throws SQLException {
        String sql = "insert into metl_data_bill (create_user, source_task, "
                + "source_obj, target_obj, job, database, source_table, "
                + "shard_field, shard_start, shard_end, state) values (?,?,?,?,?,?,?,?,?,?,?)";
        Connection conn = null;
        PreparedStatement insert = null;
        try {
            conn = metldb.getConn();
            insert = conn.prepareStatement(sql);
            insert.setString(1, dataTask.getString("create_user"));
            insert.setString(2, dataTask.getString(Constants.FIELD_OCODE));
            insert.setString(3, dataTask.getString("source_obj"));
            insert.setString(4, dataTask.getString("target_obj"));
            insert.setString(5, CommonUtil.getRootJobId(jee));
            insert.setString(6, sourceObj.getString("database"));
            insert.setString(7, sourceObj.getString("real_name"));
            insert.setString(8, sourceObj.getString("add_field"));
            insert.setString(9, start);
            insert.setString(10,end);
            //不需要审核
            if(Constants.WHETHER_FALSE.equals(dataTask.getString("is_examine"))){
                insert.setString(11,Constants.DATA_BILL_STATUS_EXAMINE_PASS);
            }else{
                insert.setString(11,Constants.DATA_BILL_STATUS_WAIT_EXAMINE);
            }
            insert.execute();
        } finally {
            Db.closeConn(jee, conn, insert);
        }
    }

    /**
     * @return dataTask 
     */
    public JSONObject getDataTask() {
        return dataTask;
    }

    /**
     * @param dataTask the dataTask to set
     */
    public void setDataTask(JSONObject dataTask) {
        this.dataTask = dataTask;
    }

    /**
     * @return sourceObj 
     */
    public JSONObject getSourceObj() {
        return sourceObj;
    }

    /**
     * @param sourceObj the sourceObj to set
     */
    public void setSourceObj(JSONObject sourceObj) {
        this.sourceObj = sourceObj;
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

    /**
     * @return etlflag 
     */
    public String getEtlflag() {
        return etlflag;
    }

    /**
     * @param etlflag the etlflag to set
     */
    public void setEtlflag(String etlflag) {
        this.etlflag = etlflag;
    }
    
}
