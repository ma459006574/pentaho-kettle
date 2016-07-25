/**
* Project Name:KettleUtil
* Date:2016年6月28日上午11:49:02
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.jobrun;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.pentaho.di.repository.Repository;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;
import com.metl.util.CommonUtil;
import com.metl.util.DateUtil;
import com.metl.util.KettleUtils;
import com.metl.util.StringUtil;

/**
 * 执行数据账单中的任务 <br/>
 * 这里会处理数据账单中，状态为审核通过的数据任务。
 * date: 2016年6月28日 上午11:49:02 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class ExecuteDatabill {
    /**
    * 日志
    */
    private static Log log = LogFactory.getLog(ExecuteDatabill.class);
    /**
    * 是否将job运行日志写入文件
    */
    private static Boolean isWriteLogFile = false;
    /**
    * 日志文件的根路径
    */
    private static String logFileRoot = "/metl/logs/kettle";
    /**
    * 项目基础数据库操作对象
    */
    private static Db metldb;
    /**
    * 数据任务与job元数据的映射：<数据任务,job元数据>
    */
    private static Map<String,JobMeta> taskJobMateMap= new HashMap<String, JobMeta>();
    /**
    * job日志已处理行数记录：<作业，已处理行数>
    */
    private static Map<Job,Integer> jobLogLine = new HashMap<Job, Integer>();
    /**
    * job日志已处理行数记录：<作业，作业对应的日志输出流>
    */
    private static Map<Job,FileOutputStream> jobLogStream = new HashMap<Job, FileOutputStream>();
    /**
    * 任务队列:<目标对象,job列表>
    */
    private static Map<String, List<Job>> taskQueue = new HashMap<String, List<Job>>();
    /**
    * javascript控件
    */
    private JobEntryEval jee;
    
    static{
        //读取配置信息
        metldb = Db.use(Constants.DATASOURCE_METL);
        JSONObject logConfig = metldb.findGeneralConfig("execute_bill_config").
                getJSONObject("logConfig");
        isWriteLogFile = logConfig.getBoolean("isWriteLogFile");
        logFileRoot = logConfig.getString("logFileRoot");
        //处理之前中断的任务
        //获取所有之前中断的数据账单。
        String sql = "select * from metl_data_bill db where db.state=? and db.is_disable=?";
        List<JSONObject> list = metldb.find(sql, Constants.DATA_BILL_STATUS_CURRENT_INPUT,
                Constants.WHETHER_FALSE);
        log.info("之前中断的任务数："+list.size());
        //若对这些数据量要求高精度，则需要将之前入的部分数据删除，重新入。
        //删除数据可能存在一定的风险，入来源是表对象的一个片段，不同时间片段内数据量也在变化,这里考虑针对不同数据对象类型具体处理
//        for(JSONObject db:list){
//            //删除之前入的数据
//        }
        //将入库中状态改为审核通过，之前可能已经入了部分数据了，这部分数据会被记入到重复数据量
        metldb.update("update metl_data_bill db set db.state=? where db.state=?", 
                Constants.DATA_BILL_STATUS_EXAMINE_PASS, 
                Constants.DATA_BILL_STATUS_CURRENT_INPUT);
        
    }
    
    /**
     * Creates a new instance of GenerateDataBill.
     */
    public ExecuteDatabill() {
    }
    
    public ExecuteDatabill(JobEntryEval jobEntryEval) {
        super();
        this.jee = jobEntryEval;
    }

    /**
    * 开始获取并执行数据账单中的任务 <br/>
    * @author jingma@iflytek.com
    */
    public void run(){
        //开始时间
        String start = metldb.getCurrentDateStr14();
        String result = Constants.SUCCESS_FAILED_NUKNOW;
        //将新增任务数记录到日志表的读取量中，完成任务数记录到新增量中
        int inputCount = 0;
        int addCount = 0;
        try {
            inputCount = addTask();
            addCount = executeTask();
            result = Constants.SUCCESS_FAILED_SUCCESS;
        } catch (Exception e) {
            jee.logError("执行数据账单任务异常", e);
            result = Constants.SUCCESS_FAILED_FAILED;
        }
        //若没有新增任务且没有出现异常，则不记录日志了
        if(inputCount==0&&Constants.SUCCESS_FAILED_SUCCESS.equals(result)){
            return;
        }
        //记录日志到数据库，便于监控。还需要将Kettle本身的运行日志记录到文件中，文件分天存放，每次一个文件
        String sql = "insert into metl_kettle_log"
                + "(job,start_time,end_time,input_count,add_count,result)values(?,?,?,?,?,?)";
        metldb.update(sql, CommonUtil.getRootJobId(jee),start,
                metldb.getCurrentDateStr14(),inputCount,addCount,result);
    }

    /**
    * 执行任务队列中的任务 <br/>
    * @author jingma@iflytek.com
    * @return 执行完成的任务数量
    */
    private int executeTask() {
        Job job = null;
        int overCount = 0;
        for(String to:taskQueue.keySet()){
            //没有入该数据对象的任务
            if(taskQueue.get(to).size()==0){
                continue;
            }
            job = taskQueue.get(to).get(0);
            if(!job.isInitialized()){
                //还未初始化
                job.start();
            }else if(!job.isActive()){
                //运行结束
                writeJobLog(job);
                try {
                    jobLogStream.get(job).close();
                } catch (Exception e) {
                    jee.logBasic("关闭日志输出流失败", e);
                }
                jobLogLine.remove(job);
                jobLogStream.remove(job);
                taskQueue.get(to).remove(job);
                overCount++;
                //没有入该数据对象的任务
                if(taskQueue.get(to).size()==0){
                    continue;
                }
                //取下一个job继续运行
                job = taskQueue.get(to).get(0);
                job.start();
            }else{
                //正在运行
            }
            writeJobLog(job);
        }
        return overCount;
    }

    /**
    * 根据数据账单添加要执行的任务 <br/>
    * @author jingma@iflytek.com
    * @return 新增任务数量
    * @throws Exception 
    */
    public int addTask() throws Exception {
        Repository rep = jee.getRepository();
        //获取审核通过且没有禁用的数据账单,用分片开始字段升序执行，确保从旧数据开始入
        String sql = "select * from metl_data_bill db where db.state=? and db.is_disable=? order by db.shard_start asc";
        List<JSONObject> list = metldb.find(sql, Constants.DATA_BILL_STATUS_EXAMINE_PASS,
                Constants.WHETHER_FALSE);
        if(list.size()>0){
            jee.logBasic("新增任务数："+list.size());
        }else{
            return 0;
        }
        JobMeta jobMeta = null;
        Job job = null;
        List<Job> jobList = null;
        for(JSONObject dataBill:list){
            String sourceTask = dataBill.getString("source_task");
            String targetObj = dataBill.getString("target_obj");
            if(!taskJobMateMap.containsKey(sourceTask)){
                jobMeta = KettleUtils.loadJob("edb_"+sourceTask, 
                        Constants.KETTLE_TP_ROOT_DIR+Constants.FXG+sourceTask,rep);
                taskJobMateMap.put(sourceTask, jobMeta);
            }
            jobMeta = (JobMeta) taskJobMateMap.get(sourceTask).realClone(false);
            //设置数据账单主键参数
            jobMeta.setParameterValue(Constants.KETTLE_PARAM_DATA_BILL_OID, 
                    dataBill.getString(Constants.FIELD_OID));
            jobMeta.setParameterValue(Constants.KETTLE_PARAM_KETTLE_LOG_OID, 
                    StringUtil.getUUIDUpperStr());
            job = new Job(rep, jobMeta);
            //若该目标对象还没有初始化
            if(!taskQueue.containsKey(targetObj)){
                jobList = new ArrayList<Job>();
                taskQueue.put(targetObj, jobList);
            }else{
                jobList = taskQueue.get(targetObj);
            }
            jobList.add(job);
            //修改数据账单状态为：正在入库中
            metldb.update("update metl_data_bill db set db.state=? where db.oid=?", 
                    Constants.DATA_BILL_STATUS_CURRENT_INPUT,
                    dataBill.getString(Constants.FIELD_OID));
        }
        return list.size();
    }

    /**
    * 写job日志 <br/>
    * @author jingma@iflytek.com
    * @param job 要写日志的job
    */
    public void writeJobLog(Job job) {
        //如果不写日志到文件
        if(!isWriteLogFile){
            return;
        }
        int lastLineNr = KettleLogStore.getLastBufferLineNr();
        int startLineNr = jobLogLine.get(job)==null?0:jobLogLine.get(job);
        jobLogLine.put(job, lastLineNr);
        //新增的日志
        String joblogStr = KettleLogStore.getAppender().getBuffer(
                job.getLogChannel().getLogChannelId(), false, 
                startLineNr, lastLineNr ).toString();
        FileOutputStream outStream = jobLogStream.get(job);
        File logFile = null;
        //还有对应的输出流
        if(outStream==null){
            logFile = new File(logFileRoot+Constants.FXG
                    +DateUtil.doFormatDate(new Date(), DateUtil.DATE_FORMATTER8));
            if(!logFile.exists()){
                logFile.mkdirs();
            }
            try {
                logFile = new File(logFile.getAbsolutePath()+Constants.FXG
                        +job.getJobname()+"_"+job.getJobMeta().getParameterValue(
                                Constants.KETTLE_PARAM_KETTLE_LOG_OID)+".txt");
                if(!logFile.exists()){
                        logFile.createNewFile();
                        outStream = new FileOutputStream(logFile);
                        jobLogStream.put(job, outStream);
                }
            } catch (Exception e) {
                jee.logError("创建日志文件失败："+logFile.getAbsolutePath(), e);
            }
        }
        try {
            outStream.write(joblogStr.getBytes());
        } catch (Exception e) {
            jee.logError("写日志文件失败", e);
        }
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
        ExecuteDatabill.metldb = metldb;
    }
    
}
