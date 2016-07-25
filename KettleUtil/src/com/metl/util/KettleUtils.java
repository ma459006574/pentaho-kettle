package com.metl.util;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.BaseRepositoryMeta;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.jobexecutor.JobExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;

import com.alibaba.fastjson.JSONObject;
import com.metl.constants.Constants;
import com.metl.db.Db;

 /**
 * ClassName: KettleUtils <br/>
 * Function: kettle定制化开发工具集. <br/>
 * date: 2015年4月29日 上午8:56:24 <br/>
 * @author jingma@iflytek.com
 * @version 0.0.1
 * @since JDK 1.6
 */
/**
*  <br/>
* date: 2016年7月20日 下午10:55:49 <br/>
* @author jingma@iflytek.com
* @version 
*/
public class KettleUtils {
	/**
	 * LOG:日志
	 */
	public static Logger log = Logger.getLogger(KettleUtils.class);
	/**
	 * repository:kettle资源库
	 */
	private static Repository repository;
	/**
	* 转换模板
	*/
	private static TransMeta transMetaTemplate;
	/**
	* 作业模板
	*/
	private static JobMeta jobMetaTemplate;
	
	/**
	 * getInstance:获取的单例资源库. <br/>
	 * @author jingma@iflytek.com
	 * @return 已经初始化的资源库
	 * @throws KettleException 若没有初始化则抛出异常
	 * @since JDK 1.6
	 */
	public static Repository getInstanceRep() throws KettleException{
		if(repository!=null){
			return repository;
		}else{
			throw new KettleException("没有初始化资源库");
		}
	}
	
	/**
	 * createFileRep:创建文件资源库. <br/>
	 * @author jingma@iflytek.com
	 * @param id 资源库id
	 * @param repName 资源库名称
	 * @param description 资源库描述
	 * @param baseDirectory 资源库目录
	 * @return 已经初始化的资源库
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static Repository createFileRep(String id, String repName, String description, String baseDirectory) throws KettleException{
        destroy();
        //初始化kettle环境
        if(!KettleEnvironment.isInitialized()){
            KettleEnvironment.init();
        }
		KettleFileRepositoryMeta fileRepMeta = new KettleFileRepositoryMeta( id, repName, description, baseDirectory);
        return createRep(fileRepMeta, id, repName, description);
	}

    /**
     * createDBRep:创建数据库资源库. <br/>
     * @author jingma@iflytek.com
     * @param name 数据库连接名称
     * @param type 数据库类型
     * @param access 访问类型
     * @param host ip地址
     * @param db 数据库名称
     * @param port 端口
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @return 初始化的资源库
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static Repository createDBRepByJndi(String name, String type,
            String db) throws KettleException{
        
        return createDBRep( name, type, "JNDI", null, 
             db, null, null, null, "DBRep", "DBRep", "数据库资源库");
    }
    
	/**
	 * createDBRep:创建数据库资源库. <br/>
	 * @author jingma@iflytek.com
	 * @param name 数据库连接名称
	 * @param type 数据库类型
	 * @param access 访问类型
	 * @param host ip地址
	 * @param db 数据库名称
	 * @param port 端口
	 * @param user 数据库用户名
	 * @param pass 数据库密码
	 * @return 初始化的资源库
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static Repository createDBRep(String name, String type, String access, String host, 
			String db, String port, String user, String pass) throws KettleException{
		return createDBRep( name, type, access, host, 
			 db, port, user, pass, "DBRep", "DBRep", "数据库资源库");
	}
	
	/**
	 * createDBRep:创建数据库资源库. <br/>
	 * @author jingma@iflytek.com
	 * @param name 数据库连接名称
	 * @param type 数据库类型
	 * @param access 访问类型
	 * @param host ip地址
	 * @param db 数据库名称
	 * @param port 端口
	 * @param user 数据库用户名
	 * @param pass 数据库密码
	 * @param id 资源库id
	 * @param repName 资源库名称
	 * @param description 资源库描述
	 * @return 已经初始化的资源库
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static Repository createDBRep(String name, String type, String access, String host, 
			String db, String port, String user, String pass,String id, String repName, String description) throws KettleException{
        destroy();
        //初始化kettle环境
        if(!KettleEnvironment.isInitialized()){
            KettleEnvironment.init();
        }
        if(System.getenv("org.osjava.sj.root")!=null){
            System.setProperty("org.osjava.sj.root", System.getenv("org.osjava.sj.root"));
            log.info("Simple-jndi配置根路径（org.osjava.sj.root）："+System.getenv("org.osjava.sj.root"));
        }
		//创建资源库数据库对象，类似我们在spoon里面创建资源库
		DatabaseMeta dataMeta = new DatabaseMeta(name, type, access, host, db, port, user, pass);
//		dataMeta.set
		//资源库元对象
		KettleDatabaseRepositoryMeta kettleDatabaseMeta = 
				new KettleDatabaseRepositoryMeta(id, repName, description, dataMeta);
		return createRep(kettleDatabaseMeta, id, repName, description);
	}
    public static Repository createRep(BaseRepositoryMeta baseRepositoryMeta,
            String id, String repName, String description) throws KettleException{
        if(baseRepositoryMeta instanceof KettleDatabaseRepositoryMeta){
            //创建资源库对象
            repository = new KettleDatabaseRepository();
            //给资源库赋值
            repository.init((KettleDatabaseRepositoryMeta) baseRepositoryMeta);
        }else{
            //创建资源库对象
            repository = new KettleFileRepository();
            //给资源库赋值
            repository.init((KettleFileRepositoryMeta) baseRepositoryMeta);
        }
        log.info(repository.getName()+"资源库初始化成功");
        return repository;
    }
	
	/**
	 * connect:连接资源库. <br/>
	 * @author jingma@iflytek.com
	 * @return 连接后的资源库
	 * @throws KettleSecurityException
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static Repository connect() throws KettleSecurityException, KettleException{
		return connect(null,null);
	}
	
	/**
	 * connect:连接资源库. <br/>
	 * @author jingma@iflytek.com
	 * @param username 资源库用户名
	 * @param password 资源库密码
	 * @return 连接后的资源库
	 * @throws KettleSecurityException
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static Repository connect(String username,String password) throws KettleSecurityException, KettleException{
		repository.connect(username, password);
		log.info(repository.getName()+"资源库连接成功");
		return repository;
	}
	
	/**
	 * setRepository:设置资源库. <br/>
	 * @author jingma@iflytek.com
	 * @param repository 外部注入资源库
	 * @since JDK 1.6
	 */
	public static void setRepository(Repository repository){
		KettleUtils.repository = repository;
	}
	
	/**
	 * destroy:释放资源库. <br/>
	 * @author jingma@iflytek.com
	 * @since JDK 1.6
	 */
	public static void destroy(){
		if(repository!=null){
			repository.disconnect();
			log.info(repository.getName()+"资源库释放成功");
            repository = null;
		}
	}

	/**
	 * loadJob:通过id加载job. <br/>
	 * @author jingma@iflytek.com
	 * @param jobId 数字型job的id，数据库资源库时用此方法
	 * @return job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(long jobId) throws KettleException {
		return repository.loadJob(new LongObjectId(jobId), null);
	}

	/**
	 * loadJob:通过id加载job. <br/>
	 * @author jingma@iflytek.com
	 * @param jobId 字符串job的id，文件资源库时用此方法
	 * @return job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobId) throws KettleException {
		return repository.loadJob(new StringObjectId(jobId), null);
	}

	/**
	 * loadTrans:加载作业. <br/>
	 * @author jingma@iflytek.com
	 * @param jobname 作业名称
	 * @param directory 作业路径
	 * @return 作业元数据
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobname, String directory) {
		return loadJob(jobname, directory, repository);
	}
	/**
	 * loadTrans:加载作业. <br/>
	 * @author jingma@iflytek.com
	 * @param jobname 作业名称
	 * @param directory 作业路径
	 * @param repository 资源库
	 * @return 作业元数据
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobname, String directory,Repository repository) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directory);
			return repository.loadJob(jobname,dir,null, null);
		} catch (KettleException e) {
			log.error("获取作业失败,jobname:"+jobname+",directory:"+directory, e);
		}
		return null;
	}
	
	/**
	 * loadTrans:加载转换. <br/>
	 * @author jingma@iflytek.com
	 * @param transname 转换名称
	 * @param directory 转换路径
	 * @return 转换元数据
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(String transname, String directory) {
		return loadTrans(transname, directory, repository);
	}
	
	/**
	 * loadTrans:加载转换. <br/>
	 * @author jingma@iflytek.com
	 * @param transname 转换名称
	 * @param directory 转换路径
	 * @param repository 资源库
	 * @return 转换元数据
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(String transname, String directory,Repository repository) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directory);
			return repository.loadTransformation( transname, dir, null, true, null);
		} catch (KettleException e) {
			log.error("获取转换失败,transname:"+transname+",directory:"+directory, e);
		}
		return null;
	}

	/**
	 * loadTrans:根据job元数据获取指定转换元数据. <br/>
	 * @author jingma@iflytek.com
	 * @param jobMeta job元数据
	 * @param teansName 转换名称
	 * @return 转换元数据
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(JobMeta jobMeta, String teansName) {
		JobEntryTrans trans = (JobEntryTrans)(jobMeta.findJobEntry(teansName).getEntry());
		TransMeta transMeta = KettleUtils.loadTrans(trans.getTransname(), trans.getDirectory());
		return transMeta;
	}

	/**
	* 加载作业中的作业实体 <br/>
	* @author jingma@iflytek.com
	* @param jobMeta 父作业
	* @param jobEntryName 作业名称
	* @param jobEntryMeta 用于承载将要加载的作业
	* @return
	*/
	public static <T extends JobEntryBase> T loadJobEntry(JobMeta jobMeta, String jobEntryName,
			T jobEntryMeta) {
		try {
			jobEntryMeta.loadRep(KettleUtils.getInstanceRep(), null, 
					jobMeta.findJobEntry(jobEntryName).getEntry().getObjectId(), 
					KettleUtils.getInstanceRep().readDatabases(),null);
		} catch (KettleException e) {
			log.error("获取作业控件失败", e);
		}
		return jobEntryMeta;
	}

	/**
	 * saveTrans:保存转换. <br/>
	 * @author jingma@iflytek.com
	 * @param transMeta 转换元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static void saveTrans(TransMeta transMeta) throws KettleException {
//		repository.save(transMeta, null, new RepositoryImporter(repository), true );
		repository.save(transMeta, null, null, true );
	}

	/**
	 * saveJob:保存job. <br/>
	 * @author jingma@iflytek.com
	 * @param jobMeta job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static void saveJob(JobMeta jobMeta) throws KettleException {
//		repository.save(jobMeta, null, new RepositoryImporter(repository), true );
		repository.save(jobMeta, null, null, true );
	}

	/**
	 * isDirectoryExist:判断指定的job目录是否存在. <br/>
	 * @author jingma@iflytek.com
	 * @param directoryName
	 * @return
	 * @since JDK 1.6
	 */
	public static boolean isDirectoryExist(String directoryName) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directoryName);
			if(dir==null){
				return false;
			}else{
				return true;
			}
		} catch (KettleException e) {
			log.error("判断job目录是否存在失败！",e);
		}
		return false;
	}
    /**
     * 获取或创建目录 <br/>
     * @author jingma@iflytek.com
     * @param parentDirectory 父级目录
     * @param directoryName 要创建的目录
     * @return
     * @since JDK 1.6
     */
    public static RepositoryDirectoryInterface getOrMakeDirectory(String parentDirectory,String directoryName) {
        try {
            RepositoryDirectoryInterface dir = repository.findDirectory(parentDirectory+"/"+directoryName);
            if(dir==null){
                return repository.createRepositoryDirectory(repository.findDirectory(parentDirectory), directoryName);
            }else{
                return dir;
            }
        } catch (KettleException e) {
            log.error("判断job目录是否存在失败！",e);
        }
        return null;
    }
	
	/**
	 * 获取指定的job目录. <br/>
	 * @author jingma@iflytek.com
	 * @param directoryName
	 * @return
	 * @since JDK 1.6
	 */
	public static String getDirectory(ObjectId dirId) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(dirId);
			if(dir==null){
				return null;
			}else{
				return dir.getPath();
			}
		} catch (KettleException e) {
			log.error("判断job目录是否存在失败！",e);
		}
		return null;
	}

	/**
	* 将步骤smi设置到转换trans中<br/>
	* @author jingma@iflytek.com
	* @param teans 转换元数据
	* @param stepName 步骤名称
	* @param smi 步骤
	*/
	public static void setStepToTrans(TransMeta teans, String stepName, StepMetaInterface smi) {
		try {
			StepMeta step = teans.findStep(stepName);
			step.setStepMetaInterface(smi);
		} catch (Exception e) {
			log.error("将步骤smi设置到转换trans中-失败",e);
		}
	}

	/**
	* 将步骤smi设置到转换trans中并保存到资源库 <br/>
	* @author jingma@iflytek.com
	* @param teans 转换元数据
	* @param stepName 步骤名称
	* @param smi 步骤
	*/
	public static void setStepToTransAndSave(TransMeta teans, String stepName, StepMetaInterface smi) {
		setStepToTrans( teans, stepName, smi);
		try {
			KettleUtils.saveTrans(teans);
		} catch (KettleException e) {
			log.error("将步骤smi设置到转换trans中并保存到资源库-失败",e);
		}
	}

	/**
	* 步骤数据预览 <br/>
	* @author jingma@iflytek.com
	* @param teans 转换
	* @param testStep 步骤名称
	* @param smi 步骤实体
	* @param previewSize 预览的条数
	* @return 预览结果
	*/
	public static List<List<Object>> stepPreview(TransMeta teans,
			String testStep, StepMetaInterface smi, int previewSize) {
		TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(
				teans,
				smi,
				testStep);
		TransPreviewUtil tpu = new TransPreviewUtil(
				previewMeta,
		        new String[] { testStep },
		        new int[] { previewSize } );
		tpu.doPreview();
		return TransPreviewUtil.getData(tpu.getPreviewRowsMeta(testStep),tpu.getPreviewRows(testStep));
	}

	/**
	* 将指定job复制到KettleUtils中的资源库 <br/>
	* @author jingma@iflytek.com
	* @param jobName job名称
	* @param jobPath job路径
	* @param repository 来源资源库
	* @throws KettleException
	*/
	public static void jobCopy(String jobName,String jobPath,Repository repository) throws KettleException {
		JobMeta jobMeta = KettleUtils.loadJob(jobName,jobPath,repository);
		for(JobEntryCopy jec:jobMeta.getJobCopies()){
			if(jec.isTransformation()){
				JobEntryTrans jet = (JobEntryTrans)jec.getEntry();
				transCopy(jet.getObjectName(), jet.getDirectory(),repository);
			}else if(jec.isJob()){
				JobEntryJob jej = (JobEntryJob)jec.getEntry();
				jobCopy(jej.getObjectName(),jej.getDirectory(),repository);
			}
		}
		jobMeta.setRepository(KettleUtils.getInstanceRep());
		jobMeta.setMetaStore(KettleUtils.getInstanceRep().getMetaStore());
		if(!isDirectoryExist(jobPath)){
			//所在目录不存在则创建
			KettleUtils.repository.createRepositoryDirectory(KettleUtils.repository.findDirectory("/"), jobPath);
		}
		KettleUtils.saveJob(jobMeta);
	}

	/**
	* 将指定转换复制到KettleUtils中的资源库 <br/>
	* @author jingma@iflytek.com
	* @param jobName 转换名称
	* @param jobPath 转换路径
	* @param repository 来源资源库
	* @throws KettleException
	*/
	public static void transCopy(String transName,String transPath,Repository repository) throws KettleException {
		TransMeta tm = KettleUtils.loadTrans(transName, transPath, repository);
		for(StepMeta sm:tm.getSteps()){
			if(sm.isJobExecutor()){
				JobExecutorMeta jem = (JobExecutorMeta)sm.getStepMetaInterface();
				jobCopy(jem.getJobName(),jem.getDirectoryPath(),repository);
			}
			else if(sm.getStepMetaInterface() instanceof TransExecutorMeta){
				TransExecutorMeta te = (TransExecutorMeta)sm.getStepMetaInterface();
				transCopy(te.getTransName(), te.getDirectoryPath(),repository);
			}
		}
		if(!isDirectoryExist(transPath)){
			//所在目录不存在则创建
			KettleUtils.repository.createRepositoryDirectory(KettleUtils.repository.findDirectory("/"), transPath);
		}
		tm.setRepository(KettleUtils.getInstanceRep());
		tm.setMetaStore(KettleUtils.getInstanceRep().getMetaStore());
		KettleUtils.saveTrans(tm);
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

    /**
    * 获取作业id <br/>
    * 获取当前作业所在目录是否有相同的作业
    * @author jingma@iflytek.com
    * @param jm 当前作业
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getJobId(JobMeta jm){
        return getJobId(jm.getName(), jm.getRepositoryDirectory());
    }
    /**
    * 作业转换id <br/>
    * @author jingma@iflytek.com
    * @param name 作业名称
    * @param repositoryDirectory 作业目录
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getJobId(String name,
            RepositoryDirectoryInterface repositoryDirectory) {
        try {
            return repository.getJobId(name, repositoryDirectory);
        } catch (KettleException e) {
            log.debug("获取作业id失败", e);
        }
        return null;
    }

    /**
    * 获取转换id <br/>
    * 获取当前转换所在目录是否有相同的转换
    * @author jingma@iflytek.com
    * @param tm 当前转换
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getTransformationID(TransMeta tm){
        return getTransformationID(tm.getName(), tm.getRepositoryDirectory());
    }
    /**
    * 获取转换id <br/>
    * @author jingma@iflytek.com
    * @param name 转换名称
    * @param repositoryDirectory 转换目录
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getTransformationID(String name,
            RepositoryDirectoryInterface repositoryDirectory) {
        try {
            return repository.getTransformationID(name, repositoryDirectory);
        } catch (KettleException e) {
            log.debug("获取转换id失败", e);
        }
        return null;
    }

    /**
    * 修复转换连接线 <br/>
    * @author jingma@iflytek.com
    * @param tm 转换元数据
    */
    public static void repairTransHop(TransMeta tm) {
        for(int i=0;i<tm.nrTransHops();i++){
            TransHopMeta hop = tm.getTransHop(i);
            hop.setFromStep(tm.findStep(hop.getFromStep().getName()));
            hop.setToStep(tm.findStep(hop.getToStep().getName()));
        }
    }

    /**
    * 将来源对象的参数拷贝到目标对象，并根据要求修改 <br/>
    * @author jingma@iflytek.com
    * @param target 要设置的目标对象
    * @param source 来源对象
    * @param params 要修改的参数
    */
    public static void setParams(NamedParams target,
            NamedParams source,Map<String, String> params) {
        //修改参数
        target.eraseParameters();
        try {
            for ( String key : source.listParameters() ) {
                String defaultVal = source.getParameterDefault( key );
                if(params.containsKey(key)){
                    defaultVal = params.get(key);
                }
                target.addParameterDefinition( key, defaultVal, 
                        source.getParameterDescription( key ) );
            }
        } catch (Exception e) {
            log.error("保存JOB失败", e);
        }
    }

    /**
    * 修复JOB的连接线，克隆的job连接线不能显示 <br/>
    * @author jingma@iflytek.com
    * @param jm job元数据
    */
    public static void repairHop(JobMeta jm) {
        for(JobHopMeta hop:jm.getJobhops()){
            hop.setFromEntry(jm.findJobEntry(hop.getFromEntry().getName()));
            hop.setToEntry(jm.findJobEntry(hop.getToEntry().getName()));
        }
    }

	/**
	 * @return transMetaTemplate 
	 */
	public static TransMeta getTransMetaTemplate() {
//		if(transMetaTemplate==null){
//			setTransMetaTemplate(KettleUtils.loadTrans(SysCode.TRANS_TEMPLATE_NAME, SysCode.TEMPLATE_DIR));
//		}
		return transMetaTemplate;
	}

	/**
	 * @param transMetaTemplate the transMetaTemplate to set
	 */
	public static void setTransMetaTemplate(TransMeta transMetaTemplate) {
		KettleUtils.transMetaTemplate = transMetaTemplate;
	}

	/**
	 * @return jobMetaTemplate 
	 */
	public static JobMeta getJobMetaTemplate() {
//		if(jobMetaTemplate==null){
//			setJobMetaTemplate(KettleUtils.loadJob(SysCode.JOB_TEMPLATE_NAME, SysCode.TEMPLATE_DIR));
//		}
		return jobMetaTemplate;
	}

	/**
	 * @param jobMetaTemplate the jobMetaTemplate to set
	 */
	public static void setJobMetaTemplate(JobMeta jobMetaTemplate) {
		KettleUtils.jobMetaTemplate = jobMetaTemplate;
	}
}

