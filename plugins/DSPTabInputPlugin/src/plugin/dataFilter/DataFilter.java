package plugin.dataFilter;

import java.io.StringReader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

import plugin.exception.DSPTabInputException;
import plugin.util.DSPContents;
import plugin.util.DSPDBUtil;
import plugin.util.IDCardUtil;
import plugin.util.OutColumnsFinder;
import plugin.util.SqlParser;

/**
 * 
 * @author qisun2
 * @email qisun2@iflytek.com
 * @date 2014年11月22日 13:40:35
 * @discription 对数据的验证转换
 * 
 */
public class DataFilter {
	//private static Logger log = Logger.getLogger(DataFilter.class);
	private LogChannelInterface log =  new LogChannel("dsp数据过滤");
	/**
	 * columns:TODO(表输入字段数组).
	 * @since JDK 1.6
	 */
	private String[] columns;
	/**
	 * valiTypeMap:TODO(各字段验证规则).
	 * @since JDK 1.6
	 */
	private Map<String,List<String[]>> valiTypeMap = new HashMap<String,List<String[]>>();
	/**
	 * valiTypeMap:TODO(各字段转换规则).
	 * @since JDK 1.6
	 */
	private Map<String,List<String[]>> TranTypeMap = new HashMap<String,List<String[]>>();
	/**
	 * db:TODO(数据库，用于查询验证转换规则).
	 * @since JDK 1.6
	 */
	public static Database db = null;
	
	/**
	 * df:TODO(用单例).
	 * @since JDK 1.6
	 */
	private static DataFilter df = null;
	
	private DataFilter() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 *  单例
	 */
	public static DataFilter getInstance(){
		if(df == null){
			df = new DataFilter();
		}
		return df;
	}
	public static  Database getDB(){
		return getDb();
	}

	/**
	 * 根据参数类型会连接上配置文件中配置的数据库连接
	 */
	public void connect() {
		db = DSPDBUtil.getConn();
//		try {
//			db.connect();
//			this.log = new LogChannel( "dsp定制插件--连接业务库:"+db.getObjectName() );
//		} catch (KettleDatabaseException e) {
//			log.logError("数据库连接异常！", e.getCause());
//			DSPDBUtil.closeConn(db);
//		}
	}

	/**
	 * 关闭数据库连接
	 */
	public void disconnect() {
		DSPDBUtil.closeConn(db);
	}

	/**
	 * 过滤当前数据row，符合规则返回true
	 * 
	 * @param jobId
	 *            任务ID
	 * @param row
	 *            表输入数据行
	 * @param sql
	 *            查询SQL
	 * @return
	 */
	public synchronized boolean doFilter(String jobId, Object[] row, String sql) {
		String colName = null;
			// 循环当前记录的每一个字段
		for (int i = 0; i < columns.length - 1; i++) {
			colName = columns[i];
			Object colVal = row[i];// 对应字段的值
			if (CollectionUtils.isNotEmpty(valiTypeMap.get(colName))) {
				for (int j = 0; j < valiTypeMap.get(colName).size(); j++) {// 防止一个字段有多种过滤规则
					String[] objs = valiTypeMap.get(colName).get(j);// 该字段的过滤规则
					if (!filterdata(colVal, objs[0], objs[1])) {
						log.logBasic(" column:" + colName
								+ " data:" + colVal + " valiType:"
								+ objs[0] + " rule:" + objs[1] + " Wrong!!!");
						// 标志该条数据错误
						row[columns.length - 1] = DSPContents.WhetherType.WHETHER_NO;
						return false;
					}
				}
			}
		}
		row[columns.length - 1] = DSPContents.WhetherType.WHETHER_YES;
		log.logDebug(" data:" + Arrays.asList(row)+ "验证通过！！");
		return true;
	}

	/**
	 * 当前转换数据row，转换完以后把row的数据重新替换
	 * 
	 * @param jobId
	 *            任务ID
	 * @param row
	 *            表输入数据行
	 * @param sql
	 *            查询SQL
	 * @return
	 */
	public synchronized Object[] doTrans(String jobId, Object[] row, String sql) {
		String colName = null;
		Object[] retRow = new Object[row.length];
		try {
			for (int i = 0; i < columns.length - 1; i++) {// 循环当前记录的每一个字段
				colName = columns[i];
				Object colVal = row[i];// 对应字段的值
				// 判断该字段是否需要转换
				if (CollectionUtils.isNotEmpty(TranTypeMap.get(colName))) {
					Object retval = "";
					for (String[] objects : TranTypeMap.get(colName)) {
						retval = dataTransByType(colVal, objects[0], objects[1]);
					}
					retRow[i] = retval;
				}else{
					//add by llchen 没做任何转换，则把原值赋值回去
					retRow[i] = row[i];
				}
			}
		} catch (DSPTabInputException e) {
			log.logError(e.getMessage());
			// 标志该条数据错误
			row[columns.length - 1] = DSPContents.WhetherType.WHETHER_NO;
			return null;
		} finally {
//			DSPDBUtil.closeConn(db);
		}
		retRow[columns.length - 1] = DSPContents.WhetherType.WHETHER_YES;
		log.logDebug(" data:" + Arrays.asList(row)+ "转换成功！！");
		return retRow;
	}

	/**
	 * 根据字段、验证类型、验证规则进行验证
	 * 
	 * @param column
	 *            该条数据的字段
	 * @param valiType
	 *            该字段的验证类型
	 * @param rule
	 *            该字段验证规则
	 * @return
	 */
	private boolean filterdata(Object column, String valiType, String rule) {
		log.logDebug("start validate,column:" + column + " valiType:" + valiType
				+ " rule:" + rule);
		boolean ret = false;
		try {
			if (DSPContents.FilterType.LEN_FILTER.equals(valiType)) {
				ret = lengthFilter(column, rule);
			} else if (DSPContents.FilterType.DATE_FILTER.equals(valiType)) {
				ret = dateFilter(column, rule);
			} else if (DSPContents.FilterType.ID_FILTER.equals(valiType)) {
				ret = IDFilter(column, rule);
			} else {
				//其他类型验证
			}
		} catch (DSPTabInputException e) {
			log.logError(e.getMessage());
			return false;
		}
		log.logDebug("end validate,result is :" + ret);
		return ret;
	}

	/**
	 * 根据字段、验证类型、验证规则进行转换
	 * 
	 * @param column
	 *            该条数据的字段
	 * @param valiType
	 *            该字段的验证类型
	 * @param rule
	 *            该字段转换规则
	 * @return
	 * @throws DSPTabInputException
	 */
	private Object dataTransByType(Object column, String valiType, String rule)
			throws DSPTabInputException {
		log.logDebug("start validate,column:" + column + " valiType:" + valiType
				+ " rule:" + rule);
		Object ret = null;
		if (DSPContents.TransType.REMOVE_ALL_BLANK_TRANS.equals(valiType)) {
			// 去掉字符串所有空格
			ret = replaceAllTrand(column);
		} else if (DSPContents.TransType.REMOVE_BEFORE_BLANK_TRANS
				.equals(valiType)) {
			// 去掉字符串首尾空格
			ret = trimStrTrans(column);
		} else if (DSPContents.TransType.DATE_TRANS.equals(valiType)) {
			// 时间转换
			ret = dateTrans(column, rule);
		} else if (DSPContents.TransType.ID_TRANS.equals(valiType)) {
			// 身份证转换
			ret = idTrans(column);
		} else if (DSPContents.TransType.REPLACE_TRANS.equals(valiType)) {
			// 字符串替换
			ret = replaceStrTrans(column, rule);
		} else if (DSPContents.TransType.DEFAULT_TRANS.equals(valiType)) {
			// 默认值转换
			ret = defaultTras(rule);
		}
		log.logDebug("end trans,result is :" + ret);
		return ret;
	}

	/**
	 * 按照自定规则替换字符串(规则最大长度为60，必须包含'/')
	 * 
	 * @param column
	 * @param rule
	 * @return
	 * @throws DSPTabInputException
	 */
	public String replaceStrTrans(Object column, String rule)
			throws DSPTabInputException {
		if (column == null) {
			return "";
		}
		if(StringUtils.isEmpty(rule)){
			return String.valueOf(column);
		}
		String val = String.valueOf(column);
		if (StringUtils.isEmpty(rule) || rule.indexOf("/") == -1) {
			return val;
		}
		if (rule.length() > 60) {
			throw new DSPTabInputException("规则长度超过限制");
		}
		String[] str = rule.split("/");
		if (str.length == 1) {
			return val.replaceAll(str[0], "");
		} else {
			return val.replaceAll(str[0], str[1]);
		}
	}

	/**
	 * 默认值转换，文本输入，长度不超过30，填写@systemdate，为系统时间
	 * 
	 * @param rule
	 * @return
	 * @throws DSPTabInputException
	 */
	public String defaultTras(String rule)
			throws DSPTabInputException {
		if (StringUtils.isEmpty(rule)) {
			return "";
		}
		if (DSPContents.SYSDATE_STR.equals(rule.toUpperCase())) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			Calendar cal = Calendar.getInstance();
			return sdf.format(cal.getTime());
		}
		if (rule.length() > 30) {
			throw new DSPTabInputException("数据长度超过最大限制");
		}
		return rule;
	}

	/**
	 * 去掉字符串中所有的空格
	 * 
	 * @param column
	 * @param rule
	 * @return
	 */
	public String replaceAllTrand(Object column) {
		if (column == null) {
			return "";
		}
		String val = String.valueOf(column);
		return val.replaceAll("\\s*", "");
	}

	/**
	 * 去掉字符串首尾空格
	 * 
	 * @param column
	 * @param rule
	 * @return
	 */
	public String trimStrTrans(Object column) {
		if (column == null) {
			return "";
		}
		String val = String.valueOf(column);
		return val.trim();
	}

	/**
	 * 日期字段格式转换
	 * 
	 * @param column
	 *            日期
	 * @param rule
	 *            转换规则
	 * @return
	 * @throws DSPTabInputException
	 */
	private Object dateTrans(Object column, String rule) throws DSPTabInputException {
		if (column == null) {
			throw new DSPTabInputException("时间数据为空");
		}
		if (StringUtils.isEmpty(rule)) {
			throw new DSPTabInputException("时间数据为空");
		}
		SimpleDateFormat sdf = null;
		
		 //根据rule代码转化成具体格式
		if(DSPContents.DATE_TYPE.FORMAT_14.equals(rule)){
			rule = "yyyyMMddHHmmss";
		}else if(DSPContents.DATE_TYPE.FORMAT_19.equals(rule)){
			rule = "yyyy-MM-dd HH:mm:ss";
		}else if(DSPContents.DATE_TYPE.FORMAT_10.equals(rule)){
			rule = "yyyy-MM-dd";
		}else if(DSPContents.DATE_TYPE.FORMAT_8.equals(rule)){
			rule = "yyyyMMdd";
		}
		
		
		if (column instanceof String) {
			try {
//				rule = rule.replace("24", "");
//				rule = rule.replaceAll("mi", "mm");
//				rule = rule.replaceAll("DD", "dd");
//				rule = rule.replaceAll("SS", "ss");
				sdf = new SimpleDateFormat(rule);
				return sdf.format(parseStringToDate(String.valueOf(column)));
			} catch (ParseException e) {
				throw new DSPTabInputException("日期格式化异常", e);
			}
		} else if (column instanceof Timestamp) {
			try {
				sdf = new SimpleDateFormat(rule);
				return new Timestamp(sdf.parse(sdf.format(column)).getTime());
			} catch (ParseException e) {
				throw new DSPTabInputException("日期格式化异常", e);
			}
		} else if(column instanceof Date){
			try {
				sdf = new SimpleDateFormat(rule);
				return sdf.parse(sdf.format(column));
			} catch (ParseException e) {
				throw new DSPTabInputException("日期格式化异常", e);
			}
		}else{
			return column;
		}
//		return sdf.format(column);
	}

	/**
	 * 身份证号码15位转18位
	 * 
	 * @param idCard
	 * @return
	 * @throws ParseException
	 * @throws DSPTabInputException
	 */
	private String idTrans(Object idCard) throws DSPTabInputException {
		if (idCard == null) {
			throw new DSPTabInputException("身份证号码为空");
		}
		String val = String.valueOf(idCard);
		if (val.length() == 18) {
			return val.toUpperCase();
		}
		return IDCardUtil.conver15CardTo18(val);
	}

	/**
	 * ID过滤
	 * 
	 * @param ID
	 * @param rule
	 * @return
	 * @throws DSPTabInputException
	 */
	private boolean IDFilter(Object column, String rule) throws DSPTabInputException {
		if (column == null) {
			throw new DSPTabInputException("身份证号码为空");
		}
		String val = String.valueOf(column);
		boolean retFlag = false;
		try {
			if (IDCardUtil.IDCardValidate(val)) {
				retFlag = true;
			}
		} catch (DSPTabInputException e) {
			throw new DSPTabInputException(e.getMessage(), e);
		}
		log.logDebug("IDCard:" + val + " type:" + retFlag);
		return retFlag;
	}

	/**
	 * 长度验证
	 * 
	 * @param column
	 *            待验证字段
	 * @param rule
	 *            长度数字
	 * @return
	 * @throws DSPTabInputException
	 */
	private boolean lengthFilter(Object column, String rule)
			throws DSPTabInputException {
		if (column == null) {
			throw new DSPTabInputException("数据为空！");
		}
//		log.logDebug("column:" + column + " length:" + rule);
		String val = String.valueOf(column);
		//TODO jingma:验证长度，对应为空的字段认为是符合规则的是否合理
		if (StringUtils.isEmpty(val)) {
			return true;
		}
		if (val.length() == Integer.valueOf(rule)) {
			return true;
		}
		return false;
	}

	/**
	 * 时间验证
	 * 
	 * @param date
	 * @param rule
	 * @return
	 * @throws DSPTabInputException
	 */
	private boolean dateFilter(Object date, String rule)
			throws DSPTabInputException {
		if (date == null) {
			throw new DSPTabInputException("时间数据为空！");
		}
		
		int len = 0;
		//add by llchen 2014-12-27
	    //根据rule代码转化成具体格式
		if(DSPContents.DATE_TYPE.FORMAT_14.equals(rule)){
			rule = "yyyyMMddHHmmss";
			len = 14;
		}else if(DSPContents.DATE_TYPE.FORMAT_19.equals(rule)){
			rule = "yyyy-MM-dd HH:mm:ss";
			len = 19;
		}else if(DSPContents.DATE_TYPE.FORMAT_10.equals(rule)){
			rule = "yyyy-MM-dd";
			len = 10;
		}else if(DSPContents.DATE_TYPE.FORMAT_8.equals(rule)){
			rule = "yyyyMMdd";
			len = 8;
		}
		
		log.logDebug("date:" + date + " rule:" + rule);
		SimpleDateFormat dateFormat = new SimpleDateFormat(rule);
		dateFormat.setLenient(false);
		try {
			if (date instanceof Date) {
				return true;
			} else if (date instanceof String) {
				//字符串类型时间，长度不相等，直接return false
				if(len != String.valueOf(date).length()){
					return false;
				}else{
					//长度相等后再format，能正常format，说明数据是正常的
					dateFormat.parse(String.valueOf(date));
				}
			}
			return true;
		} catch (ParseException e) {
			throw new DSPTabInputException("日期转换异常！", e);
		}
	}

	/**
	 * 通过SQL获取到查询的字段
	 * 
	 * @param sql
	 * @return
	 */
	private String[] getColumnBySQL(String sql) {
		log.logDebug("The select SQL:" + sql);
		SqlParser sp = new SqlParser(sql);
		String cols = sp.getCols();
		
		String tempSql = "select " + cols + " from dual";
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		List<String> list = new LinkedList<String>();
		try {
			statement = pm.parse(new StringReader(tempSql));
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		if (statement instanceof Select) {
			Select selectStatement = (Select) statement;
			OutColumnsFinder outColumnsFinder = new OutColumnsFinder();
			outColumnsFinder.getColumns(selectStatement);
			//解析sql得到的输出字段的集合
			List<String> selectItems = outColumnsFinder.getSelectItems();
			for (int i = 0; i < selectItems.size(); i++) {
				//interface中严格规定as统一为大写,这里不考虑小写的情况
				if(selectItems.get(i).contains(" AS ")){
					list.add((selectItems.get(i).substring(selectItems.get(i).lastIndexOf(" AS ")+3, selectItems.get(i).length())).trim());
				}else{
					list.add(selectItems.get(i).trim());
				}
			}
			
		}
		
//		String[] columns = sp.getCols().split(",");
//		if (columns != null) {
//			for (int i = 0; i < columns.length; i++) {
//				String col = columns[i];
//				if (col.indexOf(" ") != -1) {
//					String[] split1 = col.split(" ");
//					columns[i] = split1[split1.length - 1].trim();
//				}
//				if (col.indexOf(" as ") != -1) {
//					String[] split1 = col.split(" as ");
//					columns[i] = split1[split1.length - 1].trim();
//				}
//
//			}
//		}
//		return columns;
		String[] columns = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			columns[i] = list.get(i);
		}
		return columns;
	}

	/**
	 * 根据JOBID和节点类型nodetype查询业务库的规则，判断该组数据是否许需要验证转换
	 * 
	 * @param jobId
	 * @return
	 */
	public synchronized boolean isValidate(String jobId, String nodeType) {
		StringBuffer sb = new StringBuffer();
		sb.append(" select distinct(job_entry_type) ");
		sb.append(" from t_dsp_job job, t_dsp_job_entry en, t_dsp_validate_trans vr, t_dsp_validate_rule vrr ");
		sb.append(" where job.id=en.job_id  and en.id=vr.job_entry_id  and vr.id=vrr.validate_trans_id ");
		sb.append(" and job.kettle_job_id=").append(jobId);
		sb.append(" and en.entry_type='").append(nodeType).append("' ");
		sb.append(" and vrr.rule_dm<>'0' ");
		sb.append(" and job.del_flag='0'");
		try {
			List<Object[]> l = db.getRows(sb.toString(), 0);
			if (CollectionUtils.isNotEmpty(l)) {// 不为空返回true
				return true;
			}
			log.logDebug("jobId:" + jobId + " has no rule");
		} catch (KettleDatabaseException e) {// 异常关闭数据流返回false
			log.logError("根据jobid:" + jobId + "查询规则异常"+e.getMessage());
			return false;
		}
		return false;
	}

	/**
	 * 通过jobid和节点类型查询该节点的错误数据是否入库
	 * 
	 * @param jobId
	 * @param nodeType
	 * @return
	 */
	public synchronized boolean transIsWrongIn(String jobId, String nodeType) {
		//modify by llchen 2014-12-26
		//根据资源库id和节点类型获取该抽取任务下转换/验证是否设置了错误数据入库
		StringBuffer sb = new StringBuffer();
		sb.append(" select vr.do_type from t_dsp_job job, t_dsp_job_entry en,  t_dsp_validate_trans vr ");
		sb.append(" where job.id=en.job_id  and en.id = vr.job_entry_id ");
		sb.append(" and en.entry_type='").append(nodeType).append("' ");
		sb.append(" and job.kettle_job_id=").append(jobId);
		sb.append(" and job.del_flag='0' ");
		try {
			List<Object[]> l = db.getRows(sb.toString(), 0);
			if (CollectionUtils.isEmpty(l)) {
				return false;
			}
			Object[] type = l.get(0);
			if (DSPContents.WhetherType.WHETHER_YES.equals(String.valueOf(type[0]))) {//1：错误数据入库
				return true;
			}
		} catch (KettleDatabaseException e) {
			log.logError("根据jobid:" + jobId + "，节点类型：" + nodeType + "查询转换错误日志是否入库异常"+e.getMessage());
		}
		return false;
	}
	
	
	public synchronized boolean validateIsWrongIn(String jobId, String nodeType) {
		//modify by llchen 2014-12-26
		//根据资源库id和节点类型获取该抽取任务下转换/验证是否设置了错误数据入库
		StringBuffer sb = new StringBuffer();
		sb.append(" select vr.do_type from t_dsp_job job, t_dsp_job_entry en,  t_dsp_validate_trans vr ");
		sb.append(" where job.id=en.job_id  and en.id = vr.job_entry_id ");
		sb.append(" and en.entry_type='").append(nodeType).append("' ");
		sb.append(" and job.kettle_job_id=").append(jobId);
		sb.append(" and job.del_flag='0' ");
		try {
			List<Object[]> l = db.getRows(sb.toString(), 0);
			if (CollectionUtils.isEmpty(l)) {
				return false;
			}
			Object[] type = l.get(0);
			if (DSPContents.WhetherType.WHETHER_YES.equals(String.valueOf(type[0]))) {//1：错误数据入库
				return true;
			}
		} catch (KettleDatabaseException e) {
			log.logError("根据jobid:" + jobId + "，节点类型：" + nodeType + "查询验证错误日志是否入库异常"+e.getMessage());
		}
		return false;
	}
	
	/**
	 * 将未指定格式的日期字符串转化成java.util.Date类型日期对象，日期避免使用一些不常用的格式如：9/17/2014 <br>
	 * CreateDate：2014-11-25 <br>
	 * 
	 * @param date
	 *            ,待转换的日期字符串
	 * @return
	 * @throws ParseException
	 */
	public Date parseStringToDate(String date) throws ParseException {
		String parse = date;
		DateFormat format = null;
		//modify by llchen 含有'/'或'-',用一下方法处理 ,不支持只有时间分秒的时间
		if(parse.contains("/") || parse.contains("-")){
			parse = parse.replaceFirst("^[0-9]{4}([^0-9]?)", "yyyy$1");
			parse = parse.replaceFirst("^[0-9]{2}([^0-9]?)", "yy$1");
			parse = parse.replaceFirst("([^0-9]?)[0-9]{1,2}([^0-9]?)", "$1MM$2");
			parse = parse.replaceFirst("([^0-9]?)[0-9]{1,2}( ?)", "$1dd$2");
			parse = parse.replaceFirst("( )[0-9]{1,2}([^0-9]?)", "$1HH$2");
			parse = parse.replaceFirst("([^0-9]?)[0-9]{1,2}([^0-9]?)", "$1mm$2");
			parse = parse.replaceFirst("([^0-9]?)[0-9]{1,2}([^0-9]?)", "$1ss$2");	
			format = new SimpleDateFormat(parse);
		}else{
			date = allDataString(date);
			//默认14位，不够则补齐14位
			 format = new SimpleDateFormat("yyyyMMddHHmmss");
		}

		return format.parse(date);
	}



	/**
	 * 
	 *  格式化日期字符串 yyyymmddhh24miss
	 *  @param date
	 *  @return
	 *  @author llchen@iflytek.com
	 *  @created 2014-12-28 下午04:12:52
	 *  @lastModified
	 *  @history
	 */
	public static String formatDS(String date) {
		if (date == null)
			return "";
		return date.replace("-", "").replace("/", "").replace(":", "").replace(
				" ", "");
	}

	/**
	 * 
	 *  将日期补全，如果是14位则不补全，如果是8位“yyyyMMdd”或"yyyyMMddHH"或“yyyyMMddHHmm”格式，刚补全成“
	 *  yyyyMMddHHmsss”格式
	 *  @param date 可以是“yyyy-mm-dd hh24:mi:ss”也可以是“yyyyMMdd”或"yyyyMMddHH"或“yyyyMMddHHmm”
	 *  @return “yyyyMMddHHmmss”
	 *  @author llchen@iflytek.com
	 *  @created 2014-12-28 下午04:12:25
	 *  @lastModified
	 *  @history
	 */
	public static String allDataString(String date) {
		if (StringUtils.isBlank(date)) {
			return date;
		} else {
			date = formatDS(date);
			int strLength = date.length();
			if (strLength < 14) {
				for (int i = 0; i < 14 - strLength; i++) {
					date = date + "0";
				}
			}
			return date;
		}
	}
	public static void main(String[] args) {
		// DataFilter df = new DataFilter(DCContents.DBtype.ORACLE);
		// String[] cols =
		// df.getColumnBySQL("SELECT OBJECTID,LXJP,SM,MC FROM T_DSP_DICT");
		// for (int i = 0; i < cols.length; i++) {
		// System.out.println(cols[i]);
		// }
		// System.out.println(df.lengthFilter("低调点 ", "3"));
		// try {
		// //
		// System.out.println(IDCardUtil.IDCardValidate("44142119901015006x"));
		// System.out.println(IDCardUtil.IDCardValidate("33072419770516031X"));
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		DataFilter df = new DataFilter();
//		 try {
//		 System.out.println(df.dateTrans(new Date(Calendar.getInstance().getTimeInMillis()),
//		 "yyyyMMddHHmmss"));
//		 }catch (DSPTabInputException e) {
//		 // TODO Auto-generated catch block
//		 e.printStackTrace();
//		 }
		try {
			System.out.println(df.parseStringToDate("2014/12/28"));
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		try {
//			System.out.println(df.dateTrans("2014-09-17 16:20:11",
//					"yyyyMMdd HH:mm:ss"));
//		} catch (DSPTabInputException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		//测试sql
		// String[] cols =
		// df.getColumnBySQL("select a,b  b1,c as c1,d  as d1,e e1 from tab");
//		String[] cols = df
//				.getColumnBySQL("select mc,'0' column_falg\r\n  from zycx_zd_gj t");
		// for (int i = 0; i < cols.length; i++) {
		// System.out.println(cols[i]);
		// }
		
//		Calendar cal = Calendar.getInstance();
//		System.out.println(cal.getTime());
		
	}

	/**
	 * @return 返回 db.
	 */
	public static Database getDb() {
		return db;
	}

	/**
	 * @param db 设置 db
	 */
	public void setDb(Database db) {
		DataFilter.db = db;
	}

	/**
	 * init:(初始化). <br/>
	 * @author jinjuma@yeah.net
	 * @param sql 表输出的查询语句
	 * @param jobId 对应的jobid
	 * @return 初始化结果
	 * @since JDK 1.6
	 */
	public boolean init(String sql, String jobId) {
		columns = getColumnBySQL(sql);
		//列名
		String colName=null;
		//列对应规则列表
		List<Object[]> list=null;
		//用于组建sql
		StringBuffer sb;
		//数据库查询结果
		Object[] objs;
		//验证转换类型
		String type;
		//验证转换规则
		String rule;
		// 循环当前记录的每一个字段
		for (int i = 0; i < columns.length - 1; i++) {
			colName = columns[i];
			//modify by llchen 2014-12-26
			//获取某个字段绑定的规则列表
			sb = new StringBuffer();
			sb.append(" select vr.rule_dm,vr.value,trs.job_entry_type from t_dsp_job job,t_dsp_job_entry en,t_dsp_tableinput inp, t_dsp_tableinput_detail de, ");
			sb.append(" t_dsp_validate_rule vr, t_dsp_validate_trans trs where job.id = en.job_id and en.id=inp.job_entry_id ");
			sb.append(" and inp.id=de.tableinput_id and de.id=vr.column_id and vr.validate_trans_id=trs.id "); 
			sb.append(" and job.kettle_job_id=").append(jobId);
			sb.append(" and job.del_flag='0' ");
//			sb.append(" and trs.job_entry_type='").append(DSPContents.NodeType.FILTER_TYPE).append("' ");
			sb.append(" and de.column_name ='").append(colName.toUpperCase() ).append("' ");
			sb.append(" and vr.rule_dm<>'0'");
			
			try {
				list = db.getRows(sb.toString(), 100);
			} catch (KettleDatabaseException e) {
				log.logError("查询"+colName+"的过滤转换规则失败", e);
				return false;
			}
			if (CollectionUtils.isNotEmpty(list)) {
				log.logDebug("columnName : " + colName + " has " + list.size()+ " rules");
				valiTypeMap.put(colName, new ArrayList<String[]>());
				TranTypeMap.put(colName, new ArrayList<String[]>());
				for (int j = 0; j < list.size(); j++) {// 防止一个字段有多种过滤规则
					objs = list.get(j);// 该字段的过滤规则
					type = String.valueOf(objs[0]).trim();// 规则类型
					rule = String.valueOf(objs[1]).trim();// 规则
					//过滤规则
					if(DSPContents.NodeType.FILTER_TYPE.equals(objs[2])){
						valiTypeMap.get(colName).add(new String[]{type,rule});
					//转换规则
					}else if(DSPContents.NodeType.TRANS_TYPE.equals(objs[2])){
						TranTypeMap.get(colName).add(new String[]{type,rule});
					}
				}
			}
		}
		return true;
		
	}
	
	
	
}
