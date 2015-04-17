package plugin.util;



import java.sql.Connection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.ConnectionPoolUtil;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * kettle自定义数据库连接
 * @author qisun2
 * @email qisun2@iflytek.com
 * @createTime: 2014年11月27日 11:15:05
 *
 */
public class DSPDBUtil {
	private static LogChannelInterface log;
	private static Database db = null;
	public static Connection connection ;
	/**
	 * 根据
	 * @param DBxml
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Database getConn(){
		
		try {
			DatabaseMeta databaseMeta  = getEncodeXML(); 
			if(connection == null ){
				KettleEnvironment.init();
				log = new LogChannel("dspDataSource");
				connection = ConnectionPoolUtil.getConnection(log, databaseMeta, "dsp");
				connection.setAutoCommit(true);
//				Statement stmt  = connection.createStatement();
//				ResultSet rs = stmt.executeQuery("");
				db = new Database(databaseMeta);
				db.setConnection(connection);
				//db.connect();
			}else{
			}
			
			//db.connect();
		} catch (Exception e) {
			log.logError(e.getMessage(), e.getCause());
		}
		return db;
	}
	
	public static DatabaseMeta getEncodeXML(){
		String type = PropertyUtil.getValue("db.type");
		if(StringUtils.isEmpty(type)){
			return null;
		}
		String name = PropertyUtil.getValue("db.jdbc.name");
		if(StringUtils.isEmpty(name)){
			return null;
		}
		String host = PropertyUtil.getValue("db.jdbc.host");
		if(StringUtils.isEmpty(host)){
			return null;
		}
		String port = PropertyUtil.getValue("db.jdbc.port");
		if(StringUtils.isEmpty(port)){
			return null;
		}
		String database = PropertyUtil.getValue("db.jdbc.database");
		if(StringUtils.isEmpty(database)){
			return null;
		}
		String user = PropertyUtil.getValue("db.jdbc.user");
		if(StringUtils.isEmpty(user)){
			return null;
		}
		String pass = PropertyUtil.getValue("db.jdbc.pass");
		if(StringUtils.isEmpty(pass)){
			return null;
		}
		DatabaseMeta dbMeta = new DatabaseMeta(new String(Base64.decode(name)), new String(Base64.decode(type)), "Native", 
				new String(Base64.decode(host)), new String(Base64.decode(database)), new String(Base64.decode(port)),
				new String(Base64.decode(user)), new String(Base64.decode(pass)));
		dbMeta.setInitialPoolSize(Integer.parseInt(new String(Base64.decode(PropertyUtil.getValue("db.jdbc.initialPoolSize")))));
		dbMeta.setMaximumPoolSize(Integer.parseInt(new String(Base64.decode(PropertyUtil.getValue("db.jdbc.maximumPoolSize")))));
		return dbMeta;
	}
	
	
	/**
	 * 调用
	 * @param Database自带数据连接关闭
	 */
	public static void closeConn(Database db){
		if(db != null){
			db.disconnect();
		}
		db = null;
	}
	
	
	public static void main(String[] args) {
		getConn();
		try {
			List<Object[]> l = db.getRows("select * from r_job where id_job="+101, 10);
//			List<Object[]> l1 = db.getRows("SELECT OBJECTID,LXJP,SM,MC FROM T_DSP_DICT", 0);
			if(CollectionUtils.isNotEmpty(l)){
				System.out.println(l.size());
			}
//			DCDBUtil.stopConn(db);
		} catch (KettleDatabaseException e) {
			e.printStackTrace();
//			DCDBUtil.stopConn(db);
		}
		System.out.println(getEncodeXML());
		
		
	System.out.println("01".replaceAll("0", "A"));
		
	}

	/**
	 * @return 返回 connection.
	 */
	public static Connection getConnection() {
		return connection;
	}

	/**
	 * @param connection 设置 connection
	 */
	public static void setConnection(Connection connection) {
		DSPDBUtil.connection = connection;
	}

}
