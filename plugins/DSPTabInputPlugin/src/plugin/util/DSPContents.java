package plugin.util;

/**
 * 数据过滤规则
 * @author qisun2
 * @email qisun2@iflytek.com
 * @createTime: 2014年11月27日 11:15:05
 *
 */
public class DSPContents {
	
	public static final String SYSDATE_STR = "@SYSTEMDATE";
	
	/**
	 * 节点类型
	 * @author qisun2
	 *
	 */
	public interface NodeType{
		/**
		 * 验证
		 */
		public static final String FILTER_TYPE = "2";
		/**
		 * 转换
		 */
		public static final String TRANS_TYPE = "3";
	}
	
	/**
	 * 操作类型   1是，2否;  正确错误：1：正确，2：错误
	 * @author qisun2
	 *
	 */
	public interface WhetherType{
		/**
		 * 是否类型：是
		 */
		public static final String WHETHER_YES = "1";
		/**
		 * 是否类型：否
		 */
		public static final String WHETHER_NO = "2";
	}
	
	/**
	 * 字段过滤类型
	 * @author qisun2
	 *
	 */
	public interface FilterType{
		/**
		 * 身份证类型字段
		 */
		public static final String ID_FILTER = "1";
		
		/**
		 * 日期类型字段
		 */
		public static final String DATE_FILTER = "2";
		
		/**
		 * 字符串类型字段，只需要做长度过滤
		 */
		public static final String LEN_FILTER = "3";
		
		/**
		 * 正则表达式过滤，只需要做长度过滤
		 */
		public static final String REGEX_FILTER = "4";
	}
	
	/**
	 * 字段转换类型
	 * @author qisun2
	 *
	 */
	public interface TransType{
		/**
		 * 身份证转换(15位转18位)
		 */
		public static final String ID_TRANS = "1";
		
		/**
		 * 日期格式转换
		 */
		public static final String DATE_TRANS = "2";
		
		/**
		 * 字符串空格移除--只移除前后空格
		 */
		public static final String REMOVE_BEFORE_BLANK_TRANS= "3";
		/**
		 * 字符串空格移除--移除全部空格
		 */
		public static final String REMOVE_ALL_BLANK_TRANS = "4";
		/**
		 * 字符串替换
		 */
		public static final String REPLACE_TRANS = "5";
		/**
		 * 默认值--文本输入，长度不超过30，填写@systemdate，为系统时间
		 */
		public static final String DEFAULT_TRANS = "6";
	}

    /**
     * 数据库类型
     */
    public interface DBtype{
    	public static final String ORACLE="ORACLE";
    	public static final String MYSQL="MYSQL";
    	public static final String SQLSERVER="SQLSERVER";
    }
    
    /**
	 * 数据库连接信息,适用于DatabaseMeta其中 一个构造器DatabaseMeta(String xml)
	 */
	 public interface DatabasesXML{
		 public static String ORACLE = 
				 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
						 "<connection>" +
						 "<name>11.171</name>" +
						 "<server>192.168.11.171</server>" +
						 "<type>ORACLE</type>" +
						 "<access>Native</access>" + 
						 "<port>1521</port>" +
						 "<database>iflytek</database>" +
						 "<username>sjzx</username>" +
						 "<password>EC82FCE050A</password>" +
						 "</connection>";
		 public static String MYSQL = 
				 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
						 "<connection>" +
						 "<name>local</name>" +
						 "<server>#IP#</server>" +
						 "<type>MYSQL</type>" +
						 "<access>Native</access>" + 
						 "<port>3306</port>" +
						 "<database>test</database>" +
						 "<username>root</username>" +
						 "<password>root</password>" +
						 "</connection>";
		public static String SQLSERVER = 
				 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
						 "<connection>" +
						 "<name>11.171</name>" +
						 "<server>192.168.11.171</server>" +
						 "<type>ORACLE</type>" +
						 "<access>Native</access>" + 
						 "<port>1521</port>" +
						 "<database>iflytek</database>" +
						 "<username>sjzx</username>" +
						 "<password>EC82FCE050A</password>" +
						 "</connection>";
		 	
	    };	
	    
	    
	    /**
	     * 
	     *  时间格式代码
	     *  @author llchen@iflytek.com
	     *  @created 2014-12-27 下午02:07:59
	     *  @lastModified       
	     *  @history
	     */
	    public interface DATE_TYPE{
	    	public String FORMAT_14 = "0";
			public String FORMAT_19 = "1";
			public String FORMAT_10 = "2";
			public String FORMAT_8 = "3";
	    }
	
}
