package plugin.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

/**
 * property文件属性值获取
 * @author qisun2
 * @email qisun2@iflytek.com
 * @createTime: 2014年11月27日 11:15:05
 */
public class PropertyUtil {
	private static Properties p = new Properties();
	static {
		try {
			// System.getProperty("user.dir") 获得项目的绝对路径，然后拼装配置文件的路径
			// 读取系统外配置文件 (即Jar包外文件) --- 外部工程引用该Jar包时需要在工程下创建config目录存放配置文件
//			String filePath = System.getProperty("user.dir")
//					+ "//src//plugin//util//message//DBconfig.properties";
			String filePath =  "/iflytek/config/dspPlugin/DBconfig.properties";
			InputStream in = new BufferedInputStream(new FileInputStream(
					filePath));
			p.load(in);
			in.close();
		} catch (IOException e) {
			System.out.println("读取配置信息出错！");
		}
	}

	/**
	 * 根据key得到value的值
	 */
	public static String getValue(String key) {
		return p.getProperty(key);
	}
	
	/**
	 * 根据配置的值返回boolean类型数据，1：true  0或者空为false
	 * @param key
	 * @return
	 */
	public static boolean getBooleanValue(String key){
		String val = getValue(key);
		if(StringUtils.isNotEmpty(val)){
			if("1".equals(val.trim())){
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		System.out.println(PropertyUtil.getValue("db.type"));
	}
	
}
