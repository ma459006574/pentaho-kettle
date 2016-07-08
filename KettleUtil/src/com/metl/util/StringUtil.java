/**	
 * <br>
 * Copyright 2011 IFlyTek. All rights reserved.<br>
 * <br>			 
 * Package: com.iflytek.utils <br>
 * FileName: StringUtils.java <br>
 * <br>
 * @version
 * @author sbwang@iflytek.com
 * @created 2013-5-29
 * @last Modified 
 * @history
 */

package com.metl.util;

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * 字符处理类
 * 
 * @author sbwang@iflytek.com
 * @lastModified
 * @history
 */

public class StringUtil extends StringUtils{

    /**
     * 
     * 替换非法字符
     * 
     * @param str
     * @return
     * @author sbwang@iflytek.com
     * @created 2013-5-29 下午09:49:10
     * @lastModified
     * @history
     */
    public static String replace(String str) {
        if (StringUtils.isNotEmpty(str)) {
            /*
             * str = str.replace("'", "''").replace("]", "]]").replace("&",
             * "chr(38)").replace("%", "chr(37)").replace("\\", "chr(92)")
             * .replace("\"", "chr(34)").replace("_", "chr(95)");
             */
            str = str.replace("'", "''").replace("%", "\\%")
                    .replace("\\", "\\\\").replace("_", "\\_");
        }
        return str;
    }
    
    /**
     * 
     *  去掉尾部全部特定字符串
     *  @param str
     *  @param removeStr
     *  @return
     *  @author xkfeng@iflytek.com
     *  @created 2013-7-19 上午09:11:30
     *  @lastModified       
     *  @history
     */
    public static String removeEndStr(String str,String removeStr){
        String str2 = StringUtils.removeEnd(str, removeStr);
        while(!str2.equals(str)){
            str = str2;
            str2 = StringUtils.removeEnd(str, removeStr);
        }
        return str2;
    }
    
    /**
     * 将字符串转换成long型的数组 主要用于处理附件上传id从字符串转成long型
     * @param str 输入的字符串
     * @return long 型数组
     */
    public static Long[] convertStringToLongArray(String str){
    	String[] strArray = str.split(",");
    	//声明long型数组
    	Long[] strLongArray = new Long[strArray.length];
    	//进行转换
    	for (int i = 0; i < strArray.length; i++) {
			strLongArray[i] = Long.parseLong(strArray[i]);
		}
    	//返回
    	return strLongArray;
    }
    
    /**
	 * 获取限定长度的字符串
	 * @param str 字符串
	 * @param maxlength 限定的长度
	 * @return 限定长度的字符串
	 */
	public static String getLimitLengthString(String str,int maxlength)throws UnsupportedEncodingException{
		//创建临时的计数器
		int counterOfDoubleByte = 0;
		//将字符串转换成比特数组
		byte b[] = str.getBytes("gbk");
		//判断是否超出了指定的长度
		if(b.length < maxlength){
			//什么不操作，先返回
			return str; 
		}
		
		for(int i = 0; i < maxlength; i++){
		     if(b[i] < 0)
		       counterOfDoubleByte++;
		   }

	   if(counterOfDoubleByte % 2 == 0)
	     return new String(b, 0, maxlength, "gbk");
	   else
	     return new String(b, 0, maxlength - 1, "gbk");
	}
    
	/**
	 *  截取一段字符的长度(汉、日、韩文字符长度为2),不区分中英文,如果数字不正好，则少取一个字符位
	 * @param str 原始字符串
	 * @param srcPos 开始位置
	 * @param specialCharsLength 截取长度(汉、日、韩文字符长度为2)
	 * @return String
	 */
	public static String substring(String str, int srcPos, int specialCharsLength) {
		if (str == null || "".equals(str) || specialCharsLength < 1) {
			return "";
		}
		if (srcPos < 0) {
			srcPos = 0;
		}
		if (specialCharsLength <= 0) {
			return "";
		}
		// 获得字符串的长度
		char[] chars = str.toCharArray();
		if (srcPos > chars.length) {
			return "";
		}
		int charsLength = getCharsLength(chars, specialCharsLength);
		return new String(chars, srcPos, charsLength);
	}
	
	/**
	 * 获取一段字符的长度，输入长度中汉、日、韩文字符长度为2，输出长度中所有字符均长度为1
	 * @param chars
	 * @param specialCharsLength  输入长度，汉、日、韩文字符长度为2
	 * @return int 输出长度，所有字符均长度为1
	 */
	private static int getCharsLength(char[] chars, int specialCharsLength) {
		int count = 0;
		int normalCharsLength = 0;
		for (int i = 0; i < chars.length; i++) {
			int specialCharLength = getSpecialCharLength(chars[i]);
			if (count <= specialCharsLength - specialCharLength) {
				count += specialCharLength;
				normalCharsLength++;
			} else {
				break;
			}
		}
		return normalCharsLength;
	}
	
	/**
	 * 获取字符长度：汉、日、韩文字符长度为2，ASCII码等字符长度为1
	 * @param c
	 * @return int
	 */
	private static int getSpecialCharLength(char c) {
		if (isLetter(c)) {
			return 1;
		} else {
			return 2;
		}
	}
	/**
	 * @Title: isLetter
	 * @Description: 判断一个字符是Ascill字符还是其它字符
	 * @param c
	 * @return boolean
	 */
	public static boolean isLetter(char c) {
		int k = 0x80;
		return c / k == 0;
	}
	/**
	 * 校验字符串是否纯数字
	 * @param str
	 * @return
	 */
	public static Boolean validateNumber(String str){
		if(StringUtils.isBlank(str)){
			return false;
		}
		Pattern pattern = Pattern.compile("\\d+(\\.\\d+)?$");  
		Matcher matcher = pattern.matcher(str);  
		return matcher.matches();
	}
	/**
	 * 使用java正则表达式去掉多余的.与0
	 * @param s
	 * @return 
	 */
	public static String subZeroAndDot(String str){
		if(StringUtils.isNotBlank(str)){
			if(StringUtil.validateNumber(str)){
				if(str.contains(".")){
					str = str.replaceAll("[.]*?0+$", "");//去掉多余的0
					str = str.replaceAll("[.]$", "");//如最后一位是.则去掉
				}else {
					return str;
				}
			}
			return str;
		} else {
			return StringUtils.EMPTY;
		}
	}
	/**
	 * 将Double数值格式化后转换为字符串
	 * @param d {@link Double}
	 * @return
	 */
	public static String converDoubleToString(Double d){
		DecimalFormat df = new DecimalFormat("0.00000000000000000000");
		if(d != null){
			return subZeroAndDot(df.format(d));
		}
		return StringUtils.EMPTY;
	}
	/**
	 * 获取字符串长度（中文字符计为2位）
	 * @param value
	 * @return
	 */
	public static int getChineseLength(String value) {
        int valueLength = 0;
        String chinese = "[\u0391-\uFFE5]";
        /* 获取字段值的长度，如果含中文字符，则每个中文字符长度为2，否则为1 */
        if(StringUtils.isNotBlank(value)){
	        for (int i = 0; i < value.length(); i++) {
	            /* 获取一个字符 */
	            String temp = value.substring(i, i + 1);
	            /* 判断是否为中文字符 */
	            if (temp.matches(chinese)) {
	                /* 中文字符长度为2 */
	                valueLength += 2;
	            } else {
	                /* 其他字符长度为1 */
	                valueLength += 1;
	            }
	        }
        }
        return valueLength;
    }
	/**
	* 解析为int类型 <br/>
	* @author jingma@iflytek.com
	* @param obj
	* @return
	*/
	public static Integer parseInt(Object obj){
	    if(obj==null||isBlank(obj.toString())){
	        return 0;
	    }else{
	        return Integer.parseInt(obj.toString());
	    }
	}
    /**
    * 解析为long类型 <br/>
    * @author jingma@iflytek.com
    * @param obj
    * @return
    */
    public static long parseLong(Object obj){
        return parseInt(obj).longValue();
    }
}