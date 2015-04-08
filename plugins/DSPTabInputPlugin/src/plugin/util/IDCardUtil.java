package plugin.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

import plugin.exception.DSPTabInputException;

/**
 * 
 * @author qisun2
 * @email qisun2@iflytek.com
 * @createTime: 2014年11月27日 11:15:05
 * @discription 身份证验证和15位身份证号码转18位
 * 
 */
public class IDCardUtil {
	
	private static LogChannelInterface log =  new LogChannel("dsp身份证验证");
	
	/*********************************** 身份证验证开始 ****************************************/
	/**
	 * 身份证号码验证 1、号码的结构 公民身份号码是特征组合码，由十七位数字本体码和一位校验码组成。排列顺序从左至右依次为：六位数字地址码，
	 * 八位数字出生日期码，三位数字顺序码和一位数字校验码。 2、地址码(前六位数）
	 * 表示编码对象常住户口所在县(市、旗、区)的行政区划代码，按GB/T2260的规定执行。 3、出生日期码（第七位至十四位）
	 * 表示编码对象出生的年、月、日，按GB/T7408的规定执行，年、月、日代码之间不用分隔符。 4、顺序码（第十五位至十七位）
	 * 表示在同一地址码所标识的区域范围内，对同年、同月、同日出生的人编定的顺序号， 顺序码的奇数分配给男性，偶数分配给女性。 5、校验码（第十八位数）
	 * （1）十七位数字本体码加权求和公式 S = Sum(Ai * Wi), i = 0, ... , 16 ，先对前17位数字的权求和
	 * Ai:表示第i位置上的身份证号码数字值 Wi:表示第i位置上的加权因子 Wi: 7 9 10 5 8 4 2 1 6 3 7 9 10 5 8 4
	 * 2 （2）计算模 Y = mod(S, 11) （3）通过模得到对应的校验码 Y: 0 1 2 3 4 5 6 7 8 9 10 校验码: 1 0
	 * X 9 8 7 6 5 4 3 2
	 */

	/** 中国公民身份证号码最小长度。 */
	public static final int CHINA_ID_MIN_LENGTH = 15;

	/** 中国公民身份证号码最大长度。 */
	public static final int CHINA_ID_MAX_LENGTH = 18;

	/** 每位加权因子 */
	public static final int Wi[] = { 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10,
			5, 8, 4, 2 };
	/** 第18位校检码 */
	public static final String ValCodeArr[] = { "1", "0", "X", "9", "8", "7",
			"6", "5", "4", "3", "2" };

	/**
	 * 功能：身份证的有效验证
	 * 
	 * @param IDStr
	 *            身份证号
	 * @return Boolean
	 * @throws ParseException
	 * @throws DSPTabInputException
	 */
	public static boolean IDCardValidate(String IDStr)
			throws DSPTabInputException {
		// String Ai = "";
		// // ================ 号码的长度 15位或18位 ================
		// if (StringUtils.isEmpty(IDStr)||(IDStr.length() != 15 &&
		// IDStr.length() != 18)) {
		// throw new DSPTabInputException("身份证号码长度应该为15位或18位。");
		// }
		// // =======================(end)========================
		//
		// // ================ 数字 除最后以为都为数字 ================
		// if (IDStr.length() == 18) {
		// Ai = IDStr.substring(0, 17);
		// } else if (IDStr.length() == 15) {
		// Ai = IDStr.substring(0, 6) + "19" + IDStr.substring(6, 15);
		// }
		// if (isNumeric(Ai) == false) {
		// throw new DSPTabInputException("身份证15位号码都应为数字 ; 18位号码除最后一位外，都应为数字。");
		// }
		// // =======================(end)========================
		//
		// // ================ 出生年月是否有效 ================
		// String strYear = Ai.substring(6, 10);// 年份
		// String strMonth = Ai.substring(10, 12);// 月份
		// String strDay = Ai.substring(12, 14);// 月份
		// if (isDate(strYear + "-" + strMonth + "-" + strDay) == false) {
		// throw new DSPTabInputException("身份证生日无效。");
		// }
		// GregorianCalendar gc = new GregorianCalendar();
		// SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
		// try {
		// if ((gc.get(Calendar.YEAR) - Integer.parseInt(strYear)) > 150
		// || (gc.getTime().getTime() - s.parse(
		// strYear + "-" + strMonth + "-" + strDay).getTime()) < 0) {
		// throw new DSPTabInputException("身份证生日不在有效范围。");
		// }
		// } catch (ParseException e) {
		// throw new DSPTabInputException("身份证号码年份获取错误",e);
		// }
		// if (Integer.parseInt(strMonth) > 12 || Integer.parseInt(strMonth) ==
		// 0) {
		// throw new DSPTabInputException("身份证月份无效");
		// }
		// if (Integer.parseInt(strDay) > 31 || Integer.parseInt(strDay) == 0) {
		// throw new DSPTabInputException("身份证日期无效");
		// }
		// // =====================(end)=====================
		//
		// // ================ 地区码时候有效 ================
		// @SuppressWarnings("rawtypes")
		// Hashtable h = GetAreaCode();
		// if (h.get(Ai.substring(0, 2)) == null) {
		// throw new DSPTabInputException("身份证地区编码错误。");
		// }
		// // ==============================================
		//
		// // ================ 判断最后一位的值 ================
		// int TotalmulAiWi = 0;
		// for (int i = 0; i < 17; i++) {
		// TotalmulAiWi = TotalmulAiWi
		// + Integer.parseInt(String.valueOf(Ai.charAt(i)))
		// * Integer.valueOf(Wi[i]);
		// }
		// int modValue = TotalmulAiWi % 11;
		// String strVerifyCode = ValCodeArr[modValue];
		// Ai = Ai + strVerifyCode;
		//
		// if (IDStr.length() == 18) {
		// if (!Ai.toUpperCase().equals(IDStr.toUpperCase())) {
		// throw new DSPTabInputException("身份证无效，不是合法的身份证号码");
		// }
		// } else {
		// return false;
		// }
		// // =====================(end)=====================
		// return true;
		String errorInfo = "";
		//TODO jingma:这些不该设为常量吗？？？
		String[] ValCodeArr = { "1", "0", "x", "9", "8", "7", "6", "5", "4",
				"3", "2" };
		String[] Wi = { "7", "9", "10", "5", "8", "4", "2", "1", "6", "3", "7",
				"9", "10", "5", "8", "4", "2" };
		String Ai = "";

		if (StringUtils.isBlank(IDStr)) {
			return false;
		}

		if (IDStr.startsWith("0")) {
			return false;
		}

		if (IDStr.startsWith("0")) {
			return false;
		}

		if ((IDStr.length() != 15) && (IDStr.length() != 18)) {
			errorInfo = "身份证号码长度应该为15位或18位。";
			log.logError(errorInfo);
			return false;
		}

		if (IDStr.length() == 18)
			Ai = IDStr.substring(0, 17);
		else if (IDStr.length() == 15) {
			Ai = IDStr.substring(0, 6) + "19" + IDStr.substring(6, 15);
		}

		if (Ai.endsWith("000")) {
			return false;
		}

		if (!(isNumeric(Ai))) {
			errorInfo = "身份证15位号码都应为数字 ; 18位号码除最后一位外，都应为数字。";
			log.logError(errorInfo);
			return false;
		}

		String strYear = Ai.substring(6, 10);
		String strMonth = Ai.substring(10, 12);
		String strDay = Ai.substring(12, 14);
		if (!(isDate(strYear + "-" + strMonth + "-" + strDay))) {
			errorInfo = "身份证生日无效。";
			log.logError(errorInfo);
			return false;
		}
		GregorianCalendar gc = new GregorianCalendar();
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
		try {
			if ((gc.get(1) - Integer.parseInt(strYear) > 150)
					|| (gc.getTime().getTime()
							- s.parse(strYear + "-" + strMonth + "-" + strDay)
									.getTime() < 0L)) {
				errorInfo = "身份证生日不在有效范围。";
				log.logError(errorInfo);
				return false;
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if ((Integer.parseInt(strMonth) > 12)
				|| (Integer.parseInt(strMonth) == 0)) {
			errorInfo = "身份证月份无效";
			log.logError(errorInfo);
			return false;
		}
		if ((Integer.parseInt(strDay) > 31) || (Integer.parseInt(strDay) == 0)) {
			errorInfo = "身份证日期无效";
			log.logError(errorInfo);
			return false;
		}

		Hashtable h = GetAreaCode();
		if (h.get(Ai.substring(0, 2)) == null) {
			errorInfo = "身份证地区编码错误。";
			log.logError(errorInfo);
			return false;
		}

		int TotalmulAiWi = 0;
		for (int i = 0; i < 17; ++i) {
			TotalmulAiWi += Integer.parseInt(String.valueOf(Ai.charAt(i)))
					* Integer.parseInt(Wi[i]);
		}

		int modValue = TotalmulAiWi % 11;
		String strVerifyCode = ValCodeArr[modValue];
		Ai = Ai + strVerifyCode;

		if (IDStr.length() == 18)
			if (!(Ai.equalsIgnoreCase(IDStr))) {
				errorInfo = "身份证无效，不是合法的身份证号码";
				log.logError(errorInfo);
				return false;
			} else {
				return true;
			}

		return true;
	}

	/**
	 * 功能：设置地区编码
	 * 
	 * @return Hashtable 对象
	 */
	@SuppressWarnings("unchecked")
	private static Hashtable GetAreaCode() {
		Hashtable hashtable = new Hashtable();
		hashtable.put("11", "北京");
		hashtable.put("12", "天津");
		hashtable.put("13", "河北");
		hashtable.put("14", "山西");
		hashtable.put("15", "内蒙古");
		hashtable.put("21", "辽宁");
		hashtable.put("22", "吉林");
		hashtable.put("23", "黑龙江");
		hashtable.put("31", "上海");
		hashtable.put("32", "江苏");
		hashtable.put("33", "浙江");
		hashtable.put("34", "安徽");
		hashtable.put("35", "福建");
		hashtable.put("36", "江西");
		hashtable.put("37", "山东");
		hashtable.put("41", "河南");
		hashtable.put("42", "湖北");
		hashtable.put("43", "湖南");
		hashtable.put("44", "广东");
		hashtable.put("45", "广西");
		hashtable.put("46", "海南");
		hashtable.put("50", "重庆");
		hashtable.put("51", "四川");
		hashtable.put("52", "贵州");
		hashtable.put("53", "云南");
		hashtable.put("54", "西藏");
		hashtable.put("61", "陕西");
		hashtable.put("62", "甘肃");
		hashtable.put("63", "青海");
		hashtable.put("64", "宁夏");
		hashtable.put("65", "新疆");
		hashtable.put("71", "台湾");
		hashtable.put("81", "香港");
		hashtable.put("82", "澳门");
		hashtable.put("91", "国外");
		return hashtable;
	}

	/**
	 * 功能：判断字符串是否为数字
	 * 
	 * @param str
	 * @return
	 */
	private static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (isNum.matches()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 功能：判断字符串是否为日期格式
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isDate(String strDate) {
		Pattern pattern = Pattern
				.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])|([1-2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
		Matcher m = pattern.matcher(strDate);
		if (m.matches()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 将15位身份证号码转换为18位
	 * 
	 * @param idCard
	 *            15位身份编码
	 * @return 18位身份编码
	 * @throws DSPTabInputException
	 * @throws ParseException
	 */
	public static String conver15CardTo18(String idCard)
			throws DSPTabInputException {
		if (!IDCardValidate(idCard)) {
			log.logError("不是正确的身份证格式");
			throw new DSPTabInputException("不是正确的身份证格式");
		}
//		// 获取出生年月日
//		String birthday = idCard.substring(6, 12);
//		Date birthDate = null;
//		try {
//			birthDate = new SimpleDateFormat("yyMMdd").parse(birthday);
//		} catch (ParseException e) {
//			throw new DSPTabInputException("身份证年月日获取错误", e);
//		}
//		Calendar cal = Calendar.getInstance();
//		if (birthDate != null)
//			cal.setTime(birthDate);
//		// 获取出生年(完全表现形式,如：2010)
//		String sYear = String.valueOf(cal.get(Calendar.YEAR));
//		idCard18 = idCard.substring(0, 6) + sYear + idCard.substring(8);
//		// 转换字符数组
//		char[] cArr = idCard18.toCharArray();
//		if (cArr != null) {
//			int[] iCard = converCharToInt(cArr);
//			int iSum17 = getPowerSum(iCard);
//			// 获取校验位
//			String sVal = getCheckCode18(iSum17);
//			if (sVal.length() > 0) {
//				idCard18 += sVal;
//			} else {
//				return null;
//			}
//		}
		
		 // 若是15位，则转换成18位；否则直接返回ID
        if (15 == idCard.length()) {
            final int[] W = { 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4,
                    2, 1 };
            final String[] A = { "1", "0", "X", "9", "8", "7", "6", "5", "4",
                    "3", "2" };
            int i, j, s = 0;
            String newid;
            newid = idCard;
            newid = newid.substring(0, 6) + "19"
                    + newid.substring(6, idCard.length());
            for (i = 0; i < newid.length(); i++) {

                j = Integer.parseInt(newid.substring(i, i + 1)) * W[i];
                s = s + j;
            }
            s = s % 11;
            newid = newid + A[s];
            return newid.toUpperCase();
        } else {
            return idCard.toUpperCase();
        }
	}

	/**
	 * 将字符数组转换成数字数组
	 * 
	 * @param ca
	 *            字符数组
	 * @return 数字数组
	 * @throws DSPTabInputException
	 */
	public static int[] converCharToInt(char[] ca) throws DSPTabInputException {
		int len = ca.length;
		int[] iArr = new int[len];
		try {
			for (int i = 0; i < len; i++) {
				iArr[i] = Integer.parseInt(String.valueOf(ca[i]));
			}
		} catch (NumberFormatException e) {
			log.logError("字符数组转换成数字数组异常");
			throw new DSPTabInputException("字符数组转换成数字数组异常", e);
		}
		return iArr;
	}

	/**
	 * 将身份证的每位和对应位的加权因子相乘之后，再得到和值
	 * 
	 * @param iArr
	 * @return 身份证编码。
	 */
	public static int getPowerSum(int[] iArr) {
		int iSum = 0;
		if (Wi.length == iArr.length) {
			for (int i = 0; i < iArr.length; i++) {
				for (int j = 0; j < Wi.length; j++) {
					if (i == j) {
						iSum = iSum + iArr[i] * Wi[j];
					}
				}
			}
		}
		return iSum;
	}

	/**
	 * 将power和值与11取模获得余数进行校验码判断
	 * 
	 * @param iSum
	 * @return 校验位
	 */
	public static String getCheckCode18(int iSum) {
		String sCode = "";
		switch (iSum % 11) {
		case 10:
			sCode = "2";
			break;
		case 9:
			sCode = "3";
			break;
		case 8:
			sCode = "4";
			break;
		case 7:
			sCode = "5";
			break;
		case 6:
			sCode = "6";
			break;
		case 5:
			sCode = "7";
			break;
		case 4:
			sCode = "8";
			break;
		case 3:
			sCode = "9";
			break;
		case 2:
			sCode = "x";
			break;
		case 1:
			sCode = "0";
			break;
		case 0:
			sCode = "1";
			break;
		}
		return sCode;
	}

	/**
	 * @param args
	 * @throws ParseException
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws ParseException {
		// String IDCardNum="2101028208264112";
		String IDCardNum = "21092119450504282X";
		// String IDCardNum = "320311770706201";

		IDCardUtil cc = new IDCardUtil();
		char[] a = { 'a', '2', '3' };
		try {
			System.out.println(cc.IDCardValidate(IDCardNum));
			// System.out.println(conver15CardTo18(IDCardNum));
			converCharToInt(a);
		} catch (Exception e) {
			System.out.println(e.getCause());
		}
	}
	/*********************************** 身份证验证结束 ****************************************/

}