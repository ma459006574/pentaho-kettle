package plugin.exception;

/**
 * 表输入自定义异常类
 * @author qisun2
 * @email qisun2@iflytek.com
 * @createTime: 2014年11月27日 11:15:05
 *
 */
@SuppressWarnings("serial")
public class DSPTabInputException extends Exception {
	
	public DSPTabInputException() {
		super();
	}
	
	public DSPTabInputException(String msg) {
		super(msg);
	}
	
	public DSPTabInputException(String msg,Throwable e) {
		super(msg,e);
	}
	
	

}
