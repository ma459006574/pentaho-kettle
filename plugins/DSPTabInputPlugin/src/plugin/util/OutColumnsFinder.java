package plugin.util;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

/**
 * 
 *  sql过滤
 *  @author llchen@iflytek.com
 *  @created 2015-1-15 下午08:50:06
 *  @lastModified       
 *  @history
 */
public class OutColumnsFinder implements SelectVisitor{
	/**
	 * 输出字段list
	 */
	private List<String> selectItems = new ArrayList<String>();
	
	/**
	 * 解析输出字段(调用visit(PlainSelect plainSelect)方法)
	 * @param select
	 */
	public void getColumns(Select select){
		select.getSelectBody().accept(this);
	}
	
	/**
	 * 解析出来的输出字段  循环放入list中
	 */
	public void visit(PlainSelect plainSelect) {
		if(plainSelect.getSelectItems()!=null){
			for(int i=0;i<plainSelect.getSelectItems().size();i++){
				selectItems.add(plainSelect.getSelectItems().get(i).toString());
			}
		}
		
	}

	public void visit(Union arg0) {
		// TODO Auto-generated method stub
	}

	/**
	 * @return the selectItems
	 */
	public List<String> getSelectItems() {
		return selectItems;
	}

	/**
	 * @param selectItems the selectItems to set
	 */
	public void setSelectItems(List<String> selectItems) {
		this.selectItems = selectItems;
	}
}
