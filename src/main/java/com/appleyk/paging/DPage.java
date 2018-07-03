package com.appleyk.paging;

import java.util.ArrayList;
import java.util.List;

import com.appleyk.model.FileStatusModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 对List集合进行分页  == 使用subList对list对象进行切割
 * @author yukun24@126.com
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年6月29日-下午3:13:36
 * @param <T>
 */
public class DPage<T> {

	//当前页
	private Integer pageNum;
	//总记录数
	private Integer total;
	//总页数
	private Integer pages;
	
	//当前页显示多少条记录
	@JsonIgnore
	private Integer pageSize;
	
	private List<T> list;
	
	public DPage(List<T> list,Integer pageNum,Integer pageSize){
	
		this.pageNum  = pageNum;
		this.pageSize = pageSize;
		this.total = list.size();
		
		//总记录数和每页显示的记录之间是否可以凑成整数（pages）
		boolean full = total % pageSize == 0;
			
		//分页 == 根据pageSize（每页显示的记录数）计算pages
		if(!full){
			//如果凑不成整数
			this.pages = total/pageSize + 1;
		}else{
			//如果凑成整数
			this.pages = total/pageSize;
		}
		
		int fromIndex = 0;
		int toIndex   = 0;
		fromIndex = pageNum*pageSize-pageSize;
		if(pageNum == 0){
			throw new ArithmeticException("第0页无法展示");
		}else if(pageNum>pages){
			//如果查询的页码数大于总的页码数，list设置为[]
			list = new ArrayList<>();
		}else if(pageNum == pages){
			//如果查询的当前页等于总页数，直接索引到total处
			toIndex = total;
		}else{		
			//如果查询的页码数小于总页数，不用担心切割List的时候toIndex索引会越界，直接等
			toIndex   = pageNum*pageSize;			
		}	
		
		if(list.size() == 0){
			this.list = list;
		}else{
			this.list = list.subList(fromIndex, toIndex);
		}
		
	}
	 

	
	public Integer getPageNum() {
		return pageNum;
	}


	public void setPageNum(Integer pageNum) {
		this.pageNum = pageNum;
	}


	public Integer getTotal() {
		return total;
	}


	public void setTotal(Integer total) {
		this.total = total;
	}


	public Integer getPages() {
		return pages;
	}


	public void setPages(Integer pages) {
		this.pages = pages;
	}


	public Integer getPageSize() {
		return pageSize;
	}


	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}


	public List<T> getList() {
		return list;
	}


	public void setList(List<T> list) {
		this.list = list;
	}




	public static void main(String[] args) throws Exception{
		
		ObjectMapper mapper = new ObjectMapper();
		List<FileStatusModel> data = new ArrayList<>();
		FileStatusModel model1 = new FileStatusModel();
		model1.setPath("A");
		FileStatusModel model2 = new FileStatusModel();
		model2.setPath("B");
		FileStatusModel model3 = new FileStatusModel();
		model3.setPath("C");
		FileStatusModel model4 = new FileStatusModel();
		model4.setPath("D");
		FileStatusModel model5 = new FileStatusModel();
		model5.setPath("E");
		FileStatusModel model6 = new FileStatusModel();
		model6.setPath("F");
		data.add(model1);data.add(model2);data.add(model3);data.add(model4);data.add(model5);data.add(model6);
		int pageNum = 13;
		int pageSize = 2; 
		System.out.println("pageNum: "+pageNum+",pageSize : "+pageSize);
		DPage<FileStatusModel> page = new DPage<>(data,pageNum, pageSize);
		System.out.println(mapper.writeValueAsString(page));
	} 
}
