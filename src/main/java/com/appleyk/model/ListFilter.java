package com.appleyk.model;

public class ListFilter {

	//目录
	private String dirPath;
	//操作
	private Integer op;
	//过滤条件
	private String regex;
	//当前页码
	private Integer pageNum;
	//每页显示的记录数
	private Integer pageSize;
	
	public ListFilter(){
		//默认第一页
		this.pageNum  = 1  ;
		//默认一页显示25个
		this.pageSize = 25 ;
		this.op = 12;
	}

	public String getDirPath() {
		return dirPath;
	}

	public void setDirPath(String dirPath) {
		this.dirPath = dirPath;
	}



	public Integer getOp() {
		return op;
	}

	public void setOp(Integer op) {
		this.op = op;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public Integer getPageNum() {
		return pageNum;
	}

	public void setPageNum(Integer pageNum) {
		this.pageNum = pageNum;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}
	
}
