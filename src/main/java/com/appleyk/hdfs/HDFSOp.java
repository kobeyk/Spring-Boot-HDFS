package com.appleyk.hdfs;


/**
 * HDFS 文件操作类 【包含一系列参数，待完善】
 * @author yukun24@126.com
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年6月29日-上午10:04:23
 */
public class HDFSOp {

	// 源路径
	private String srcPath;
	// 目标路径
	private String destPath;
	// 文件的操作类型
	private OP op;
	// 是否递归删除
	private boolean recursive = true;
	// 是否覆盖源文件
	private boolean override = true;
	// 是否删除文件的时候跳过回收站（true：删除文件的时候，会把文件临时存到回收站）
	private boolean skipTrash = false;
	// 写入或读取的文件内容
	private String content;
	// 文件过滤条件（用在获取文件列表的时候，过滤Path）
	private String regex;
	// 文件或目录的权限
	private String permission;

	public HDFSOp() {

	}

	public String getSrcPath() {
		return srcPath;
	}

	public void setSrcPath(String srcPath) {
		this.srcPath = srcPath;
	}

	public String getDestPath() {
		return destPath;
	}

	public void setDestPath(String destPath) {
		this.destPath = destPath;
	}

	public OP getOp() {
		return op;
	}

	public void setOp(OP op) {
		this.op = op;
	}

	public boolean isRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}

	public boolean isOverride() {
		return override;
	}

	public void setOverride(boolean override) {
		this.override = override;
	}

	public boolean isSkipTrash() {
		return skipTrash;
	}

	public void setSkipTrash(boolean skipTrash) {
		this.skipTrash = skipTrash;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public String getPermission() {
		return permission;
	}

	public void setPermission(String permission) {
		this.permission = permission;
	}
}
