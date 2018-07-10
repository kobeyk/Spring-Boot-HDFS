package com.appleyk.model;

public class HDFSFileStatus {

	// 文件或目录路径
	private String path;
	// hdfs副本数
	private Short replication;
	// 是否是目录
	private boolean isDirectory;
	// 文件或目录长度
	private long len;
	// 文件大小
	private String size;
	// 当前所属用户
	private String owner;
	// 当前所属用户组
	private String group;
	// 权限
	private String permission;
	// 创建时间
	private long accessTime;
	// 最后修改时间
	private long modificationTime;
	// NameNode块大小
	private long blockSize;
	// 读权限
	private boolean readAccess;
	// 写权限
	private boolean writeAccess;
	// 执行权限
	private boolean executeAcess;

	
	public HDFSFileStatus(){
		
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Short getReplication() {
		return replication;
	}

	public void setReplication(Short replication) {
		this.replication = replication;
	}

	public boolean isDirectory() {
		return isDirectory;
	}

	public void setDirectory(boolean isDirectory) {
		this.isDirectory = isDirectory;
	}

	public long getLen() {
		return len;
	}

	public void setLen(long len) {
		this.len = len;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getPermission() {
		return permission;
	}

	public void setPermission(String permission) {
		this.permission = permission;
	}

	public long getAccessTime() {
		return accessTime;
	}

	public void setAccessTime(long accessTime) {
		this.accessTime = accessTime;
	}

	public long getModificationTime() {
		return modificationTime;
	}

	public void setModificationTime(long modificationTime) {
		this.modificationTime = modificationTime;
	}

	public long getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}

	public boolean isReadAccess() {
		return readAccess;
	}

	public void setReadAccess(boolean readAccess) {
		this.readAccess = readAccess;
	}

	public boolean isWriteAccess() {
		return writeAccess;
	}

	public void setWriteAccess(boolean writeAccess) {
		this.writeAccess = writeAccess;
	}

	public boolean isExecuteAcess() {
		return executeAcess;
	}

	public void setExecuteAcess(boolean executeAcess) {
		this.executeAcess = executeAcess;
	}

}
