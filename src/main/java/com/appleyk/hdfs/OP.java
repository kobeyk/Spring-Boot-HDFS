package com.appleyk.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;


/**
 * HDFS 文件操作 类型表
 * @author yukun24@126.com
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年6月29日-上午9:50:40
 */
public enum OP {

	CREATE("创建文件/目录", 0), 
	RENAME("重命名文件/目录", 1),
	COPY("复制文件/目录",2),
	MOVE("移动文件/目录",3),
	DELETE("删除文件/目录", 4),
	EMPTYTRASH("清空回收站",5),
	LISTSTATUS("获取文件/目录状态列表",6),
	OPEN("打开文件",7),
	WRITE("写入内容",8),
	APPEND("追加内容",9);
	

	private final String name;

	private final int value;

	OP(String name, int value) {
		this.value = value;
		this.name = name;
	}

	@JsonCreator
	public static OP getEnum(int value) {
		for (OP op : OP.values()) {
			if (op.getValue() == value) {
				return op;
			}
		}
		return null;
	}

	@JsonValue
	public int getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}
