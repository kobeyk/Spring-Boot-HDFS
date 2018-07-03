package com.appleyk.model;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 自定义PathFilter接口的实现类
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年6月29日-上午9:36:55
 */
public class RegexExcludePathFilter implements PathFilter {

	private final String regex;

	public RegexExcludePathFilter(String regex) {
		this.regex = regex;
	}

	
	/**
	 * 实现PathFilter接口的accept方法，过滤掉包含regex内容的Path
	 */
	@Override
	public boolean accept(Path path) {
		return !path.toString().matches(regex);
	}

}
