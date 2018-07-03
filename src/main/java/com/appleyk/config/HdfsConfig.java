package com.appleyk.config;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class HdfsConfig {

	@Value("${hadoop.hdfs.uri}")
	private String uri;

	@Bean(name = "conf")
	public Configuration getConf() {

		Configuration conf = new Configuration();

		/**
		 * dfs.client.block.write.replace-datanode-on-failure.enable=true
		 * 如果在写入管道中存在一个DataNode或者网络故障时， 那么DFSClient将尝试从管道中删除失败的DataNode，
		 * 然后继续尝试剩下的DataNodes进行写入。 结果，管道中的DataNodes的数量在减少。 enable
		 * ：启用特性，disable：禁用特性 该特性是在管道中添加新的DataNodes。
		 * 当集群规模非常小时，例如3个节点或更少时，集群管理员可能希望将策略在默认配置文件里面设置为NEVER或者禁用该特性。
		 * 否则，因为找不到新的DataNode来替换，用户可能会经历异常高的管道写入错误,导致追加文件操作失败
		 */
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

		/**
		 * dfs.client.block.write.replace-datanode-on-failure.policy=DEFAULT
		 * 这个属性只有在dfs.client.block.write.replace-datanode-on-failure.
		 * enable设置true时有效： ALWAYS ：当一个存在的DataNode被删除时，总是添加一个新的DataNode NEVER
		 * ：永远不添加新的DataNode DEFAULT：副本数是r，DataNode的数时n，只要r >=
		 * 3时，或者floor(r/2)大于等于n时 r>n时再添加一个新的DataNode，并且这个块是hflushed/appended
		 */
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

		/**
		 * 本地开启HDFS的回收站功能，设置回收站的文件过期时间为360/60 = 6个小时
		 * 如果HDFS开启的有回收站功能，以其设置的interval为准
		 */
		conf.set("fs.trash.interval", "360");

		conf.set("fs.defaultFS", uri);		
		return conf;
	}
}
