import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import com.appleyk.hdfs.HdfsApi;
import com.appleyk.hdfs.HdfsApiException;
import com.appleyk.model.RegexExcludePathFilter;

public class HdfsApiTest {

	private HdfsApi api;
	private FileSystem fs = null;

	/**
	 * 初始化Api
	 * 
	 * @throws Exception
	 */
	public void initApi() throws Exception {

		Configuration conf = new Configuration();
	
		/**
		 * dfs.client.block.write.replace-datanode-on-failure.enable=true
		 * 如果在写入管道中存在一个DataNode或者网络故障时，
		 * 那么DFSClient将尝试从管道中删除失败的DataNode，
		 * 然后继续尝试剩下的DataNodes进行写入。
		 * 结果，管道中的DataNodes的数量在减少。
		 * enable ：启用特性，disable：禁用特性
		 * 该特性是在管道中添加新的DataNodes。
		 * 当集群规模非常小时，例如3个节点或更少时，集群管理员可能希望将策略在默认配置文件里面设置为NEVER或者禁用该特性。
		 * 否则，因为找不到新的DataNode来替换，用户可能会经历异常高的管道写入错误,导致追加文件操作失败
		 */
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		
		/**
		 * dfs.client.block.write.replace-datanode-on-failure.policy=DEFAULT
		 * 这个属性只有在dfs.client.block.write.replace-datanode-on-failure.enable设置true时有效：
		 * ALWAYS ：当一个存在的DataNode被删除时，总是添加一个新的DataNode
		 * NEVER  ：永远不添加新的DataNode
		 * DEFAULT：副本数是r，DataNode的数时n，只要r >= 3时，或者floor(r/2)大于等于n时
		 * r>n时再添加一个新的DataNode，并且这个块是hflushed/appended
		 */
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		
		/**
		 * 本地开启HDFS的回收站功能，设置回收站的文件过期时间为360/60 = 6个小时
		 * 如果HDFS开启的有回收站功能，以其设置的interval为准
		 */
		conf.set("fs.trash.interval", "360");
		
		/**
		 * 设置hdfs文件系统的uri(远程连接)
		 */
		conf.set("fs.defaultFS", "hdfs://192.168.142.138:9000");		
		api = new HdfsApi(conf, "root");
		
	}

	/**
	 * 拿到当前HDFS文件系统的用户目录
	 * home目录是用户的宿主目录，一个用户登录系统，进入后，所处的位置就是/home
	 */
	@Test
	public void getHomeDir() throws Exception{
		initApi();
		Path homeDir = api.getHomeDir();
		System.out.println("Home dir is : "+homeDir);
		api.close();
	}
	
	/**
	 * 获取当前HDFS文件系统的状态
	 * @throws Exception
	 */
	@Test
	public void getFSStatus() throws Exception{
		initApi();
		api.getStatus();
		api.close();
	}
	
	/**
	 * 创建文件，并写入内容
	 * 
	 * @throws HdfsApiException
	 * @throws IOException
	 */
	@Test
	public void createFile() throws Exception {
		initApi();
		String content = "Hello HDFS !";
		api.putStringToFile("1.txt", content);
		api.close();
	}

	/**
	 * 追加文件，添加内容
	 */
	@Test
	public void appendFile() throws Exception {
		initApi();
		api.appendStringToFile("1.txt", "2018年6月28日17:49:50");
		api.close();
	}
	
	/**
	 * 验证HDFS回收站功能是否开启
	 */
	@Test
	public void enabledTrash() throws Exception{
		initApi();
		boolean result = api.trashEnabled();
		System.out.println("回收站功能是否可用："+result);
		api.close();
	}
	
	
	/**
	 * 删除文件或目录
	 */
	@Test
	public void delete() throws Exception{
		initApi();
		boolean result = fs.delete(new Path("hdfs://192.168.142.138:9000/3.txt"), true);
		System.out.println("删除状态："+result);
		api.close();
	}
	
	/**
	 * 删除文件或目录 == 是否启用回收功能
	 * 
	 */
	@Test
	public void rmdir() throws Exception{
		initApi();
		//参数依次为：要删除的文件或目录/是否递归删除（针对目录）/是否跳过回收站（如果true，表示直接、彻底删除）
		boolean result = api.rmdir("1.txt", true, true);
		System.out.println("文件删除状态："+result);
		api.close();
	}
	
	
	/**
	 * 清空回收站
	 */
	@Test
	public void emptyTrash() throws Exception {
		initApi();
		boolean result = api.emptyTrash();
		System.out.println("清空回收站状态："+result);
		api.close();
	}
	
	/**
	 * 从回收站里面恢复文件或目录
	 */
	@Test
	public void restoreFromTrash() throws Exception{
		initApi();
		String trashPath = api.getTrashDirPath("Current");
		System.out.println("path = "+trashPath);
		boolean result = api.restoreFromTrash(trashPath+"/1.txt", "");
		System.out.println("恢复文件状态："+result);
	}
	
	/**
	 * 打开文件，并读取内容
	 * @throws Exception
	 */
	@Test
	public void openFile() throws Exception{
		initApi();
		String content = api.readFileToString("output/part-r-00000");
		System.out.println("内容：\n"+content);
		api.close();
	}
	
	
	@Test
	public void reName() throws Exception{
		initApi();
		boolean result = api.rename("1.txt", "2.txt");
		//如果重命名成功，读取文件内容
		if(result){
			System.out.println(api.readFileToString("2.txt"));
		}
		api.close();
	}
	
	/**
	 * 判断文件是否存在
	 * @throws Exception
	 */
	@Test
	public void existsFile() throws Exception{
		initApi();
		boolean result = api.existFile("2.txt");
		System.out.println("是否存在："+result);
	}
	
	/**
	 * 根据文件路径过滤条件，获取指定路径下面的文件/目录列表
	 * @throws Exception
	 */
	@Test
	public void getListStatus() throws Exception{
		initApi();
		PathFilter pathFilter = new RegexExcludePathFilter(".*txt");
		api.getFileList("", pathFilter);
		api.close();
	}
}
