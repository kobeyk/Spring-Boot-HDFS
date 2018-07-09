package com.appleyk.hdfs.service;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import com.appleyk.hdfs.HdfsApi;
import com.appleyk.model.FileStatusModel;
import com.appleyk.model.HDFSOp;
import com.appleyk.model.ListFilter;
import com.appleyk.paging.DPage;

public interface HdfsApiService {

	/**
	 * 
	CREATE("创建文件/目录", 0), 
	RENAME("重命名文件/目录", 1),
	COPY("复制文件/目录",2),
	MOVE("移动文件/目录",3),
	DELETE("删除文件/目录", 4),
	EMPTYTRASH("清空回收站",5),
	OPEN("打开文件",7),
	WRITE("写入内容",8),
	APPEND("追加内容",9),
	UPLOAD("上传文件或目录",10),
	DOWNLOAD("下载文件或目录",11);
	 */
	
	/**
	 * 创建文件或目录
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean create(HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 删除文件或目录（牵扯到回收站功能）
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean delete(HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 重命名文件或目录
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean rename(HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 复制文件或目录
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean copy  (HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 移动文件或目录
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean move  (HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 清空回收站
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean emptyTrash(HdfsApi api) throws Exception;
	
	/**
	 * 打开一个文件，读取内容
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	void open  (HdfsApi api,HDFSOp hdfsOp,HttpServletResponse response) throws Exception;
	
	/**
	 * 往文件里写内容，如果文件存在则覆盖，否则创建写入
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean write  (HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 文件内容追加
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean append  (HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 下载文件
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean downLoad(HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	
	/**
	 * 上传文件
	 * @param api
	 * @param hdfsOp
	 * @return
	 * @throws Exception
	 */
	boolean upLoad(HdfsApi api,HDFSOp hdfsOp) throws Exception;
	
	/**
	 * 获得文件列表状态
	 * @param api
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	DPage<FileStatusModel> getFileListStatus(HdfsApi api , ListFilter filter)  throws Exception;
	
	/**
	 * 获得主用户目录下的文件列表状态
	 * @param api
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	DPage<FileStatusModel> getHomeListStatus(HdfsApi api , ListFilter filter)  throws Exception;
	
	/**
	 * 获得垃圾回收站目录下面的文件列表状态
	 * @param api
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	DPage<FileStatusModel> getTrashListStatus(HdfsApi api , ListFilter filter) throws Exception;
	
}
