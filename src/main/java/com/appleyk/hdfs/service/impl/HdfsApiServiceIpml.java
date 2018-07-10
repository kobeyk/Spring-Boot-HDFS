package com.appleyk.hdfs.service.impl;

import java.io.OutputStream;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import com.appleyk.exception.HdfsApiException;
import com.appleyk.hdfs.HdfsApi;
import com.appleyk.hdfs.service.HdfsApiService;
import com.appleyk.model.HDFSFileStatus;
import com.appleyk.model.HDFSOp;
import com.appleyk.model.ListFilter;
import com.appleyk.model.RegexExcludePathFilter;
import com.appleyk.paging.DPage;

@Service
@Primary
public class HdfsApiServiceIpml implements HdfsApiService {

	@Override
	public boolean create(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		String content = hdfsOp.getContent();
		// 如果内容空，直接创建文件或目录
		if (StringUtils.isBlank(content)) {
			api.mkdir(srcPath);
		} else {// 否则，创建文件的同时，写入内容
			api.putStringToFile(srcPath, content);
		}

		return true;
	}

	@Override
	public boolean delete(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		// 是否递归删除
		boolean recursive = hdfsOp.getRecursive();
		// 是否跳过回收站
		boolean skiptrash = hdfsOp.getSkipTrash();
		return api.rmdir(srcPath, recursive, skiptrash);

	}

	@Override
	public boolean rename(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		boolean result = true;

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		String destPath = hdfsOp.getDestPath();
		if (StringUtils.isBlank(destPath)) {
			destPath = srcPath;
		}

		result = api.rename(srcPath, destPath);

		if (result) {
			return true;
		} else {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}
	}

	@Override
	public boolean copy(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		String destPath = hdfsOp.getDestPath();
		if (StringUtils.isBlank(destPath)) {
			destPath = "";
		}

		api.copy(srcPath, destPath);

		return true;
	}

	@Override
	public boolean move(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		String destPath = hdfsOp.getDestPath();
		if (StringUtils.isBlank(destPath)) {
			destPath = "";
		}

		api.move(srcPath, destPath);

		return true;
	}

	@Override
	public boolean emptyTrash(HdfsApi api) throws Exception {

		if (api.trashEnabled()) {
			return api.emptyTrash();
		}

		return false;
	}

	@Override
	public void open(HdfsApi api, HDFSOp hdfsOp, HttpServletResponse response) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		if (!api.exists(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		if (!api.existFile(srcPath)) {
			throw new HdfsApiException("The path is not a file so it can not open ");
		}

		FSDataInputStream in = null;
		OutputStream out = null;
		try {
			in = api.open(srcPath);
			out = response.getOutputStream();

			if (getSuffixName(srcPath).equals("jpg")) {
				response.setContentType("image/png"); // 设置返回的文件类型
			}

			byte[] data = new byte[in.available()];
			while (in.read(data) != -1) {
				out.write(data);
				out.flush();
				out.close();
				in.close();
			}
		} catch (Exception ex) {

			throw new HdfsApiException("The file read error or no data available in it  ");
		}
	}

	@Override
	public boolean write(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		String content = hdfsOp.getContent();
		if (StringUtils.isBlank(content)) {
			throw new HdfsApiException("The written content is empty and the operation terminates ");
		}

		api.putStringToFile(srcPath, content);
		return true;
	}

	@Override
	public boolean append(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}
		if (!api.existFile(srcPath)) {
			throw new HdfsApiException("The path is not a file so it can not open ");
		}

		String content = hdfsOp.getContent();
		if (StringUtils.isBlank(content)) {
			throw new HdfsApiException("The written content is empty and the operation terminates ");
		}

		api.appendStringToFile(srcPath, content);
		return true;
	}

	/**
	 * 是否是图片
	 * 
	 * @param suffix
	 * @return
	 */
	public boolean imageEnabled(String suffix) {

		if (suffix.equals("jpg") && suffix.equals("JPEG") && suffix.equals("jpeg") && suffix.equals("png")
				&& suffix.equals("gif")) {
			return true;
		}
		return false;
	}

	/**
	 * 获取文件后缀名
	 * 
	 * @param file
	 * @return
	 */
	public String getSuffixName(String path) {
		return path.substring(path.lastIndexOf(".") + 1);
	}

	@Override
	public boolean downLoad(HdfsApi api, HDFSOp hdfsOp) throws Exception {
		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Src Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}
		String destPath = hdfsOp.getDestPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Dest Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}
		api.downLoadFile(srcPath, destPath);
		return true;
	}

	@Override
	public boolean upLoad(HdfsApi api, HDFSOp hdfsOp) throws Exception {

		String srcPath = hdfsOp.getSrcPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Src Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}
		String destPath = hdfsOp.getDestPath();
		if (StringUtils.isBlank(srcPath)) {
			throw new HdfsApiException(
					"Dest Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		api.existDir(destPath, true);
		boolean overwrite = hdfsOp.getOverride();
		api.upLoadFile(srcPath, destPath, false, overwrite);

		return true;
	}

	@Override
	public DPage<HDFSFileStatus> getFileListStatus(HdfsApi api, ListFilter filter) throws Exception {

		String dirPath = filter.getDirPath();
		if (dirPath == null) {
			throw new HdfsApiException(
					"Dir Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		int pageNum  = filter.getPageNum() ;
		int pageSize = filter.getPageSize();
 		List<HDFSFileStatus> result = api.getFileList(dirPath, getPathFilter(filter.getRegex()));
		DPage<HDFSFileStatus> dPage = new DPage<>(result, pageNum, pageSize);

		return dPage;
	}

	@Override
	public DPage<HDFSFileStatus> getHomeListStatus(HdfsApi api, ListFilter filter) throws Exception {
	
		Path homeDir = api.getHomeDir();
		String dirPath = "user/"+homeDir.getName();
		if (StringUtils.isBlank(dirPath)) {
			throw new HdfsApiException(
					"Home Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		int pageNum  = filter.getPageNum() ;
		int pageSize = filter.getPageSize();
 		List<HDFSFileStatus> result = api.getFileList(dirPath, getPathFilter(filter.getRegex()));
		DPage<HDFSFileStatus> dPage = new DPage<>(result, pageNum, pageSize);

		return dPage;	
	}

	@Override
	public DPage<HDFSFileStatus> getTrashListStatus(HdfsApi api, ListFilter filter) throws Exception {

		Path trashDir = api.getTrashDir();
		String dirPath = "user/"+api.getHomeDir().getName()+"/"+trashDir.getName().trim();
		if (StringUtils.isBlank(dirPath)) {
			throw new HdfsApiException(
					"Home Path does not exist on HDFS or WebHDFS is disabled. Please check your path or enable WebHDFS");
		}

		int pageNum  = filter.getPageNum() ;
		int pageSize = filter.getPageSize();
 		List<HDFSFileStatus> result = api.getFileList(dirPath, getPathFilter(filter.getRegex()));
		DPage<HDFSFileStatus> dPage = new DPage<>(result, pageNum, pageSize);
		return dPage;	
	}

	/**
	 * 获得路径过滤器
	 * @param regex
	 * @return
	 */
	private PathFilter getPathFilter(String regex) {
		
		PathFilter pathFilter;
		if (StringUtils.isBlank(regex)) {
			pathFilter = null;
		} else {
			pathFilter = new RegexExcludePathFilter(regex);
		}
		
		return pathFilter;
	}
}
