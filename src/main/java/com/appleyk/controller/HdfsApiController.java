package com.appleyk.controller;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.appleyk.hdfs.HDFSOp;
import com.appleyk.hdfs.HdfsApi;
import com.appleyk.hdfs.OP;
import com.appleyk.model.FileStatusModel;
import com.appleyk.model.RegexExcludePathFilter;
import com.appleyk.paging.DPage;
import com.appleyk.result.ResponseMessage;
import com.appleyk.result.ResponseResult;
import com.appleyk.result.ResultData;


@RestController
@RequestMapping("/appleyk/webhdfs/v1")
public class HdfsApiController {

	@Autowired
	private Configuration conf;

	@Value("${hadoop.hdfs.user}")
	private String user;

	@PostMapping
	public ResponseResult oPHdfs(@RequestBody HDFSOp hdfsOp) throws Exception {

		/**
		 * 自定扩展
		 */
		
		boolean result = false;
		HdfsApi api = new HdfsApi(conf, user);
		String op = hdfsOp.getOp().getName();
		if (op.equals(OP.CREATE)) {
			result = api.mkdir(hdfsOp.getSrcPath());
		}
		api.close();
		if (result) {
			return new ResponseResult(ResponseMessage.OK);
		} else {
			return new ResponseResult(ResponseMessage.INTERNAL_SERVER_ERROR);
		}

	}

	@GetMapping
	public ResponseResult getFileStatus(
			@RequestParam("dirPath") String dirPath, 
			@RequestParam("regex") String regex,
			@RequestParam("pageNum")  Integer pageNum,
			@RequestParam("pageSize") Integer pageSize)
			throws Exception {
		HdfsApi api = new HdfsApi(conf, user);
		PathFilter pathFilter;
		if (StringUtils.isBlank(regex)) {
			pathFilter = null;
		} else {
			pathFilter = new RegexExcludePathFilter(regex);
		}
	
		List<FileStatusModel> result = api.getFileList(dirPath, pathFilter);
		DPage<FileStatusModel> dPage = new DPage<>(result, pageNum, pageSize);
		/**
		 * 构造返回结果
		 */
		ResultData<FileStatusModel> resultData = new ResultData<>(ResponseMessage.OK,dPage);
		return new ResponseResult(resultData);
	}

}
