package com.appleyk.controller;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.appleyk.exception.HdfsApiException;
import com.appleyk.hdfs.HdfsApi;
import com.appleyk.hdfs.service.HdfsApiService;
import com.appleyk.model.FileStatusModel;
import com.appleyk.model.HDFSOp;
import com.appleyk.model.ListFilter;
import com.appleyk.model.OP;
import com.appleyk.paging.DPage;
import com.appleyk.result.ResponseMessage;
import com.appleyk.result.ResponseResult;
import com.appleyk.result.ResultData;

@CrossOrigin
@RestController
@RequestMapping("/appleyk/webhdfs/v1")
public class HdfsApiController {

	@Autowired
	private Configuration conf;

	@Value("${hadoop.hdfs.user}")
	private String user;

	@Autowired
	private HdfsApiService apiService;

	@PostMapping
	public ResponseResult oPHdfs(@RequestBody HDFSOp hdfsOp, HttpServletResponse response) throws Exception {

		/**
		 * 自定义扩展
		 */

		boolean result = false;
		HdfsApi api = new HdfsApi(conf, user);
		OP op = hdfsOp.getOp();
		if (op == null) {
			throw new HdfsApiException("无法接收文件操作标识为空的请求");
		}

		if (op.equals(OP.CREATE)) {
			result = apiService.create(api, hdfsOp);
		} else if (op.equals(OP.DELETE)) {
			result = apiService.delete(api, hdfsOp);
		} else if (op.equals(OP.COPY)) {
			result = apiService.copy(api, hdfsOp);
		} else if (op.equals(OP.EMPTYTRASH)) {
			result = apiService.emptyTrash(api);
		} else if (op.equals(OP.MOVE)) {
			result = apiService.move(api, hdfsOp);
		} else if (op.equals(OP.RENAME)) {
			result = apiService.rename(api, hdfsOp);
		} else if (op.equals(OP.WRITE)) {
			result = apiService.write(api, hdfsOp);
		} else if (op.equals(OP.APPEND)) {
			result = apiService.append(api, hdfsOp);
		} else if (op.equals(OP.OPEN)) {
			apiService.open(api, hdfsOp, response);
		} else if (op.equals(OP.UPLOAD)) {
			result = apiService.upLoad(api, hdfsOp);
		} else if (op.equals(OP.DOWNLOAD)) {
			result = apiService.downLoad(api, hdfsOp);
		}

		api.close();

		if (result) {
			return new ResponseResult(ResponseMessage.OK);
		} else {
			return new ResponseResult(ResponseMessage.INTERNAL_SERVER_ERROR);
		}

	}

	@GetMapping
	public ResponseResult getFileStatus(ListFilter listFilter) throws Exception {

		HdfsApi api = new HdfsApi(conf, user);
	    OP op=OP.getEnum(listFilter.getOp());
		if (op == null) {
			throw new HdfsApiException("无法接收文件操作标识为空的请求");
		}

		DPage<FileStatusModel> dPage = null;

		if (op.equals(OP.FILElIST)) {
			dPage = apiService.getFileListStatus(api, listFilter);
		} else if (op.equals(OP.HOMELIST)) {
			dPage = apiService.getHomeListStatus(api, listFilter);
		} else if (op.equals(OP.TRASHLIST)) {
			dPage = apiService.getTrashListStatus(api, listFilter);
		}

		/**
		 * 构造返回结果
		 */
		ResultData<FileStatusModel> resultData = new ResultData<>(ResponseMessage.OK, dPage);

		api.close();
		return new ResponseResult(resultData);
	}

}
