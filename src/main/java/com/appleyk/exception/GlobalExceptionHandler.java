package com.appleyk.exception;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.appleyk.result.ResponseResult;

@CrossOrigin
@RestControllerAdvice
public class GlobalExceptionHandler {

	 private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);
	
	@ExceptionHandler
	public ResponseResult processException(Exception ex, HttpServletRequest request, HttpServletResponse response){
		
		SimpleDateFormat formatDate = new SimpleDateFormat("yyyyMMddHHmmss");
		String date = formatDate.format(new Date());
	
		
		if(ex instanceof Exception) {
			logger.info("\n "+ ex.getMessage() +" --" + date + "\n" + ex);
			return new ResponseResult(500, ex.getMessage());
		}
		
		
		if(ex instanceof HdfsApiException) {
			logger.info("\n "+ ex.getMessage() +" --" + date + "\n" + ex);
			return new ResponseResult(500, ex.getMessage());
		}
		
		
		/**
		 * 未知异常
		 */
		logger.info(ex.toString());
		return new ResponseResult(500, ex);
	}

}
