package com.jd.zk.utils;

import lombok.NonNull;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wanhongfei on 2017/1/10.
 */
public class Args2Map {

	/**
	 * 解析输入参数
	 *
	 * @param args
	 * @return
	 */
	@SneakyThrows
	public static Map<String, String> args2Map(@NonNull String[] args) {
		if (args.length == 0 || args.length % 2 == 1) throw new Exception(String.format("执行参数参数不正确"));
		Map<String, String> params = new HashMap<String, String>();
		for (int i = 0, len = args.length; i < len; i += 2) {
			params.put(args[i], args[i + 1]);
		}
		return params;
	}
}
