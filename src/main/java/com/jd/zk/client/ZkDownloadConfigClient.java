package com.jd.zk.client;

import com.jd.zk.utils.Args2Map;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.util.Map;

/**
 * Created by wanhongfei on 2017/1/10.
 */
public class ZkDownloadConfigClient {

	private static final String SEPARATOR = "/";
	private static final String ZK_URI = "-zk";
	private static final String ZK_NODE_URI = "-p";
	private static final String LOCAL_FILE_PATH = "-f";

	/**
	 * 启动入口
	 * -zk 10.0.1.85:2181,10.0.1.86:2181,10.0.1.87:2181
	 * -p /flume/a1
	 * -f D:\flume-zookeeper.properties
	 *
	 * @param args
	 */
	@SneakyThrows
	public static void main(String args[]) {
		for (String arg : args) {
			System.out.println(arg);
		}
		// 解析输入
		Map<String, String> params = Args2Map.args2Map(args);
		ZooKeeper zk = new ZooKeeper(params.get(ZK_URI), 300000, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		download2Local(zk, params.get(ZK_NODE_URI), params.get(LOCAL_FILE_PATH));
	}

	/**
	 * 从zk下载配置文件到本地
	 *
	 * @param zk
	 * @param zknodes
	 * @param filePath
	 */
	@SneakyThrows
	private static void download2Local(@NonNull ZooKeeper zk, @NonNull String zknodes, @NonNull String filePath) {
		String data = new String(zk.getData(zknodes, true, null));
		FileUtils.write(new File(filePath), data, false);
	}

}
