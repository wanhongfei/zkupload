package com.jd.zk.client;

import com.jd.zk.utils.Args2Map;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by wanhongfei on 2017/1/10.
 */
public class ZkUploadConfigClient {

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
		// 解析输入
		Map<String, String> params = Args2Map.args2Map(args);
		ZooKeeper zk = new ZooKeeper(params.get(ZK_URI), 300000, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		createZkNode(zk, params.get(ZK_NODE_URI));
		uploadFile2ZkNode(zk, params.get(ZK_NODE_URI), params.get(LOCAL_FILE_PATH));
	}

	/**
	 * 构建指定zknode
	 *
	 * @param zk
	 * @param zknodes
	 */
	@SneakyThrows
	private static void createZkNode(@NonNull ZooKeeper zk, @NonNull String zknodes) {
		String[] nodes = zknodes.split(SEPARATOR);
		StringBuilder sb = new StringBuilder();
		for (int i = 0, len = nodes.length; i < len; i++) {
			if ("".equals(nodes[i])) continue;
			sb.append(SEPARATOR).append(nodes[i]);
			Stat stat = null;
			if ((stat = zk.exists(sb.toString(), true)) == null) {
				zk.create(sb.toString(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println(String.format("正在创建zknode:%s", sb.toString()));
			} else {
				System.out.println(String.format("zknode:%s已存在", sb.toString()));
			}
		}
	}

	/**
	 * 上传文件到zk指定节点上
	 *
	 * @param zk
	 * @param zknodes
	 * @param filePath
	 */
	@SneakyThrows
	private static void uploadFile2ZkNode(@NonNull ZooKeeper zk, @NonNull String zknodes, @NonNull String filePath) {
		@Cleanup InputStream is = new FileInputStream(filePath);
		@Cleanup ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
		int ch = -1;
		while ((ch = is.read()) != -1) {
			bytestream.write(ch);
		}
		byte[] data = bytestream.toByteArray();
		// 更新节点字段
		Stat stat = zk.exists(zknodes, true);
		if (stat == null) {
			zk.create(zknodes, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			zk.delete(zknodes, stat.getVersion());
			zk.create(zknodes, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
}
