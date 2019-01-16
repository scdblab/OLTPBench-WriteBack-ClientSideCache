package com.oltpbenchmark.benchmarks.tpcc.validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class KafkaScripts {
	public static String ZOOKHOSTSTR = "10.0.0.240:2181";
	public static String KAFKA_FLDR = "/home/mr1/Validation/kafka/";

	public static void deleteTopic(String topic) {
		try {
			// bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic MEMBER
			String cmd = KAFKA_FLDR + "bin/kafka-topics.sh --zookeeper " + ZOOKHOSTSTR + " --delete --topic " + topic;
			Runtime.getRuntime().exec(cmd);
			// Process p = new ProcessBuilder(cmd).start();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

	}

	public static boolean isTopicExist(String topic) {

		// bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic MEMBER
		String cmd = KAFKA_FLDR + "bin/kafka-topics.sh --zookeeper " + ZOOKHOSTSTR + " --list ";
		String r = Utilities.executeRuntime(cmd, true);
		if (r.toLowerCase().contains(topic.toLowerCase())) {
			return true;
		}
		return false;

	}

	public static boolean isValidatorRunning(String app) {
		String r = Utilities.executeRuntime("jps -m | grep \"ValidationMain\" | grep -ie \"-app " + app + "\"", true);
		if (r.toLowerCase().contains(app.toLowerCase())) {
			return true;
		}
		return false;
	}

	public static void killValidator(String app) {
		Utilities.executeRuntime("jps -m | grep \"ValidationMain\" | grep -ie \"-app " + app + "\" | cut -b1-6 | xargs -t kill -9", true);
	}

	public static String createTopic(String topic, int numPartitions) {
		String output = "";

		try {
			// --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic test
			String cmd = "sudo " + KAFKA_FLDR + "bin/kafka-topics.sh --zookeeper " + ZOOKHOSTSTR + " --create --replication-factor 1 --partitions " + numPartitions + " --topic " + topic;
			Process p = Runtime.getRuntime().exec(cmd);
			Thread.sleep(1000);
			InputStream stdout = p.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
			String line = "";
			while ((line = reader.readLine()) != null) {
				if (line.contains("already exists")) {
					output = "already exists";
					break;
				}
				output = output + line;
			}
			// Process p = new ProcessBuilder(cmd).start();
			p.waitFor();
			stdout.close();
			reader.close();

		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		// output is: Created topic "test".
		// or: already exists.

		return output;
	}

	public static void main(String[] args) {
		boolean r = true;
		killValidator("Bg");
		System.out.println(r);

	}

}
