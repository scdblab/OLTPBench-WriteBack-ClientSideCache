package com.oltpbenchmark.benchmarks.tpcc.validation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;

import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;

class BGLogObject {
	String optype = "", mopType = "", seqId, threadId, rid, value, starttime, endtime, updatetype; // only for update log records
	String actionType;

	public BGLogObject(String opt, String mot, String sid, String tid, String resid, String start, String end, String val, String type, String actionType) {
		optype = opt;
		mopType = mot;
		seqId = sid;
		threadId = tid;
		rid = resid;
		value = val;
		starttime = start;
		endtime = end;
		updatetype = type;
		this.actionType = actionType;
	}

	public String getActionType() {
		return actionType;
	}

	public void setActionType(String actionType) {
		this.actionType = actionType;
	}

	public String getOptype() {
		return optype;
	}

	public void setOptype(String optype) {
		this.optype = optype;
	}

	public String getMopType() {
		return mopType;
	}

	public void setMopType(String mopType) {
		this.mopType = mopType;
	}

	public String getSeqId() {
		return seqId;
	}

	public void setSeqId(String seqId) {
		this.seqId = seqId;
	}

	public String getThreadId() {
		return threadId;
	}

	public void setThreadId(String threadId) {
		this.threadId = threadId;
	}

	public String getRid() {
		return rid;
	}

	public void setRid(String rid) {
		this.rid = rid;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getStarttime() {
		return starttime;
	}

	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}

	public String getEndtime() {
		return endtime;
	}

	public void setEndtime(String endtime) {
		this.endtime = endtime;
	}

	public String getUpdatetype() {
		return updatetype;
	}

	public void setUpdatetype(String updatetype) {
		this.updatetype = updatetype;
	}

}

public class Utilities {
	public static final String newline = System.getProperty("line.separator");

	public static String concatWithSeperator(char Seperator, String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
			if (i + 1 != params.length)
				sb.append(Seperator);
		}
		return sb.toString();
	}

	public static boolean compareValues(String value1, String value2) {
		if (isNumeric(value1) && isNumeric(value2)) {
			double a = Double.parseDouble(value1);
			double b = Double.parseDouble(value2);
			double c = Math.abs(a - b);
			return TPCCConstants.ERROR_MARGIN > c;

		} else {
			return value1.equals(value2);
		}
	}

	public static boolean isNumeric(String value) {
		try {
			Double.parseDouble(value);
			return true;
		} catch (Exception e) {
			return false;
		}

	}

	public static String concat(String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
		}
		return sb.toString();
	}

	public static String concat(String st1, char ch, String st2) {
		StringBuffer sb = new StringBuffer();
		sb.append(st1);
		sb.append(ch);
		sb.append(st2);
		return sb.toString();

	}

	public static String concat(String st1, String st2, char ch, String st3) {
		StringBuffer sb = new StringBuffer();
		sb.append(st1);
		sb.append(st2);
		sb.append(ch);
		sb.append(st3);
		return sb.toString();

	}

	public static String applyIncrements(String v1, String v2) {
		double result = Double.parseDouble(v1) + Double.parseDouble(v2);
		String str = null;
		if (result == 0)
			str = "0.00";
		else
			str = TPCCConstants.DECIMAL_FORMAT.format(result);
		return str;
	}

	// private String getStocksLogString(int ol_count) {
	// StringBuilder sb = new StringBuilder();
	// for (int i = 1; i <= ol_count; i++) {
	// String tokens[] = ((String) this.transactionResults.get("stock" + i)).split("_");
	// String stockId = generateID(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
	// String properties = getPropertiesString(TPCCConstants.STOCK_PROPERIES, tokens[2], TPCCConstants.NEW_VALUE_UPDATE);
	// sb.append(getEntityLogString(TPCCConstants.STOCK_ENTITY, stockId, properties));
	// if (i != ol_count)
	// sb.append(TPCCConstants.ENTITY_SEPERATOR);
	//
	// }
	// return sb.toString();
	// }
	public static String getLogString(LogRecord r) {

		StringBuilder sb = new StringBuilder();
		sb.append(r.getType());
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getActionName());
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		String tokens[] = r.getId().split(ValidationConstants.KEY_SEPERATOR + "");
		sb.append(tokens[0]);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(tokens[1]);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getStartTime());
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getEndTime());
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		String entities = Utilities.generateEntitiesLog(r.getEntities());
		sb.append(entities);
		sb.append(newline);
		return sb.toString();
	}

	public static String getLogString(char type, String actionName, int threadId, int sequenceId, long startTime, long endTime, String... entities) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(threadId+","+sequenceId);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < entities.length; i++) {
			sb.append(entities[i]);
			if ((i + 1) != entities.length)
				sb.append(TPCCConstants.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}

	public static void executeRuntime(String cmd, boolean wait, String dist) {
		Process p;
		File ufile = new File(dist);
		FileWriter ufstream = null;
		try {
			ufstream = new FileWriter(ufile);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		StringBuilder sb = new StringBuilder();
		BufferedWriter file = new BufferedWriter(ufstream);
		try {

			p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line + "\n");
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}
		try {
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1);
			file.write(sb.toString());
			file.flush();
			file.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	public static int getNumOfFiles(String logDir) {
		File dir = new File(logDir);
		return dir.list().length;
	}

	public static String executeRuntime(String cmd, boolean wait) {
		Process p;

		StringBuilder sb = new StringBuilder();
		try {

			p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}

		return sb.toString();

	}

//	public static void replayBGLogs(String logDir, String topic, int numValidators, boolean multiThreads, boolean multiTopic, String ServerIP) {
//		int threadCount = getNumOfFiles(logDir) / 2;
//		if (!multiThreads) {
//			threadCount = 1;
//		}
//		KafkaProducer<String, String> kafkaProducer = null;
//		try {
//			Properties props = new Properties();
//			props.put("bootstrap.servers", ServerIP+":9092");
//			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//			//props.put(" key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//			props.put("acks", "all");
//			props.put("retries", 0);
//			props.put("batch.size", 16384);
//			props.put("linger.ms", 1);
//			props.put("buffer.memory", 33554432);
//			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//			kafkaProducer = new KafkaProducer<String, String>(props);
//		} catch (Exception ex) {
//			ex.printStackTrace(System.out);
//		}
//
//		ExecutorService exec = Executors.newFixedThreadPool(threadCount);
//		Set<Future<Long>> set = new HashSet<Future<Long>>();
//		for (int i = 0; i < threadCount; i++) {
//			ReplayThread t = new ReplayThread(i + "", kafkaProducer, numValidators, logDir, topic, multiThreads, multiTopic);
//
//			Future<Long> future = exec.submit(t);
//			set.add(future);
//		}
//
//		try {
//			long sum = 0;
//			for (Future<Long> future : set) {
//				sum += future.get();
//			}
//			System.out.println("The sum of the logs:" + sum);
//			System.out.println("Done");
//			kafkaProducer.close();
//		} catch (Exception e) {
//			e.printStackTrace(System.out);
//		}
//
//	}

//	static class ReplayThread implements Callable<Long> {
//		String id;
//		KafkaProducer<String, String> producer;
//		int numValidators;
//		String logDir;
//		String topic;
//		boolean multiThreads;
//		boolean multiTopic;
//
//		public ReplayThread(String id, KafkaProducer<String, String> producer, int validators, String dir, String topic, boolean multi, boolean multiTopic) {
//			this.id = id;
//			this.producer = producer;
//			this.numValidators = validators;
//			this.logDir = dir;
//			this.topic = topic;
//			multiThreads = multi;
//			this.multiTopic = multiTopic;
//
//		}
//
//		@Override
//		public Long call() throws Exception {
//			int threadCount = 2;
//			if (!multiThreads) {
//				threadCount = getNumOfFiles(logDir);
//
//			}
//			FileInputStream[] fstreams = new FileInputStream[threadCount];
//			DataInputStream[] dataInStreams = new DataInputStream[threadCount];
//			BufferedReader[] bReaders = new BufferedReader[threadCount];
//			long logsCount = 0;
//			HashSet<String> logs = new HashSet<String>();
//			for (int i = 0; i < threadCount; i = i + 2) {
//
//				try {
//					int machineid = 0;
//					if (!multiThreads) {
//						id = i / 2 + "";
//					}
//					fstreams[i] = new FileInputStream(logDir + "/read" + machineid + "-" + id + ".txt");
//					dataInStreams[i] = new DataInputStream(fstreams[i]);
//					bReaders[i] = new BufferedReader(new InputStreamReader(dataInStreams[i]));
//
//					fstreams[i + 1] = new FileInputStream(logDir + "/update" + machineid + "-" + id + ".txt");
//					dataInStreams[i + 1] = new DataInputStream(fstreams[i + 1]);
//					bReaders[i + 1] = new BufferedReader(new InputStreamReader(dataInStreams[i + 1]));
//				} catch (FileNotFoundException e) {
//					e.printStackTrace(System.out);
//					System.out.println("Log file not found " + e.getMessage());
//				}
//			}
//
//			// ==================================================================
//			int seq = 0;
//			try {
//				String line = null;
//				boolean allDone = false;
//				LogRecord[] records = new LogRecord[threadCount];
//				while (!allDone) {
//					allDone = true;
//					for (int i = 0; i < threadCount; i++) {
//						if (records[i] == null) {
//							if ((line = bReaders[i].readLine()) != null) {
//								records[i] = LogRecord.createLogRecord(line);
//								allDone = false;
//							}
//						} else {
//							allDone = false;
//						}
//					}
//					LogRecord currentRecord = null;
//					if (allDone) {
//						;
//						// if (ReadWrite.size() != 0) {
//						// allDone = false;
//						// currentRecord = ReadWrite.get(0);
//						// ReadWrite.remove(0);
//						// }
//					} else {
//						currentRecord = getEarilestRecord(records);
//
//					}
//					if (!allDone) {
//						// if(currentRecord.getId().equals("57-0"))
//						// System.out.println();
//						while (logs.contains(currentRecord.getId())) {
//							System.out.println("Log:" + currentRecord.getId() + " already exit");
//
//							currentRecord.setId("101-" + seq);
//							seq++;
//							// System.out.println("Log:"+ currentRecord.getId() +" already exit");
//							// System.exit(0);
//						}
//						logs.add(currentRecord.getId());
//						sendToKafka(currentRecord);
//						logsCount++;
//					}
//				}
//			} catch (Exception e) {
//				e.printStackTrace(System.out);
//				System.exit(0);
//			}
//			try {
//				for (int i = 0; i < threadCount; i++) {
//					if (dataInStreams[i] != null)
//						dataInStreams[i].close();
//					if (bReaders[i] != null)
//						bReaders[i].close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace(System.out);
//			}
//
//			return logsCount;
//		}
//
//		private void sendToKafka(LogRecord currentRecord) {
//			if (currentRecord.getId().equals("41-24"))
//				System.out.println();
//			Entity[] entities = currentRecord.getEntities();
//			if (entities.length > 1) {
//				System.out.println("Error: Expecting one entity");
//				System.exit(0);
//			}
//			int key = Integer.parseInt(entities[0].key);
//
//			// R_S = (TopicId % NumVal)
//			// R_E = (TopicId % NumVal) + (NumVal)
//			// W_S = (TopicId % NumVal) + (2 * NumVal)
//			// W_E = (TopicId % NumVal) + (3 * NumVal)
//
//			String tempTopic = topic;
//			if (multiTopic) {
//				numValidators = 2;
//				tempTopic += (key % numValidators);
//				numValidators = 1;
//			}
//			int partition = (key % numValidators);
//			if (currentRecord.getType() != ValidationConstants.READ_RECORD)
//				partition = (key % numValidators) + (2 * numValidators);
//
//			producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), Long.toString(currentRecord.getStartTime())));
//			partition = (key % numValidators) + numValidators;
//			if (currentRecord.getType() != ValidationConstants.READ_RECORD)
//				partition = (key % numValidators) + (3 * numValidators);
//			String value = Utilities.getLogString(currentRecord);
//			producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), value));
//
//		}
//
//	}

	public static LogRecord getEarilestRecord(LogRecord[] records) {
		if (records == null || records.length == 0) {
			System.out.println("ERROR: (records == null || records.length == 0) returned true");
			System.exit(0);
		}
		LogRecord min = records[0];
		int index = 0;
		for (int i = 1; i < records.length; i++) {
			if (records[i] == null)
				continue;
			if (min == null || min.getStartTime() > records[i].getStartTime()) {
				index = i;
				min = records[i];
			}
		}
		records[index] = null;
		return min;
	}

	public static String getStaleLogString(char type, String actionName, String id, long startTime, long endTime, long readOffset, long updateOffset, String... entities) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(id);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(readOffset);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(updateOffset);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < entities.length; i++) {
			sb.append(entities[i]);
			if ((i + 1) != entities.length)
				sb.append(TPCCConstants.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}

	public static String getEntityLogString(String name, String key, String properies) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(key);
		sb.append(TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(properies);
		return sb.toString();
	}

	public static final String ValidationLogDir = "C:/Users/MR1/Documents/Validation/logs";

	private static void restartTopics(String topic, int numOfValidators, boolean multiTopics) {
		for (int i = 0; i < (multiTopics ? numOfValidators : 1); i++) {
			KafkaScripts.deleteTopic(topic + (multiTopics ? i : ""));
			KafkaScripts.deleteTopic("stats_" + topic + (multiTopics ? i : ""));
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String result = KafkaScripts.createTopic(topic + (multiTopics ? i : ""), (multiTopics ? 4 : 4 * numOfValidators));
			if (result.equals("already exists") || result.toLowerCase().contains("error")) {
				System.out.println(result+ ". Retrying...");
				restartTopics(topic, numOfValidators, multiTopics);
			}
			if (!KafkaScripts.isTopicExist(topic)) {
				System.out.println("topic was not created. Retrying ...");
				restartTopics(topic, numOfValidators, multiTopics);

			}
		}
	}

//	public static void main(String[] args) throws IOException {
////		logDir=$1
////		numV=$2
////		topic=$3
//		//"/home/yaz/bglogs/BG-10K-100fpm-100Threads-1Min-AccptFrd-incFriend"
//		String ServerIP = "10.0.0.240";
//		KafkaScripts.ZOOKHOSTSTR = KafkaScripts.ZOOKHOSTSTR.replace("localhost", ServerIP);
//		KafkaScripts.KAFKA_FLDR = KafkaScripts.KAFKA_FLDR.replace("yaz", "mr1");
//		String logDir=args[0];
//		int numValid = Integer.parseInt(args[1]);
//		String topic = args[2];
//		boolean multiTopic = false;
//		restartTopics(topic, numValid, multiTopic);
//
////		replayBGLogs(logDir, topic, numValid, false, multiTopic, ServerIP);
//		System.exit(0);
//		logDir = "C:/Users/MR1/Documents/Validation/BG-ValidationLogs/" + "BGTrace1/" + "10K-100fpm-100Threads-1Min-RjctInvtFrd-decInc";
//		// String logDir = "C:/Users/MR1/Documents/Validation/BG-ValidationLogs/" + "short10sec/bg1k2friends10thread10secstale";
//		String newLog = logDir + "/V";
//		boolean b = createDirectory(newLog);
//		ValidationConstants.THREAD_COUNT = getNumOfFiles(logDir) / 2;
//		parseBGLogFiles(logDir, ValidationConstants.THREAD_COUNT, 0, newLog);
//		System.out.println("Done Parsing.");
//		moveToValidation(newLog, ValidationLogDir);
//		System.out.println("Done Copying.");
//
//		// ===========================
//
//		// ValidationConstants.debug = true;
//		// String dir = "C:/Users/MR1/Documents/Validation/debug/";
//		// String src = dir + "src.txt";
//		// String dest = dir + "dest.txt";
//		// sortLogsInAFile(src, dest);
//		// System.out.println("Done.");
//	}

	public static String generateEntitiesLog(Entity[] entities) {
		// CUS;1-2-493;BALANCE:-374.44:N#YTD_P:374.44:N#P_CNT:2:N
		StringBuilder sb = new StringBuilder();
		try {

			for (Entity e : entities) {
				sb.append(concat(e.name, TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + "", e.key, TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + ""));
				for (Property p : e.getProperties()) {
					sb.append(concat(p.getName(), TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getValue(), TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getType() + "" + TPCCConstants.PROPERY_SEPERATOR + ""));

				}
				sb.deleteCharAt(sb.length() - 1); // remove last #
				sb.append(concat(TPCCConstants.ENTITY_SEPERATOR + ""));
			}

			sb.deleteCharAt(sb.length() - 1); // remove last &
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}

		return sb.toString();
	}

	public static String generateEntitiesLog(ArrayList<Entity> entities) {
		// CUS;1-2-493;BALANCE:-374.44:N#YTD_P:374.44:N#P_CNT:2:N
		StringBuilder sb = new StringBuilder();
		try {

			for (Entity e : entities) {
				sb.append(concat(e.name, TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + "", e.key, TPCCConstants.ENTITY_ATTRIBUTE_SEPERATOR + ""));
				for (Property p : e.getProperties()) {
					sb.append(concat(p.getName(), TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getValue(), TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getType() + "" + TPCCConstants.PROPERY_SEPERATOR + ""));

				}
				sb.deleteCharAt(sb.length() - 1); // remove last #
				sb.append(concat(TPCCConstants.ENTITY_SEPERATOR + ""));
			}

			sb.deleteCharAt(sb.length() - 1); // remove last &
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}

		return sb.toString();
	}

	private static void moveToValidation(String oldDir, String newDir) throws IOException {
		File source = new File(oldDir);
		File dest = new File(newDir);
		FileUtils.cleanDirectory(dest);
		try {
			FileUtils.copyDirectory(source, dest);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean createDirectory(String newDir) throws IOException {
		File dir = new File(newDir);
		boolean b = dir.mkdir();
		FileUtils.cleanDirectory(dir);
		return b;
	}

//	public static HashMap<String, LinkedList<DBState>> generateBGInitialState(int membersCount, int friendCountPerMember, int pendingCountPerMember) {
//		HashMap<String, LinkedList<DBState>> initState = new HashMap<String, LinkedList<DBState>>();
//
//		for (int i = 0; i < membersCount; i++) {
//			String fCount = Integer.toString(friendCountPerMember);
//			String pCount = Integer.toString(pendingCountPerMember);
//			DBState member = new DBState(0, fCount, pCount);
//			LinkedList<DBState> memberLL = new LinkedList<DBState>();
//			memberLL.add(member);
//			initState.put(TPCCConstants.MEMBER_ENTITY + TPCCConstants.KEY_SEPERATOR + i, memberLL);
//		}
//
//		return initState;
//
//	}

//	public static void parseBGLogFiles(String dir, int threadCount, int machineid, String outputDir) {
//
//		parseLogFiles(dir, threadCount, machineid, "update", outputDir);
//		parseLogFiles(dir, threadCount, machineid, "read", outputDir);
//
//	}

//	private static int parseLogFiles(String dir, int threadCount, int machineid, 
//			String type, String outputDir) {
//		int count = 0;
//
//		for (int i = 0; i < threadCount; i++) {
//			BufferedReader br = null;
//			String line = null;
//			String[] tokens = null;
//			BufferedWriter log = null;
//			FileWriter newfstream = null;
//			FileInputStream fstream = null;
//			DataInputStream in = null;
//
//			try {
//				File newfile = new File(outputDir + "/" + type + machineid + "-" + i + ".txt");
//				newfstream = new FileWriter(newfile);
//
//				log = new BufferedWriter(newfstream);
//
//				fstream = new FileInputStream(dir + "/" + type + machineid + "-" + i + ".txt");
//				in = new DataInputStream(fstream);
//				br = new BufferedReader(new InputStreamReader(in));
//
//			} catch (FileNotFoundException e) {
//				e.printStackTrace(System.out);
//				System.out.println("Log file not found " + e.getMessage());
//				// Since the file isn't found - move to the next iteration
//				continue;
//			} catch (IOException e) {
//				e.printStackTrace(System.out);
//			}
//
//			try {
//				int seq = 0;
//				while ((line = br.readLine()) != null) {
//					BGLogObject record = null;
//					count++;
//					tokens = line.split(",");
//					if (type.equalsIgnoreCase("update")) {
//						// UPDATE,PENDFRND,4,0,8519,556175393867908,556175407016902,1,I,InviteFriends
//						// opt, item, itemId, String val, String start, String end, String actionType, String updateType, String threadId, String seqId
//						record = new BGLogObject(tokens[0], tokens[1], String.valueOf(seq++)/* tokens[2] */, tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], tokens[8], tokens[9]);
//
//						String propertyName[] = null, propertyValue = null;
//						if (record.getMopType().equals("PENDFRND")) {
//							propertyName = TPCCConstants.PENDING_COUNT_PROPERIES;
//						} else if (record.getMopType().equals("ACCEPTFRND")) {
//							propertyName = TPCCConstants.FRIEND_COUNT_PROPERIES;
//						}
//
//						String v = record.getUpdatetype();
//						if (v.equals("I")) {
//							propertyValue = "+1";
//						}
//						if (v.equals("D")) {
//							propertyValue = "-1";
//						}
//						String memberId = record.getRid();
//
//						String memberProperties = Utilities.getPropertiesString(propertyName, propertyValue, TPCCConstants.INCREMENT_UPDATE);
//
//						String membersLogString = Utilities.getEntityLogString(TPCCConstants.MEMBER_ENTITY, memberId, memberProperties);
//
//						String UpdateLogString = Utilities.getLogString(TPCCConstants.UPDATE_RECORD, record.getActionType(), Integer.parseInt(record.getThreadId()), Integer.parseInt(record.getSeqId()), Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), membersLogString);
//						log.write(UpdateLogString);
//					} else { // read file
//
//						record = new BGLogObject(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]);
//						String propertyName[] = null;
//						if (record.getMopType().equals("PENDFRND")) {
//							propertyName = TPCCConstants.PENDING_COUNT_PROPERIES;
//						} else if (record.getMopType().equals("ACCEPTFRND")) {
//							propertyName = TPCCConstants.FRIEND_COUNT_PROPERIES;
//						}
//						String memberId = record.getRid();
//
//						String memberProperties = Utilities.getPropertiesString(propertyName, record.getValue(), TPCCConstants.VALUE_READ);
//
//						String membersLogString = Utilities.getEntityLogString(TPCCConstants.MEMBER_ENTITY, memberId, memberProperties);
//
//						String readLogString = Utilities.getLogString(TPCCConstants.READ_RECORD, record.getActionType(), Integer.parseInt(record.getThreadId()), Integer.parseInt(record.getSeqId()), Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), membersLogString);
//						log.write(readLogString);
//
//						// record = new BGLogObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]);
//
//						// record = new BGLogObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]));
//
//					}
//				} // end while
//			} // end try
//			catch (IOException e) {
//				e.printStackTrace(System.out);
//			}
//			try {
//				if (log != null)
//					log.flush();
//				log.close();
//
//				if (newfstream != null)
//					newfstream.close();
//				if (br != null)
//					br.close();
//				if (fstream != null)
//					fstream.close();
//				if (in != null)
//					in.close();
//			} catch (IOException e) {
//				e.printStackTrace(System.out);
//			}
//		}
//
//		return count;
//	}

	public static String getPropertiesString(String[] properiesNames, Object... params) {
		StringBuilder sb = new StringBuilder();
		int j = 0;
		for (int i = 0; i < properiesNames.length; i++, j++) {
			sb.append(properiesNames[i]);
			sb.append(TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(params[j]);
			sb.append(TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(params[++j]);
			if ((i + 1) != properiesNames.length)
				sb.append(TPCCConstants.PROPERY_SEPERATOR);
		}
		return sb.toString();
	}

	private static void sortLogsInAFile(String src, String dist) throws IOException {
		FileInputStream fstreams = new FileInputStream(src);
		DataInputStream dataInStreams = new DataInputStream(fstreams);
		BufferedReader bReaders = new BufferedReader(new InputStreamReader(dataInStreams));

		File distFile = new File(dist);
		FileWriter ufstream = new FileWriter(distFile);
		BufferedWriter bWriter = new BufferedWriter(ufstream);

		String line = null;

		try {
			ArrayList<LogRecord> records = new ArrayList<LogRecord>();
			while ((line = bReaders.readLine()) != null) {
				records.add(LogRecord.createLogRecord(line));
			}
			Collections.sort(records, LogRecord.Comparators.START);
			for (int i = 0; i < records.size(); i++) {
				bWriter.write(records.get(i).toString() + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
		try {
			if (dataInStreams != null)
				dataInStreams.close();
			if (bReaders != null)
				bReaders.close();
			fstreams.close();
			bWriter.flush();
			bWriter.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	public static String getPropertiesString2(String[] pNames, String[] pValues, char[] pTypes) {
		StringBuilder sb = new StringBuilder();
		boolean firstOne = true;
		for (int i = 0; i < pNames.length; i++) {
			if (pTypes[i] != TPCCConstants.NO_READ_UPDATE) {
				if (!firstOne) {
					sb.append(TPCCConstants.PROPERY_SEPERATOR);
				} else {
					firstOne = false;
				}
				sb.append(pNames[i]);
				sb.append(TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR);
				sb.append(pValues[i]);
				sb.append(TPCCConstants.PROPERY_ATTRIBUTE_SEPERATOR);
				sb.append(pTypes[i]);
			}
		}
		return sb.toString();
	}

	public static String getLogString(char type, String actionName, int threadId, int sequenceId, long startTime, long endTime, ArrayList<String> customers, ArrayList<String> orders) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(threadId+","+sequenceId);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
	
		sb.append(startTime);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(TPCCConstants.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < customers.size(); i++) {
			sb.append(customers.get(i));
			sb.append(TPCCConstants.ENTITY_SEPERATOR);
			sb.append(orders.get(i));
			if ((i + 1) != customers.size())
				sb.append(TPCCConstants.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}
}
