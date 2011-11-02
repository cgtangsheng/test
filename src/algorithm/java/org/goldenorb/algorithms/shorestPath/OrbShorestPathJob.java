package org.goldenorb.algorithms.shorestPath;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.TextMessage;

public class OrbShorestPathJob extends OrbRunner {
	public static final String ALGORITHM_NAME = "shorestPath";
	public static final String USAGE = "mapred.input.dir=/home/user/input/ mapred.output.dir=/home/user/output/ goldenOrb.orb.requestedPartitions=3 goldenOrb.orb.reservedPartitions=0";

	public static void main(String[] args) {
		OrbShorestPathJob job = new OrbShorestPathJob();
		job.startJob(args);
	}

	private void startJob(String[] args) {
		OrbConfiguration orbConf = new OrbConfiguration(true);

		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(ShorestPathVertex.class);
		orbConf.setMessageClass(TextMessage.class);
		orbConf.setVertexInputFormatClass(ShorestPathVertexReader.class);
		orbConf.setVertexOutputFormatClass(ShorestPathVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);

		try {
			parseArgs(orbConf, args, ALGORITHM_NAME);
		} catch (Exception e) {
			printHelpMessage();
			System.exit(-1);
		}

		try {
			orbConf.writeXml(System.out);
		} catch (IOException e) {
			e.printStackTrace();
		}

		runJob(orbConf);
	}

	@Override
	public void printHelpMessage() {
		super.printHelpMessage();
		System.out.println(USAGE);
	}
}
