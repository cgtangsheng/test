package org.goldenorb.algorithms.count;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.IntMessage;

public class OrbCountJob extends OrbRunner {
	public static final String ALGORITHM_NAME = "Count";
	public static final String USAGE = "mapred.input.dir=/home/user/input/ mapred.output.dir=/home/user/output/ goldenOrb.orb.requestedPartitions=3 goldenOrb.orb.reservedPartitions=0";

	public static void main(String[] args) {
		OrbCountJob job = new OrbCountJob();
		job.startJob(args);
	}

	private void startJob(String[] args) {
		OrbConfiguration orbConf = new OrbConfiguration(true);

		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(CountVertex.class);
		orbConf.setMessageClass(IntMessage.class);
		orbConf.setVertexInputFormatClass(CountReader.class);
		orbConf.setVertexOutputFormatClass(CountWriter.class);
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
