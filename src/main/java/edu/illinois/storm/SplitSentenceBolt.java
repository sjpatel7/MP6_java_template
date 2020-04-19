package edu.illinois.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/** a bolt that split sentences */
public class SplitSentenceBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    /* ----------------------TODO-----------------------
    Task: split sentence and emit words
		Hint: split on "[^a-zA-Z0-9-]"
    ------------------------------------------------- */

		// End
		String sentence = tuple.getString(0);
		String[] words = sentence.split("[^a-zA-Z0-9-]");
		for (int i = 0; i < words.length; i++) {
			collector.emit(new Values(words[i]));
		}
		
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: declare output fields
    ------------------------------------------------- */

		// End
		declarer.declare(new Fields("word"));
  }
}
