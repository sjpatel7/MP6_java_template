package edu.illinois.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/** a bolt that tracks word count */
public class WordCountBolt extends BaseBasicBolt {
  // Hint: Add necessary instance variables if needed

  Map<String, Integer> counts = new HashMap<String, Integer>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    /* ----------------------TODO-----------------------
    Task: word count
		Hint: using instance variable to tracking the word count
    ------------------------------------------------- */

		// End
		String w = tuple.getString(0);
		Integer count = counts.get(w);
		if (count == null) {
			count = 1;
			counts.put(w, count);
			collector.emit(new Values(w, count));
		} else {
			count++;
			counts.put(w, count);
			collector.emit(new Values(w, count));
		}
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: declare output fields
    ------------------------------------------------- */

		// End
		declarer.declare(new Fields("word", "count"));
  }
}
