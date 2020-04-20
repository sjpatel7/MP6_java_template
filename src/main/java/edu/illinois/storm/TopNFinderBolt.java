package edu.illinois.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*; //for TreeMap

/** a bolt that finds the top n words. */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;

  // Hint: Add necessary instance variables and inner classes if needed
  private int _n;
  private TreeMap<Integer, String> _topNMap;
  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  public TopNFinderBolt withNProperties(int N) {
    /* ----------------------TODO-----------------------
    Task: set N
    ------------------------------------------------- */

		// End
	  	this._n = N;
	  	this._topNMap = new TreeMap<Integer, String>();
		return this;
  }

  @Override
  public void execute(Tuple tuple) {
    /* ----------------------TODO-----------------------
    Task: keep track of the top N words
		Hint: implement efficient algorithm so that it won't be shutdown before task finished
		      the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
    ------------------------------------------------- */

		// End
	  String word = tuple.getStringByField("word");
	  Integer count = tuple.getIntegerByField("count");
	  //if word already in treeMap, remove and update
	  //if (_topNMap.values().contains(word)) {
	  	//for (Integer key : _topNMap.keySet()) {
			//if (_topNMap.get(key).equals(word)) {
				//_topNMap.remove(key);
			//}
		//}
	  //}
	  if (_topNMap.size() < _n) {
	  	//add word and count if less than N elements in top N
	  	_topNMap.put(count, word);
	  } else if (count > _topNMap.firstKey()) {
	  	_topNMap.put(count, word);
	  }
		  
	  if (_topNMap.size() > _n) {
	  	_topNMap.remove(_topNMap.firstKey());
	  }
	  
	  String topNList = "";
	  for (Integer key : _topNMap.keySet()) {
		  topNList += _topNMap.get(key) + ", ";
	  }
	  topNList = topNList.substring(0, topNList.length() - 2);
	  collector.emit(new Values("top-N", topNList));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define output fields
		Hint: there's no requirement on sequence;
					For example, for top 3 words set ("hello", "word", "cs498"),
					"hello, world, cs498" and "world, cs498, hello" are all correct
    ------------------------------------------------- */

    // END
	  declarer.declare(new Fields("top-N", "list"));
  }

}
