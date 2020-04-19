package edu.illinois.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/** a spout that generate sentences from a file */
public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext _context;
  private String inputFile;

  // Hint: Add necessary instance variables if needed
  FileReader _fr;
  
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this._context = context;
    this._collector = collector;

    /* ----------------------TODO-----------------------
    Task: initialize the file reader
    ------------------------------------------------- */

    // END
    try {
      this._fr = new FileReader(this.inputFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // Set input file path
  public FileReaderSpout withInputFileProperties(String inputFile) {
    this.inputFile = inputFile;
    return this;
  }

  @Override
  public void nextTuple() {

    /* ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to add a small sleep when the file is entirely read to prevent a busy-loop
    ------------------------------------------------- */
    // END
    try {
      BufferedReader br = new BufferedReader(this._fr);
      //CHANGE if sentence is delimited by period, not new line!
      String line;
      while ((line = br.readLine()) != null) {
        this._collector.emit(new Values(line));
      }
      br.close();
      Utils.sleep(1000);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define the declarer
    ------------------------------------------------- */

    // END
    declarer.declare(new Fields("sentence"));
  }

  @Override
  public void close() {
    /* ----------------------TODO-----------------------
    Task: close the file
    ------------------------------------------------- */

    // END
    try {
      this._fr.close();
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  public void fail(Object msgId) {}

  public void ack(Object msgId) {}

  @Override
  public void activate() {}

  @Override
  public void deactivate() {}

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
