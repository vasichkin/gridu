package com.gridu;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;



@Description(
  name="IpInBlock",
  value="returns 'True', when Ip belongs to network, null otherwise",
  extended="SELECT IpInBlock('127.0.0.1', '27.0.0.0/24') from foo limit 1;"
  )
public class IpInBlock extends UDF {
  
  public Text evaluate(Text addr, Text range) {
    String res;
    try {
      SubnetUtils utils = new SubnetUtils(range.toString());
      boolean isInRange = utils.getInfo().isInRange(addr.toString());
      res = (isInRange) ? "True" : "False";
    }
    catch (Throwable t) {
      res = "None";
    }
    return new Text(res);
  }
}