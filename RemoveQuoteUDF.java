package com.cheng.ex.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RemoveQuoteUDF extends UDF{
	public Text evaluate(Text str) {
		if (str.toString() == null) {
			return null;
		}
		return new Text(str.toString().replaceAll("\"", ""));
	}
}
