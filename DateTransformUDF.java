package com.cheng.ex.hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class DateTransformUDF extends UDF{
	private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	
	/**
	 * 31/Aug/2015:00:04:37 +0800
	 * 
	 * 20150831000437
	 * 
	 * @param str
	 * @return
	 */
	public Text evaluate(Text input) {
		Text output = new Text();
		if (input == null) {
			return null;
		}
		String inputData = input.toString().trim();
		if (inputData == null) {
			return null;
		}
		Date parseInput;
		try {
			parseInput = inputFormat.parse(inputData);
			String outputData = outputFormat.format(parseInput);
			output.set(outputData);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return output;
		}
		return output;
	}
	public static void main(String[] args) {
		System.out.println(new DateTransformUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
	}
}
