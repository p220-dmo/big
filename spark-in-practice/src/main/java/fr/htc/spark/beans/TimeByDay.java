package fr.htc.spark.beans;

import static fr.htc.spark.common.CsvConstants.CSV_TIME_BY_DAY_TIME_ID;

import java.io.Serializable;

public class TimeByDay implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private Integer timeId;
	private Integer year;
	private String quarter;
	public Integer getTimeId() {
		return timeId;
	}
	public void setTimeId(Integer timeId) {
		this.timeId = timeId;
	}
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public String getQuarter() {
		return quarter;
	}
	public void setQuarter(String quarter) {
		this.quarter = quarter;
	}
	
	
	public static TimeByDay parse(String line) {
		String[] columns = line.split(";");
		TimeByDay timeByDate = new TimeByDay();
		
		timeByDate.setTimeId(Integer.parseInt(columns[CSV_TIME_BY_DAY_TIME_ID]));
		timeByDate.setYear(Integer.parseInt(columns[4]));
		timeByDate.setQuarter(columns[8]);
		
		return timeByDate;
		
		
	}

}
