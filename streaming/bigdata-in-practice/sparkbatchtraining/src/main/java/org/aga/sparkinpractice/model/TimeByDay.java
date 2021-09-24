package org.aga.sparkinpractice.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class TimeByDay {
    private int timeId;
    private String theDate;
    private String theDay;
    private String theMonth;
    private int theYear;
    private int dayOfMonth;
    private int weekOfYear;
    private int monthOfYear;
    private String quarter;
    private String fiscalPeriod;


    public static TimeByDay parse(String timeByDayAsString,String sep){
        String[] split = timeByDayAsString.split(sep);
        return TimeByDay.builder()
                .timeId(Integer.valueOf(split[0]))
                .theDate(split[1])
                .theDay(split[2])
                .theMonth(split[3])
                .theYear(Integer.valueOf(split[4]))
                .dayOfMonth(Integer.valueOf(split[5]))
                .weekOfYear(Integer.valueOf(split[6]))
                .monthOfYear(Integer.valueOf(split[7]))
                .quarter(split[8])
                .fiscalPeriod(split.length==10 ? split[9]:null)
                .build()
                ;
    }

    public static TimeByDay parse(String timeByDayAsString){
        return parse(timeByDayAsString,";") ;
    }

}