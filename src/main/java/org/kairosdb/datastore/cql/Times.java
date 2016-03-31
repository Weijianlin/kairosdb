package org.kairosdb.datastore.cql;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;

/**
 * @author weijian
 * Date : 2016-03-17 22:34
 */

public final class Times {

    private Times() {
        throw new AssertionError();
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault());
    }


    public static int getSecondOfYear(LocalDateTime localDateTime){
        return (int) ((toTimestamp(localDateTime) - toTimestamp(startOfYear(localDateTime.getYear()))) / 1000);
    }

    public static long toTimestamp(int year, int secOfYear){
        return toTimestamp(startOfYear(year)) + secOfYear * 1000L;
    }

    public static LocalDateTime startOfYear(int year){
        return LocalDateTime.of(year, 1, 1, 0, 0);
    }

    public static long toTimestamp(LocalDateTime localDateTime){
        return localDateTime.atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli();
    }

    public static long startMilliOfYear(int year){
        return toTimestamp(startOfYear(year));
    }

    public static long endMilliOfYear(int year){
        return startMilliOfYear(year + 1) - 1;
    }

    public static void main(String[] args) {
        System.out.println(Times.toLocalDateTime(Long.MAX_VALUE));
        System.out.println(Times.toLocalDateTime(Long.MIN_VALUE));

        long now = System.currentTimeMillis();
        LocalDateTime dt = LocalDateTime.now();
        LocalDateTime nowdt = toLocalDateTime(now);

        int year = nowdt.getYear();
        System.out.println(now);
        System.out.println(toTimestamp(dt));
        System.out.println(dt);
        System.out.println(nowdt);
        System.out.println(nowdt.getYear());

        System.out.println(getSecondOfYear(nowdt));
        System.out.println(now/1000 - toTimestamp(LocalDateTime.of(year, 1, 1, 0, 0))/1000);

        System.out.println(toTimestamp(startOfYear(year)));
        System.out.println(startOfYear(year));
        System.out.println(toTimestamp(year, getSecondOfYear(nowdt)));


        System.out.println(startMilliOfYear(year));
        System.out.println(endMilliOfYear(year));
        System.out.println(toTimestamp(LocalDateTime.of(year, 12, 31, 23, 59, 59)));
    }
}
