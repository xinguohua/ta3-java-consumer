package com.bbn.tc.services.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class TimeUtil {
    public static void main(String[] args) {
        // 指定 JSON 文件路径
//        String jsonFilePath = "/Users/xinguohua/Code/ta3-java-consumer/tc-bbn-kafka/ta1-cadets-e3-official-1.json";
//
//        // 指定时间区间（UTC 时间）
//        LocalDateTime startTime = LocalDateTime.of(2018, 4, 6, 11, 21);  // 开始时间
//        LocalDateTime endTime = LocalDateTime.of(2018, 4, 6, 12, 9);    // 结束时间
//
//        // 解析并收集日志
//        List<String> logsInRange = parseLogsInTimeRange(jsonFilePath, startTime, endTime);
//
//        String outputFilePath = "1.txt";
//        writeLogsToFile(logsInRange, outputFilePath);

        long seconds = 1523551475049393157L / 1_000_000_000;
        int nanoAdjustment = (int) (seconds % 1_000_000_000);

        // 使用秒和纳秒部分创建 Instant 对象
        Instant instant = Instant.ofEpochSecond(seconds, nanoAdjustment);

        // 将 Instant 转换为 UTC 时间的 LocalDateTime
        LocalDateTime logTime = LocalDateTime.ofInstant(instant, ZoneId.of("America/New_York"));
        System.out.println(logTime);
    }

    public static List<String> parseLogsInTimeRange(String jsonFilePath, LocalDateTime startTime, LocalDateTime endTime) {
        List<String> logsInRange = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    JsonNode logEntry = mapper.readTree(line);
                    if (logEntry != null) {
                        // 假设每个日志条目中有一个名为 "startTimestampNanos" 的字段
                        if (logEntry.has("datum") && logEntry.get("datum").has("com.bbn.tc.schema.avro.cdm18.Subject") &&
                                logEntry.get("datum").get("com.bbn.tc.schema.avro.cdm18.Subject").has("startTimestampNanos")) {
                            long nanos = logEntry.get("datum").get("com.bbn.tc.schema.avro.cdm18.Subject").get("startTimestampNanos").asLong();

                            // 将纳秒时间戳转换为秒和纳秒部分
                            long seconds = nanos / 1_000_000_000;
                            int nanoAdjustment = (int) (nanos % 1_000_000_000);

                            // 使用秒和纳秒部分创建 Instant 对象
                            Instant instant = Instant.ofEpochSecond(seconds, nanoAdjustment);

                            // 将 Instant 转换为 UTC 时间的 LocalDateTime
                            LocalDateTime logTime = LocalDateTime.ofInstant(instant, ZoneId.of("America/Los_Angeles"));
                            // 检查日志时间是否在指定时间区间内
//                            if (!logTime.isBefore(startTime) && !logTime.isAfter(endTime)) {
//                                logsInRange.add(logEntry.toString());
//                            }
                            logsInRange.add(logTime.toString());

                        }
                    }
                } catch (IOException e) {
                    System.err.println("Invalid JSON line: " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(logsInRange.toString());

        return logsInRange;
    }

    public static void writeLogsToFile(List<String> logs, String outputFilePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (String log : logs) {
                writer.write(log);
                writer.newLine();
            }
            System.out.println("Logs successfully written to " + outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
