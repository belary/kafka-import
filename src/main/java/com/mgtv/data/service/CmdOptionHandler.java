package com.mgtv.data.service;


import com.mgtv.data.config.CmdlineConstants;
import com.mgtv.data.utils.CollectionUtils;
import com.mgtv.data.utils.LogProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class CmdOptionHandler {


    private static final Logger log =
            LoggerFactory.getLogger(CmdOptionHandler.class);

    @Resource
    private ApplicationArguments arguments;

    public void handleArgumentOption() throws Exception {

        if (CollectionUtils.isEmpty(arguments.getOptionValues(CmdlineConstants.BOOTSTRAP))
                || CollectionUtils.isEmpty(arguments.getOptionValues(CmdlineConstants.TOPIC))
                || CollectionUtils.isEmpty(arguments.getOptionValues(CmdlineConstants.FROM_DIR))
                || CollectionUtils.isEmpty(arguments.getOptionValues(CmdlineConstants.MAX_RECORD_PER_SECOND))
        ) {
            log.info("invalid params");
            printUsage();
            return;
        }

        if (arguments.containsOption(CmdlineConstants.HELP)) {
            printUsage();
            return;
        }

        kafkaTransactionProcess();
    }

    private void kafkaTransactionProcess() throws IOException {

        String fromDir, bootStrap, topic;
        fromDir = getFirstElement(arguments.getOptionValues(CmdlineConstants.FROM_DIR));
        bootStrap= getFirstElement(arguments.getOptionValues(CmdlineConstants.BOOTSTRAP));
        topic = getFirstElement(arguments.getOptionValues(CmdlineConstants.TOPIC));
        int maxRecordPerSec = Integer.parseInt(getFirstElement(arguments.getOptionValues(CmdlineConstants.MAX_RECORD_PER_SECOND)));
        LogProducer logProducer = new LogProducer(bootStrap, topic, maxRecordPerSec);

        sendKafkaTask(fromDir, logProducer);
    }

    private void sendKafkaTask(String fromDir, LogProducer logProducer) throws IOException {

        Stream<Path> logFiles= Files.list(Paths.get(fromDir));

        List<String> totalList = new ArrayList<>();
        // stream流式并发处理
        logFiles.parallel().forEach(
                p ->{
                    try (BufferedReader reader = Files.newBufferedReader(p)) {
                        List<String> streamLogs = reader.lines().collect(Collectors.toList());
                        List<String> mgtvIos= streamLogs
                                .parallelStream()
                                .filter(s-> s.toLowerCase().indexOf("lib%22%3A%22mgtvandroid".toLowerCase()) > 0
                                        && s.toLowerCase().contains("event_name%22%3A%22page".toLowerCase())
                                ).collect(Collectors.toList());
                        totalList.addAll(mgtvIos);
                    } catch (Exception ex) {
                        log.error("reading file error", ex);
                    }
                }
        );

        totalList.forEach(System.out::println);
    }

    private <T> T getFirstElement(List<T> lstElements) {
        if (lstElements == null || lstElements.size() == 0) {
            return null;
        }
        return lstElements.get(0);
    }


    public static void printUsage() {
        final String USAGE = "\n" +
        "Usage:\n" +
        "    s3download [--recursive] [--pause] <s3_path> <local_paths>\n\n" +
        "Where:\n" +
        "    --from_dir               - local dir to import.\n\n" +
        "    --bootstrap              - Specify the kafka bootstrap server seperated by comma.\n\n" +
        "    --topic                  - kafka topic to send\n\n" +
        "    --max_record_per_second  - Throttle the send speed by per second \n\n" +

        "Examples:\n" +
        "         --local_path=e:\\tmp\\test  --bootstrap=10.26.33.7:6667,10.26.33.8:6667,10.26.33.102:6667 --topic=transaction --max_record_per_second=100\n\n";

        System.out.println(USAGE);
        System.out.println("    --bootstrap=${bootstrap} --topic=${topic} --max_record_per_second=${max_record_per_second}");
    }

}
