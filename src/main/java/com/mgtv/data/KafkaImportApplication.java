package com.mgtv.data;

import com.mgtv.data.service.CmdOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class KafkaImportApplication
{
    private static final Logger logger =
            LoggerFactory.getLogger(KafkaImportApplication.class);

    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context =
                SpringApplication.run(KafkaImportApplication.class, args);

        // 解析命令行
        CmdOptionHandler cmdRunner = context.getBean(CmdOptionHandler.class);

        // 根据命令行执行
        cmdRunner.handleArgumentOption();

        logger.info("kafka import task completed.");
    }
}
