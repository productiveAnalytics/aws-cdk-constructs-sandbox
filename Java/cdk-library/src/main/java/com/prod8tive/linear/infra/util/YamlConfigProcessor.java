package com.prod8ctive.infra.util;

import com.prod8ctive.infra.config.LambdaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class YamlConfigProcessor {
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigProcessor.class);


    /**
     * load the flink.yaml, flink_<ENV>.yaml and module specific yaml configurations.
     * @return
     * @throws Exception
     */
    public static LambdaConfig loadLambdaConfig(String env) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        LambdaConfig lambdaConfig = new LambdaConfig();
        ObjectReader objectReader = mapper.readerForUpdating(lambdaConfig);
        String lambdaConfigFileName = "./src/main/resources/config/lambda_" + env + ".yaml";

        System.out.println("lambdaConfigFileName: " + lambdaConfigFileName);

        try {
            InputStream ins = new FileInputStream(lambdaConfigFileName);
            objectReader.readValue(ins);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("LOG:: load configirations env {} file {}", env, lambdaConfigFileName);
        return lambdaConfig;
    }
}
