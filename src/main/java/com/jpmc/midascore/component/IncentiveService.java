package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private final RestTemplate restTemplate;
    
    @Value("${incentive.api.url:http://localhost:8080/incentive}")
    private String incentiveApiUrl;

    public IncentiveService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public Incentive getIncentive(Transaction transaction) {
        try {
            logger.info("Calling incentive API for transaction: {}", transaction);
            Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
            logger.info("Received incentive: {}", incentive);
            return incentive;
        } catch (Exception e) {
            logger.error("Error calling incentive API for transaction: {}", transaction, e);
            return null;
        }
    }
}

