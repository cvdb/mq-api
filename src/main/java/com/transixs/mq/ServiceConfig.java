package com.transixs.mq;

import com.transixs.config.manager.IConfig;
import com.transixs.config.manager.ConfigResolver;

public enum ServiceConfig implements IConfig {

  SERVICE_NAME, 
  PULSAR_SERVICE_URL, 
  SYNC_RESPONSE_TIMEOUT_SECONDS
    ;

  @Override
  public String getValue() {
    return ConfigResolver.getProperty(this);
  }

  @Override
  public String getValue(String defaultValue) {
    return ConfigResolver.getProperty(this, defaultValue);
  }

}


