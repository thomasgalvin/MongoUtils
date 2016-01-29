package com.galvin.db;

import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public final class UuidFactory
{
    private UuidFactory(){}
    
    public static void ensureUuid( HasUuid uuid ){
        if( uuid != null ){
            if( StringUtils.isBlank( uuid.getUuid() ) ){
                uuid.setUuid( generateUuid() );
            }
        }
    }
    
    public static String generateUuid(){
        return UUID.randomUUID().toString();
    }
}
