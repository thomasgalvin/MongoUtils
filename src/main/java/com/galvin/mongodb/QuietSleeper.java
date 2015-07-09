package com.galvin.mongodb;

import com.mongodb.DBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuietSleeper
{
    private static final Logger logger = LoggerFactory.getLogger( QuietSleeper.class );
    
    public static DBCollection sleepUntilAvailable( MongoDbConnection connection, String collectionName  )
        throws PersistenceException
    {
        return sleepUntilAvailable( connection, collectionName, 0, 1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000, 
                                                                   1000 );
    }
    
    public static DBCollection sleepUntilAvailable( MongoDbConnection connection, String collectionName, int ... sleepTimes )
        throws PersistenceException
    {
        DBCollection result = getConnection( connection, collectionName );
        if( result == null ) {
            for( int sleepTime : sleepTimes ) {
                result = getConnection( connection, collectionName );
                if( result != null ) {
                    return result;
                }
                else {
                    quietlySleep( sleepTime );
                }
            }
            
            throw new PersistenceException( "Collection " + collectionName + " never became available." );
        }
        return result;
    }
    
    private static void quietlySleep( long sleepTime )
    {
        try
        {
            Thread.sleep( sleepTime );
        }
        catch( Throwable t )
        {}
    }
    
    private static DBCollection getConnection( MongoDbConnection connection, String collectionName )
    {
        try {
        return connection.getCollection( collectionName );
        } catch( Throwable t ) {
            return null;
        }
    }
}
