package com.galvin.mongodb;

import com.mongodb.DBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuietSleeper
{
    private static final Logger logger = LoggerFactory.getLogger( QuietSleeper.class );
    
    /**
    Sleeps until a MongoDB Collection is available. By default, sleeps for 0, 5000, 10000, 20000, and 30000 milliseconds
    @param connection a connection to MongoDB
    @param collectionName the name of the collection you want to retrieve
    @throws PersistenceException on error, or if the connection never becomes available
    */
    public static void sleepUntilAvailable( MongoDbConnection connection, String collectionName  )
        throws PersistenceException
    {
        sleepUntilAvailable( connection, collectionName, 0, 1000, 
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
    
    /**
    Sleeps until a MongoDB Collection is available.
    @param connection a connection to MongoDB
    @param collectionName the name of the collection you want to retrieve
    @param sleepTimes an unbounded number of sleep times; e.g. 1000, 10000, 20000, 30000 milliseconds
    @throws PersistenceException on error, or if the connection never becomes available
    */
    public static void sleepUntilAvailable( MongoDbConnection connection, String collectionName, int ... sleepTimes )
        throws PersistenceException
    {
        if( !isConnectionAvailable( connection, collectionName ) )
        {
            for( int sleepTime : sleepTimes )
            {
                if( isConnectionAvailable( connection, collectionName ) )
                {
                    logger.info( "Collection " + collectionName + " is available." );
                    return;
                }
                else
                {
                    logger.info( "Collection " + collectionName + " is not available; sleeping for: " + sleepTime + " milliseconds." );
                    quietlySleep( sleepTime );
                }
            }
            
            throw new PersistenceException( "Collection " + collectionName + " never became available." );
        }
        else
        {
            logger.info( "Collection " + collectionName + " is available." );
        }
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
    
    private static boolean isConnectionAvailable( MongoDbConnection connection, String collectionName )
    {
        try
        {
            DBCollection collection = connection.getCollection( collectionName );
            return collection != null;
        }
        catch( Throwable t )
        {
            return false;
        }
    }
}
