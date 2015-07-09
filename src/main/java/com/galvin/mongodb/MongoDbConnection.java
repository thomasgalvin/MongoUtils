package com.galvin.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbConnection
{

    private final Logger logger = LoggerFactory.getLogger( MongoDbConnection.class );
    public static final int DEFAULT_MONGO_DB_PORT = 27017;

    private String host;
    private int port;
    private String dbName;
    private String user;
    private String password;

    private MongoClient mongo;
    private DB db;

    private final Object MONGO_CLIENT_LOCK = new Object();
    private final Object MONGO_DB_LOCK = new Object();
    private final Object USERS_COLLECTION_LOCK = new Object();

    //////////////////
    // constructors //
    //////////////////
    
    public MongoDbConnection()
    {
        this( "mydata" );
    }
    
    public MongoDbConnection( String dbName )
    {
        this( "127.0.0.1", DEFAULT_MONGO_DB_PORT, dbName, null, null );
    }
    
    public MongoDbConnection( String host, int port, String dbName )
    {
        this( host, port, dbName, null, null );
    }

    public MongoDbConnection( String host, String dbName )
    {
        this( host, DEFAULT_MONGO_DB_PORT, dbName, null, null );
    }

    public MongoDbConnection( String host, String dbName, String user, String password )
    {
        this( host, DEFAULT_MONGO_DB_PORT, dbName, user, password );
    }

    public MongoDbConnection( String host, int port, String dbName, String user, String password )
    {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
    }

    ///////////////
    // accessors //
    ///////////////
    
    public MongoClient getMongoClient() throws UnknownHostException
    {
        synchronized( MONGO_CLIENT_LOCK )
        {
            if( mongo == null )
            {
                logger.info( "Connecting to MongoDB running on " + host + ":" + port );
                mongo = new MongoClient( host, port );
                logger.info( "Connecting to MongoDB running on " + host + ":" + port + " sucessful." );
            }
        }
        return mongo;
    }

    public DB getDB() throws UnknownHostException, MongoDbAuthenticationException
    {
        synchronized( MONGO_DB_LOCK )
        {
            if( db == null )
            {
                logger.info( "Connecting to " + host + ":" + port + ":" + dbName );
                db = getMongoClient().getDB( dbName );

                //authentication
                if( !StringUtils.isEmpty( user ) && !StringUtils.isEmpty( password ) )
                {
                    logger.info( "Authenticating as: " + user );
                    boolean auth = db.authenticate( user, password.toCharArray() );
                    if( auth )
                    {
                        logger.info( "Authenticatation successful." );
                    }
                    else
                    {
                        db = null;
                        throw new MongoDbAuthenticationException( host, port, dbName, user );
                    }
                }
                
                logger.info( "Connecting to " + host + ":" + port + ":" + dbName + " sucessful." );
            }
        }
        return db;
    }

    public DBCollection getCollection( String collectionName ) throws UnknownHostException, MongoDbAuthenticationException
    {
        synchronized( USERS_COLLECTION_LOCK )
        {
            DBCollection collection;
            
            if( getDB().collectionExists( collectionName ) )
            {
                collection = getDB().getCollection( collectionName );
            }
            else
            {
                logger.info( "Creating collection: " + collectionName );
                DBObject collectionParameters = new BasicDBObject();
                collection = getDB().createCollection( collectionName, collectionParameters );
                logger.info( "Collection + " + collectionName + " created." );
            }
            return collection;
        }
    }
    
    //////////////////////////
    // getters and setters //
    //////////////////////////
    
    public String getHost()
    {
        return host;
    }

    public void setHost( String host )
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort( int port )
    {
        this.port = port;
    }

    public String getDbName()
    {
        return dbName;
    }

    public void setDbName( String dbName )
    {
        this.dbName = dbName;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser( String user )
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword( String password )
    {
        this.password = password;
    }
    
}
