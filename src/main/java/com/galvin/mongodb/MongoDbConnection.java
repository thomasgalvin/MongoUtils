package com.galvin.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;

public class MongoDbConnection
{
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
                mongo = new MongoClient( host, port );
            }
        }
        return mongo;
    }

    public DB getDB() throws UnknownHostException, LoginException
    {
        synchronized( MONGO_DB_LOCK )
        {
            if( db == null )
            {
                db = getMongoClient().getDB( dbName );

                //authentication
                if( !StringUtils.isEmpty( user ) && !StringUtils.isEmpty( password ) )
                {
                    boolean auth = db.authenticate( user, password.toCharArray() );
                    if( !auth )
                    {
                        db = null;
                        throw new LoginException( host, port, dbName, user );
                    }
                }
            }
        }
        return db;
    }

    public DBCollection getCollection( String collectionName ) throws UnknownHostException, LoginException
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
                DBObject collectionParameters = new BasicDBObject();
                collection = getDB().createCollection( collectionName, collectionParameters );
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
