package com.galvin.mongodb;

public class LoginException
extends RuntimeException
{
    private String host;
    private int port;
    private String dbName;
    private String user;

    //////////////////
    // constructors //
    //////////////////
    
    public LoginException()
    {
    }

    public LoginException( String message )
    {
        super( message );
    }

    public LoginException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public LoginException( Throwable cause )
    {
        super( cause );
    }

    public LoginException( String host, int port, String dbName, String user )
    {
        super( createErrorMssage( host, port, dbName, user ) );
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
    }

    public LoginException( String host, int port, String dbName, String user, Throwable cause )
    {
        super( createErrorMssage( host, port, dbName, user ), cause );
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
    }
    
    private static String createErrorMssage( String host, int port, String dbName, String user )
    {
        return "Unable to connect to " + user + "@" + host + ":" + port + ":" + dbName;
    }
    
    /////////////////////////
    // getters and setters //
    /////////////////////////
    
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
    
}
