package com.galvin.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrudUtil<T extends HasUuid> {
    private static final Logger logger = LoggerFactory.getLogger( CrudUtil.class );
    
    private static final String CANONICAL_NAME = "_____canonical_class_name_____";
    public static final String UUID_FIELD = "uuid";
    private DBCollection collection;
    private String className;
    private HashMap<String, CrudUtil> adapters = new HashMap();

    public CrudUtil( DBCollection collection, String className ) {
        this.collection = collection;
        this.className = className;
    }

    private BasicDBObject marshall( T target ) throws PersistenceException {
        try {
            ensureUuid( target );
            BasicDBObject result = new BasicDBObject();

            Class clazz = target.getClass();
            result.append( CANONICAL_NAME, clazz.getCanonicalName() );

            Field[] fields = clazz.getDeclaredFields();
            for( Field field : fields ) {
                if( recordField( field ) ) {
                    field.setAccessible( true );
                    Object value = field.get( target );

                    if( value != null ) {
                        String name = field.getName();
                        String fieldClassName = value.getClass().getCanonicalName();

                        CrudUtil adapter = getAdapter( fieldClassName );
                        
                        if( adapter != null && value instanceof HasUuid ) {
                            value = adapter.marshall( (HasUuid)value );
                        }
                        else if( value instanceof List ){
                            value = marshall( (List)value );
                        }
                        else if( isEnum( field ) ) {
                            Enum enumValue = (Enum)value;
                            value = enumValue.ordinal();
                        }

                        result.append( name, value );
                    }
                }
            }

            return result;
        }
        catch( IllegalAccessException ex ) {
            throw new PersistenceException( "Error in CrudUtil.marshall", ex );
        }
    }
    
    private BasicDBList marshall( List list ) throws PersistenceException {
        if( list != null ) {
            BasicDBList result = new BasicDBList();
            
            for( Object value : list ) {
                if( value != null ) {
                    String objectClassName = value.getClass().getCanonicalName();
                    CrudUtil adapter = getAdapter( objectClassName );

                    if( adapter != null && value instanceof HasUuid ) {
                        value = adapter.marshall( (HasUuid)value );
                    }
                    else if( value instanceof List ) {
                        value = marshall( (List)value );
                    }
                    else if( value instanceof Enum ) {
                        Enum enumValue = (Enum)value;
                        value = enumValue.ordinal();
                    }
                    
                    result.add( value );
                }
            }

            return result;
        }
        
        return null;
    }

    private void ensureUuid( T target ) {
        if( StringUtils.isBlank( target.getUuid() ) ) {
            target.setUuid( UUID.randomUUID().toString() );
        }
    }
    
    private boolean recordField( Field field ){
        return !Modifier.isTransient( field.getModifiers() );
    }
    
    private boolean setField( String name ){
        return !CANONICAL_NAME.equals( name ) &&
               !name.startsWith( "_" );
    }
    
    private boolean isEnum( Field field ){
        Class type = field.getClass();
        return type.isEnum();
    }

    private T unmarshall( DBObject record ) throws PersistenceException {
        return unmarshall( (BasicDBObject)record );
    }
    
    private T unmarshall( BasicDBObject record ) throws PersistenceException {
        try {
            Class clazz = Class.forName( className );
            T result = (T)clazz.newInstance();
            
            Set<Entry<String, Object>> entries = record.entrySet();
            Iterator<Entry<String, Object>> iter = entries.iterator();
            while( iter.hasNext() ){
                Entry<String, Object> entry = iter.next();
                String name = entry.getKey();
                if( setField( name ) ) {
                    Field field = clazz.getDeclaredField( name );
                    if( field != null ) {
                        Object value = entry.getValue();
                        if( value != null ) {
                            String fieldClassName = value.getClass().getCanonicalName();
                            CrudUtil adapter = getAdapter( fieldClassName );
                            
                            if( adapter != null && value instanceof BasicDBObject ) {
                                value = adapter.unmarshall( (BasicDBObject)value );
                            }
                            else if( value instanceof BasicDBList ){
                                value = unmarshall( (BasicDBList)value );
                            }
                            else if( isEnum( field ) ) {
                                int ordinal = (Integer)value;
                                value = field.getType().getEnumConstants()[ordinal];
                            }

                            field.setAccessible( true );
                            field.set( result, value );
                        }
                    }
                }
            }

            return result;
        }
        catch( ClassNotFoundException | 
               InstantiationException | 
               IllegalAccessException |
               NoSuchFieldException ex ) {
            throw new PersistenceException( "Error in CrudUtil.unmarshall", ex );
        }
    }
    
    private List unmarshall( BasicDBList list ) throws PersistenceException {
        List result = new ArrayList();
        
        if( list != null ){
            for( Object value : list ){
                if( value != null ){
                    String objectClassName = value.getClass().getCanonicalName();
                    if( value instanceof BasicDBObject ){
                        BasicDBObject tmp = (BasicDBObject)value;
                        objectClassName = tmp.getString( CANONICAL_NAME );
                    }
                    
                    CrudUtil adapter = getAdapter( objectClassName );
                    
                    if( adapter != null && value instanceof BasicDBObject ) {
                        value = adapter.unmarshall( (BasicDBObject)value );
                    }
                    else if( value instanceof BasicDBList ) {
                        value = unmarshall( (BasicDBList)value );
                    }
                    
                    result.add( value );
                }
            }
        }
        
        return result;
    }

    public String store( T target ) throws PersistenceException {
        String uuid = target.getUuid();
        if( !StringUtils.isBlank( uuid ) ){
            if( exists( uuid ) ){
                delete( uuid );
            }
        }
        
        BasicDBObject record = marshall( target );
        getCollection().insert( record );
        return target.getUuid();
    }

    public List<T> retrieve( boolean bool, String fieldName ) throws PersistenceException {
        
        BasicDBObject query = new BasicDBObject( fieldName, bool );
        DBCursor cursor = getCollection().find( query );
        List<T> result = new ArrayList( cursor.size() );

        while( cursor.hasNext() ) {
            result.add( unmarshall( cursor.next() ) );
        }

        return result;
    }

    public T retrieve( String uuid ) throws PersistenceException {
        return retrieve( uuid, UUID_FIELD );
    }

    public T retrieve( String uuid, String fieldName ) throws
        PersistenceException {
        List<T> result = retrieve( Arrays.asList( uuid ), fieldName );
        if( result != null && !result.isEmpty() ) {
            if( result.size() > 1 ){
                throw new PersistenceException( "Query for uuid [" + uuid + "] in field [" + fieldName + "] was not unique." );
            }
            return result.get( 0 );
        }
        return null;
    }
    
    public List<T> retrieveAll( String uuid ) throws PersistenceException {
        return retrieveAll( uuid, UUID_FIELD );
    }

    public List<T> retrieve( List<String> uuids ) throws PersistenceException {
        return retrieve( uuids, UUID_FIELD );
    }

    public List<T> retrieve( List<String> uuids, String fieldName ) throws PersistenceException {
        BasicDBObject query = createUuidQuery( uuids, fieldName );
        DBCursor cursor = getCollection().find( query );
        List<T> result = new ArrayList( cursor.size() );

        while( cursor.hasNext() ) {
            result.add( unmarshall( cursor.next() ) );
        }

        return result;
    }

    public BasicDBObject createUuidQuery( String uuid, String fieldName ) {
        List<String> uuids = new ArrayList();
        uuids.add( uuid );
        return createUuidQuery( uuids, fieldName );
    }

    public BasicDBObject createUuidQuery( List<String> uuids, String fieldName ) {
        String[] uuidArray = uuids.toArray( new String[ uuids.size() ] );
        BasicDBObject query = createInQuery( fieldName, uuidArray );
        return query;
    }

    public List<T> retrieveAll( String uuid, String fieldName ) throws
        PersistenceException {
        List<String> uuids = Arrays.asList( uuid );
        return retrieve( uuids, fieldName );
    }

    public List<T> retrieveAll() throws PersistenceException {
        DBCursor cursor = getCollection().find( createTypeQuery( className ) );
        List<T> result = new ArrayList( cursor.size() );

        while( cursor.hasNext() ) {
            DBObject record = cursor.next();
            T organization = unmarshall( record );
            result.add( organization );
        }

        return result;
    }

    public boolean exists( String uuid ) throws PersistenceException {
        return exists( uuid, UUID_FIELD );
    }

    public boolean exists( String uuid, String fieldName ) throws
        PersistenceException {
        return exists( Arrays.asList( uuid ), fieldName );
    }

    public boolean exists( List<String> uuids, String fieldName ) throws PersistenceException {
        String[] uuidArray = uuids.toArray( new String[ uuids.size() ] );
        BasicDBObject query = createInQuery( fieldName, uuidArray );
        BasicDBObject limit = createUuidQueryLimiter();
        DBCursor cursor = getCollection().find( query, limit );
        return cursor.hasNext();
    }

    public boolean delete( String uuid ) throws PersistenceException {
        return delete( uuid, UUID_FIELD );
    }

    public boolean delete( String uuid, String fieldName ) throws PersistenceException {
        return delete( Arrays.asList( uuid ), fieldName );
    }

    public boolean delete( List<String> uuids ) throws PersistenceException {
        return delete( uuids, UUID_FIELD );
    }

    public boolean delete( List<String> uuids, String fieldName ) throws PersistenceException {
        String[] uuidArray = uuids.toArray( new String[ uuids.size() ] );

        boolean existed = exists( uuids, fieldName );

        if( existed ) {
            BasicDBObject query = createInQuery( fieldName, uuidArray );
            getCollection().remove( query );

            boolean exists = exists( uuids, fieldName );
            return !exists;
        }
        else {
            return false;
        }
    }

    public boolean deleteAll() throws PersistenceException {
        boolean found = false;
        DBCursor cursor = collection.find();
        while( cursor.hasNext() ) {
            collection.remove( cursor.next() );
            found = true;
        }

        return found;
    }

    ////////////////////
    // search methods //
    ////////////////////
    
    public List<T> search( BasicDBObject dbObj ) throws PersistenceException {
        DBCursor cursor = this.getCollection().find( dbObj );
        List<T> result = new ArrayList( cursor.size() );

        while( cursor.hasNext() ) {
            DBObject record = cursor.next();
            T obj = this.unmarshall( record );
            result.add( obj );
        }

        return result;
    }

    public List<String> getUuids() throws PersistenceException {
        return getStrings( UUID_FIELD );
    }
    
    public List<String> getStrings( String fieldName ) throws PersistenceException {
        BasicDBObject query = new BasicDBObject( fieldName, new BasicDBObject( "$exists", true ) );
        BasicDBObject fields = new BasicDBObject( fieldName, 1 );

        DBCursor cursor = this.getCollection().find( query, fields );
        List<String> result = new ArrayList();

        while( cursor.hasNext() ) {
            DBObject record = cursor.next();
            result.add( record.get( fieldName ).toString() );
        }

        return result;
    }
    
    private BasicDBObject createInQuery( String field, Object[] values )
    {
        return new BasicDBObject( field, new BasicDBObject( "$in", values ) );
    }
    
    private BasicDBObject createUuidQueryLimiter()
    {
        BasicDBObject limit = new BasicDBObject( UUID_FIELD, 1 );
        return limit;
    }
    
    private BasicDBObject createTypeQuery( Class clazz )
    {
        return createTypeQuery( clazz.getCanonicalName() );
    }
    
    private BasicDBObject createTypeQuery( String className )
    {
        BasicDBObject query = new BasicDBObject( CANONICAL_NAME, className );
        return query;
    }

    ////////////////////
    // public methods //
    ////////////////////
    
    public DBCollection getCollection(){
        return collection;
    }
    
    public String getClassName() {
        return className;
    }
    
    public void register( String className, CrudUtil crudUtil ) {
        adapters.put( className, crudUtil );
    }
    
    public void unregister( String className ){
        adapters.remove( className );
    }
    
    private CrudUtil getAdapter(String className ){
        if( getClassName().equals( className ) ){
            return this;
        }
        else {
            return adapters.get( className );
        }
    }
}
