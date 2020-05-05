import psycopg2
import os

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    print("we are here in get Open")
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    print("we are here in loadRatings")
    
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+" (row_id serial primary key,userid INT, ph VARCHAR(10),  movieid INT , qh VARCHAR(10),  rating FLOAT, rh VARCHAR(10), Timestamp INT)")
    loadout = open(ratingsfilepath,'r')
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('userid','ph','movieid','qh','rating','rh','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN ph, DROP COLUMN qh,DROP COLUMN rh, DROP COLUMN Timestamp")
    cur.close()
    



def rangePartition(ratingstablename, numberofpartitions, openconnection):
    print("This would have been more fun as a ranch, but oh well...")
    zero = 0.0
    five = 5.0
    partLen = abs(five-zero) / numberofpartitions

    cur = openconnection.cursor()
    for i in range(0,numberofpartitions):
        tblName = RANGE_TABLE_PREFIX + repr(i)
        createtable(tblName,cur)
        ll = i * partLen
        ul = ll + partLen
        if ll == zero:
            query = " INSERT INTO {0} SELECT  FROM {1} WHERE {2} >= {3} and {2} <= {4}".format(tblName,ratingstablename,'rating',ll,ul)
        else:
            query = " INSERT INTO {0} SELECT  FROM {1} WHERE {2} > {3} and {2} <= {4}".format(tblName,ratingstablename,'rating',ll,ul)

        cur.execute(query)
        openconnection.commit()
    pass

    


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    print("going round and round and round and I'm dizzy")
    partNum = 0
    cur = openconnection.cursor()
    for i in range(0,numberofpartitions):
        tbName = RROBIN_TABLE_PREFIX + repr(i)
        createtable(tbName,cur)

        if (i != (numberofpartitions - 1)):
            partNum = i + 1;
        else:
            partNum = 0;
        try:
            query = "INSERT INTO {0} SELECT {1},{2},{3} FROM (SELECT ROW_NUMBER() OVER() as row_number,* FROM {4}) as foo WHERE MOD(row_number,{5}) = cast ('{6}' as bigint) ".format(tbName,'userid', 'movieid','rating', ratingstablename, numberofpartitions, partNum)
            cur.execute(query)
            openconnection.commit()
        except Exception as zvaitika:
            print(zvaitika)
    pass



def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    partition = 0
    counter=tblCnt(cur, RROBIN_TABLE_PREFIX)
    prev = rows(cur, RROBIN_TABLE_PREFIX + repr(0) )
    
    for i in range(1,counter):
        nxt = rows(cur, RROBIN_TABLE_PREFIX + repr(i) )
        if ( nxt < prev ):
            partition = i
            break
        
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(ratingstablename,userid, itemid, rating)
    cur.execute(query)
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(RROBIN_TABLE_PREFIX+repr(partition),userid,itemid,rating)
    cur.execute(query)
    openconnection.commit()
    pass


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    numberofpartitions = tblCnt(cur, RANGE_TABLE_PREFIX)
    zero = 0.0
    five = 5.0
    partLen = abs(five-zero) / numberofpartitions

    for i in range( 0, numberofpartitions):
        ll = i * partLen
        ul = ll + partLen
        if ll == ll:
            if (rating >= ll) and (rating <= ul):
                break
        elif (rating > ll) and (rating <= ul):
            break

    pekuisa = i
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(ratingstablename,userid,itemid,rating)
    cur.execute(query)
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format(RANGE_TABLE_PREFIX + repr(pekuisa),userid,itemid,rating)
    cur.execute(query)
    openconnection.commit()
    pass


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()




def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
            
def createtable(tablename,cursor):
    try:
        query = "CREATE TABLE {0} ( {1} integer, {2} integer,{3} real);".format(tablename,'userid','movieid','rating')
        cursor.execute(query)
    except Exception as ex:
        print("Table not created because: ",ex)


def tblCnt(cur, tableprefix):
    query = "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND                                                                         table_name LIKE '{0}%';".format(tableprefix)
    cur.execute(query)
    partitioncount = int(cur.fetchone()[0])
    return partitioncount

def rows(cur,tablename):
    query = "SELECT count(*) FROM {0}".format(tablename)
    cur.execute(query)
    count = int(cur.fetchone()[0])
    return count
    
if __name__ == '__main__':

    
        
    try:

        createDB()

        with getOpenConnection() as con:
            loadRatings(ratingstablename, rfp, con)
            rangePartition(ratingstablename, 5, con)

    except Exception as detail:
        print ("Mayday! Error encountered. Details:  ==> ", detail)
