import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Purpose of this method : Creates and connects to sparkifydb database. Returns cursor and connection to DB
     ----------------
    Return
    cur : Cursor of the sparkifydb database  |Object type should be psycopg2.cursor
    conn : Connectio to the sparkifycdb database |object type should be psycopg2.connect
    ----------------
    """
    # connect to default database
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres studentdb")
        print(e)
    
    conn.set_session(autocommit=True)
    
    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the studentdb")
        print(e)
    

    
    # create sparkify database with UTF8 encoding
    try: 
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e: 
        print("Error: Could not drop sparkifydb ")
        print(e)
    
    try: 
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e: 
        print("Error: Could not create sparkifydb ")
        print(e)
    

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres sparkifydb")
        print(e)
        
    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the sparkifydb")
        print(e)
        
    return cur, conn


def drop_tables(cur, conn):
    """
    Purpose of this method: Drops all tables exsiting and created on the database
     ----------------
    Parameter
    cur : Cursor of the sparkifydb database  |Object type should be psycopg2.cursor
    conn : Connectio to the sparkifycdb database |object type should be psycopg2.connect
    ----------------
    """
    for query in drop_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print(f"Error: Could not excute table drop query")
            print(e)
        


def create_tables(cur, conn):
    """
    Purpose of this method: Created tables defined on the sql_queries script
    ----------------
    Parameter
    cur : Cursor of the sparkifydb database  |Object type should be psycopg2.cursor
    conn : Connectio to the sparkifycdb database |object type should be psycopg2.connect
    ----------------
    """
    
    for query in create_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print(f"Error: Could not excute table create query")
            print(e)


def main():
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()