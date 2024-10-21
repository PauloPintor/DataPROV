import psycopg2

def executeQuery(query):
    # Details for database connection
    HOST = "localhost"
    DATABASE = "ProvenanceValidation"
    USER = "postgres"
    PASSWORD = "postgres"
    PORT = 5433

    try:
        # Connect to DB
        connection = psycopg2.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            port = PORT
        )
        
        # Execute SQL query
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        return rows

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to PostgreSQL: {error}")
        
    finally:
        # Close cursor and connection
        if connection:
            cursor.close()
            connection.close()
#            print("Connection to PostgreSQL closed.")