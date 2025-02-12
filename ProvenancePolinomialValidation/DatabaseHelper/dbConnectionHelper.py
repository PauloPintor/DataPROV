import psycopg2


def executeQuery(query):
    HOST = "localhost"
    USER = "***"
    PASSWORD = "***"
    PORT = 5432
    DATABASE = "tpch"

    connection = None
    try:
        # Connect to DB
        connection = psycopg2.connect(
            host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT
        )

        # Execute SQL query
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        column_names = (
            [desc[0] for desc in cursor.description] if cursor.description else []
        )

        return rows, column_names

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to PostgreSQL: {error}")

    finally:
        # Close cursor and connection
        if connection:
            cursor.close()  # type: ignore
            connection.close()
