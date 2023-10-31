import psycopg2

# Connect to your postgres DB
# conn = psycopg2.connect('postgresql://airflow:airflow@localhost/postgres')

# # Open a cursor to perform database operations
# cur = conn.cursor()

# # Execute a query
# cur.execute("create table public.characters (id integer, name varchar, status varchar, species varchar, type varchar, gender varchar, origin varchar, location varchar, image varchar, url varchar, created timestamp, primary key (id))")
# conn.commit()
# Retrieve query results
# records = cur.fetchall()

# print(records)

f = open('./tmp/data_dump.csv', 'r')

print(f.readlines())