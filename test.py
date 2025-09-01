import sqlite3
import pandas as pd

# This creates a file health_data.db
DB_NAME = "health_data.db"
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

def get_connection():
    return sqlite3.connect(DB_NAME)

def run_query(query,params=None):
    try:
        if params is None:
            params = ()

        query_type = query.strip().split()[0].upper()

        if query_type == "SELECT":
            # ✅ Let pandas manage connection
            with sqlite3.connect(DB_NAME) as conn:
                df = pd.read_sql_query(query, conn, params=params)
            return df
        else:
            # ✅ Manual execution for INSERT/UPDATE/DELETE
            with sqlite3.connect(DB_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
            print("✅ Query executed successfully!")
            return None
    except Exception as e:
        print(f"❌ Error executing query: {e}")
    finally:
        conn.close()

print(run_query("SELECT * FROM DISEASES LIMIT 10"))
print(run_query("SELECT * FROM VACCINES LIMIT 10"))
print(run_query("SELECT * FROM COUNTRIES LIMIT 10"))
#print(run_query("SELECT DISTINCT disease_code, disease_description FROM incidence_rate LIMIT 10"))
