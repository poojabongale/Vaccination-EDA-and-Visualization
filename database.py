import sqlite3
import pandas as pd

DB_NAME = "health_data.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # Countries table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        iso_code CHAR(3) PRIMARY KEY,
        country_name TEXT NOT NULL,
        who_region TEXT
    );
    """)

    # Diseases table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS diseases (
        disease_code TEXT PRIMARY KEY,
        disease_description TEXT NOT NULL
    );
    """)

    # Vaccines table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vaccines (
        vaccine_code TEXT PRIMARY KEY,
        vaccine_name TEXT NOT NULL
    );
    """)

    # Coverage data
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS coverage_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT,
        iso_code CHAR(3),
        name TEXT,
        year INTEGER,
        antigen TEXT,
        antigen_description TEXT,
        coverage_category TEXT,
        coverage_category_description TEXT,
        target_number INTEGER,
        doses INTEGER,
        coverage REAL,
        FOREIGN KEY (iso_code) REFERENCES countries(iso_code)
    );
    """)

    # Incidence rate
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS incidence_rate (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT,
        iso_code CHAR(3),
        country_name TEXT,
        year INTEGER,
        disease_code TEXT,
        disease_description TEXT,
        denominator INTEGER,
        incidence_rate REAL,
        FOREIGN KEY (iso_code) REFERENCES countries(iso_code)
    );
    """)

    # Reported cases
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reported_cases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT,
        iso_code CHAR(3),
        country_name TEXT,
        year INTEGER,
        disease_code TEXT,
        disease_description TEXT,
        cases INTEGER,
        FOREIGN KEY (iso_code) REFERENCES countries(iso_code)
    );
    """)

    # Vaccine introduction
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vaccine_introduction (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        iso_code CHAR(3),
        country_name TEXT,
        who_region TEXT,
        year INTEGER,
        description TEXT,
        intro TEXT,
        FOREIGN KEY (iso_code) REFERENCES countries(iso_code)
    );
    """)

    # Vaccine schedule
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vaccine_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        iso_code CHAR(3),
        country_name TEXT,
        who_region TEXT,
        year INTEGER,
        vaccine_code TEXT,
        vaccine_description TEXT,
        schedule_rounds TEXT,
        target_pop TEXT,
        target_pop_description TEXT,
        geoarea TEXT,
        age_administered TEXT,
        source_comment TEXT,
        FOREIGN KEY (iso_code) REFERENCES countries(iso_code)
    );
    """)

    conn.commit()
    conn.close()
    print("✅ Tables created successfully!")

# Column mapping for CSV cleaning
COLUMN_MAPPING = {
    "ISO_3_CODE": "ISO_CODE",
    "COUNTRYNAME": "COUNTRY_NAME",
    "COUNTRY": "COUNTRY_NAME"
}

def load_csv_to_sqlite(csv_file, table_name):
    try:
        conn = get_connection()
        df = pd.read_csv(csv_file)
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        df.columns = df.columns.str.upper().str.replace(" ", "_")
        df.rename(columns={"GROUP": "GROUP_NAME"}, inplace=True)
        df.rename(columns=COLUMN_MAPPING, inplace=True)

        if "ISO_CODE" in df.columns:
            df = df.dropna(subset=["ISO_CODE"])

        table_cols = pd.read_sql_query(f"PRAGMA table_info({table_name});", conn)["name"].tolist()
        df = df[[c for c in df.columns if c in [col.upper() for col in table_cols]]]

        if df.empty:
            print(f"No matching columns for {table_name}, skipping insert.")
        else:
            df.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"✅ Loaded data into {table_name}")

    except Exception as e:
        print(f"❌ Error loading {csv_file} into {table_name}: {e}")
    finally:
        conn.close()

def populate_vaccines():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print("Fetching vaccine data...")
    # Extract unique vaccines from the vaccine_schedule table
    vaccine_query = """
        SELECT DISTINCT vaccine_description
        FROM vaccine_schedule
        WHERE vaccine_description IS NOT NULL
    """
    vaccine_df = pd.read_sql_query(vaccine_query, conn)

    # Drop duplicates
    vaccine_df = vaccine_df.drop_duplicates().reset_index(drop=True)

    # Generate unique vaccine codes
    vaccine_df["vaccine_code"] = ["VAC" + str(i + 1).zfill(3) for i in range(len(vaccine_df))]

    vaccine_df = vaccine_df[["vaccine_code", "vaccine_description"]]
    vaccine_df.rename(columns={"vaccine_description": "vaccine_name"}, inplace=True)

    vaccine_df.to_sql("vaccines", conn, if_exists="replace", index=False)
    print(f"✅ Populated vaccines table with {len(vaccine_df)} rows.")

    conn.close()
def populate_diseases():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # ---- POPULATE DISEASES ----
    print("Fetching disease data...")
    disease_query = """
        SELECT DISTINCT disease_description
        FROM incidence_rate
        WHERE disease_description IS NOT NULL \
        """
    disease_df = pd.read_sql_query(disease_query, conn)

    # Drop duplicates and reset index
    disease_df = disease_df.drop_duplicates().reset_index(drop=True)

    # Generate unique disease codes
    disease_df["disease_code"] = ["DIS" + str(i + 1).zfill(3) for i in range(len(disease_df))]

    # Reorder columns
    disease_df = disease_df[["disease_code", "disease_description"]]

    # Insert into diseases table
    disease_df.to_sql("diseases", conn, if_exists="replace", index=False)
    print(f"✅ Populated diseases table with {len(disease_df)} rows.")
    conn.close()
    print("🎉 Done!")

def populate_countries():
    conn = sqlite3.connect(DB_NAME)

    print("Fetching country data...")
    # Load columns in coverage_data
    coverage_cols = pd.read_sql_query("PRAGMA table_info(coverage_data);", conn)["name"].tolist()
    country_col = "name" if "name" in coverage_cols else None

    if not country_col:
        raise ValueError("No country column found in coverage_data")

    # Get unique country names
    query = f"""
        SELECT DISTINCT {country_col} AS country
        FROM coverage_data
        WHERE {country_col} IS NOT NULL
    """
    country_df = pd.read_sql_query(query, conn)

    # Generate ISO codes
    country_df = country_df.drop_duplicates().reset_index(drop=True)
    country_df["iso_code"] = ["C" + str(i+1).zfill(3) for i in range(len(country_df))]
    country_df = country_df[["iso_code", "country"]]

    # Write to DB
    country_df.to_sql("countries", conn, if_exists="replace", index=False)
    print(f"✅ Populated countries table with {len(country_df)} rows.")
    conn.close()

if __name__ == "__main__":
    create_tables()
    load_csv_to_sqlite("coverage_cleaned.csv", "coverage_data")
    load_csv_to_sqlite("incidence_rate_data_cleaned.csv", "incidence_rate")
    load_csv_to_sqlite("reported_cases_data_cleaned.csv", "reported_cases")
    load_csv_to_sqlite("vaccine_introduction_data_cleaned.csv", "vaccine_introduction")
    load_csv_to_sqlite("vaccine_schedule_data_cleaned.csv", "vaccine_schedule")
    populate_vaccines()
    populate_diseases()
    populate_countries()
    print("✅ Database setup complete!")
