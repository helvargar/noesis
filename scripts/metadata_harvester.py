import json
import psycopg2
from cryptography.fernet import Fernet

def harvest_metadata():
    print("--- Project Revelation: Metadata Harvester (Standalone) ---")
    
    # Decryption key from app config
    MASTER_KEY = "3q9M1_u5u8PR-XZ7k3z2Kq5v8PR-XZ7k3z2Kq5v8PR8="
    cipher = Fernet(MASTER_KEY.encode())

    try:
        # 1. Get database credentials
        with open('data/tenants.json', 'r') as f:
            tenant_data = json.load(f)
            t_config = tenant_data['tenant_b4b6daaa']['database']
        
        password = cipher.decrypt(t_config['password_encrypted'].encode()).decode()
        
        conn = psycopg2.connect(
            host=t_config['host'],
            port=t_config['port'],
            database=t_config['database'],
            user=t_config['username'],
            password=password
        )
        cur = conn.cursor()
        schema = t_config.get('schema_name', 'guide')
        
        db_intel = {
            "schema": schema,
            "tables": {}
        }
        
        # 2. Get list of tables
        cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
        tables = [t[0] for t in cur.fetchall()]
        
        # We focus on the most important ones for the museum domain
        focus_tables = ['artistwork', 'artist', 'room', 'pathway', 'technique', 'artistcategory', 'site']
        
        for table in focus_tables:
            if table not in tables: continue
            
            print(f"Processing table: {table}...")
            db_intel["tables"][table] = {
                "columns": {},
                "ddl": "",
                "sample_values": {}
            }
            
            # 2a. Get Columns and Types
            cur.execute(f"""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
            """)
            cols = cur.fetchall()
            col_specs = []
            for col in cols:
                name, dtype, nullable = col
                db_intel["tables"][table]["columns"][name] = {"type": dtype, "nullable": nullable}
                col_specs.append(f"{name} {dtype.upper()}")
            
            db_intel["tables"][table]["ddl"] = f"CREATE TABLE {table} (\n  " + ",\n  ".join(col_specs) + "\n);"

            # 2b. Get Sample Values for relevant columns
            sample_cols = [
                'artistworktitle', 'artistname', 'roomname', 
                'techniquedescription', 'artistcategorydescription', 
                'pathwaydescription', 'pathwayname'
            ]
            
            for col in db_intel["tables"][table]["columns"]:
                if col.lower() in sample_cols:
                    print(f"  Sampling values for {col}...")
                    try:
                        cur.execute(f'SELECT DISTINCT "{col}" FROM {schema}.{table} WHERE "{col}" IS NOT NULL LIMIT 10')
                        samples = [str(r[0]) for r in cur.fetchall()]
                        db_intel["tables"][table]["sample_values"][col] = samples
                    except Exception as e:
                        print(f"  Error sampling {col}: {e}")
                        conn.rollback()

        # 3. Save intelligence
        with open('data/db_intelligence.json', 'w') as f:
            json.dump(db_intel, f, indent=2)
            
        print("\n--- Harvesting Complete! data/db_intelligence.json created. ---")
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    harvest_metadata()
