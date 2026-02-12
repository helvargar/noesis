
import os
import json
from sqlalchemy import create_engine, inspect, text
from app.core.security import decrypt_key
from app.services.tenant_service import tenant_service

def inspect_db():
    tenant_id = "tenant_b4b6daaa" # Acme Corp
    tenant = tenant_service.get_tenant(tenant_id)
    if not tenant:
        print("Tenant not found")
        return

    db_uri = tenant_service.get_db_connection_string(tenant_id)
    schema = tenant.database.schema_name or "guide"
    
    engine = create_engine(db_uri)
    inspector = inspect(engine)
    
    all_tables = inspector.get_table_names(schema=schema)
    print(f"Tables in schema '{schema}': {all_tables}")
    
    schema_info = {}
    
    for table_name in all_tables:
        print(f"\n--- Inspecting Table: {table_name} ---")
        columns = inspector.get_columns(table_name, schema=schema)
        schema_info[table_name] = {
            "columns": [],
            "samples": []
        }
        
        column_names = []
        for col in columns:
            col_info = {
                "name": col['name'],
                "type": str(col['type']),
                "comment": col.get('comment', '')
            }
            schema_info[table_name]["columns"].append(col_info)
            column_names.append(col['name'])
        
        # Get samples
        try:
            with engine.connect() as conn:
                # Limit to first 5 rows
                res = conn.execute(text(f"SELECT * FROM {schema}.{table_name} LIMIT 5"))
                rows = res.fetchall()
                for row in rows:
                    # Convert to dict for readability
                    schema_info[table_name]["samples"].append(dict(zip(column_names, row)))
        except Exception as e:
            print(f"Error sampling {table_name}: {e}")

    with open("data/full_schema_inspection.json", "w") as f:
        json.dump(schema_info, f, indent=2, default=str)
    
    print("\nInspection complete. Results saved to data/full_schema_inspection.json")

if __name__ == "__main__":
    inspect_db()
