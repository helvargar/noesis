import re
import sqlparse
from typing import List

class SQLGuardrails:
    """
    Ensures generated SQL is safe to execute.
    """
    
    ALLOWED_COMMANDS = {'SELECT'}

    @staticmethod
    def validate_sql(sql: str, allowed_tables: list[str]) -> bool:
        """
        Parses SQL to ensure it's a safe SELECT query on allowed tables.
        """
        # Clean the SQL
        sql_clean = sql.strip().upper()
        
        # 1. Block destructive keywords absolutely
        forbidden = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE", "GRANT", "CREATE", "REPLACE"]
        for word in forbidden:
            # Match word with boundaries to avoid false positives (e.g. "altare")
            if re.search(rf"\b{word}\b", sql_clean):
                raise ValueError(f"Forbidden SQL command detected: {word}")

        # 2. Must start with SELECT
        if not sql_clean.startswith("SELECT"):
             raise ValueError("Only SELECT queries are allowed.")

        # 3. Simple table isolation check
        # We check that every word following a FROM or JOIN is in the allowed list
        # This is a heuristic but much safer than no check.
        # Format: FROM table, JOIN table
        pattern = r"(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)"
        matches = re.findall(pattern, sql_clean, re.IGNORECASE)
        for match in matches:
            # Remove schema prefix if present
            table_name = match.split(".")[-1]
            if table_name.lower() not in [t.lower() for t in allowed_tables]:
                raise ValueError(f"Access to table '{table_name}' is not authorized.")

        return True
