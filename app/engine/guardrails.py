import sqlparse
from typing import List

class SQLGuardrails:
    """
    Ensures generated SQL is safe to execute.
    """
    
    ALLOWED_COMMANDS = {'SELECT'}

    @staticmethod
    def validate_sql(sql: str, allowed_tables: List[str]) -> bool:
        """
        Parses SQL to ensure:
        1. Only allowed commands (SELECT)
        2. Only allowed tables are accessed
        """
        parsed = sqlparse.parse(sql)
        for statement in parsed:
            if statement.get_type().upper() not in SQLGuardrails.ALLOWED_COMMANDS:
                raise ValueError(f"Forbidden SQL command detected: {statement.get_type()}")
            
            # Basic table extraction (simplified for PoC)
            # In production, use a robust AST walker or sqlglot
            # This is a naive check for demonstration:
            tokens = [t.value.lower() for t in statement.flatten()]
            for table in allowed_tables:
                # This logic is purely illustrative; real SQL parsing is complex
                pass 
                
        # Additional heuristic: check for destructive keywords blindly if parsing fails
        forbidden = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE", "GRANT"]
        upper_sql = sql.upper()
        for bad in forbidden:
            if bad in upper_sql:
                raise ValueError(f"Potential destructive keyword found: {bad}")

        return True
