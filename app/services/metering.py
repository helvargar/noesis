"""
Metering Service - Track usage per tenant for governance and billing plans.
"""
import json
import os
from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel

METERING_STORE_PATH = "./data/metering.json"

class UsageRecord(BaseModel):
    """Single usage record."""
    tenant_id: str
    timestamp: datetime
    query_type: str  # "sql", "rag", "hybrid"
    model_used: str
    estimated_tokens: int
    success: bool

class TenantUsageSummary(BaseModel):
    """Aggregated usage for a tenant."""
    tenant_id: str
    period: str  # e.g., "2024-02"
    total_queries: int
    sql_queries: int
    rag_queries: int
    hybrid_queries: int
    total_tokens: int
    successful_queries: int
    failed_queries: int

class MeteringService:
    """Tracks and reports usage metrics per tenant."""
    
    def __init__(self):
        self._records: List[UsageRecord] = []
        self._load_from_disk()
    
    def _load_from_disk(self):
        if os.path.exists(METERING_STORE_PATH):
            try:
                with open(METERING_STORE_PATH, "r") as f:
                    data = json.load(f)
                    self._records = [UsageRecord.model_validate(r) for r in data]
            except Exception as e:
                print(f"Warning: Could not load metering data: {e}")
    
    def _save_to_disk(self):
        os.makedirs(os.path.dirname(METERING_STORE_PATH), exist_ok=True)
        with open(METERING_STORE_PATH, "w") as f:
            json.dump([r.model_dump(mode="json") for r in self._records], f, indent=2, default=str)
    
    def record_usage(
        self,
        tenant_id: str,
        query_type: str,
        model_used: str,
        estimated_tokens: int,
        success: bool = True
    ):
        """Record a single query usage."""
        record = UsageRecord(
            tenant_id=tenant_id,
            timestamp=datetime.utcnow(),
            query_type=query_type,
            model_used=model_used,
            estimated_tokens=estimated_tokens,
            success=success
        )
        self._records.append(record)
        self._save_to_disk()
    
    def get_monthly_summary(self, tenant_id: str, year: int, month: int) -> TenantUsageSummary:
        """Get usage summary for a specific month."""
        period = f"{year}-{month:02d}"
        
        relevant = [
            r for r in self._records
            if r.tenant_id == tenant_id 
            and r.timestamp.year == year 
            and r.timestamp.month == month
        ]
        
        return TenantUsageSummary(
            tenant_id=tenant_id,
            period=period,
            total_queries=len(relevant),
            sql_queries=sum(1 for r in relevant if r.query_type == "sql"),
            rag_queries=sum(1 for r in relevant if r.query_type == "rag"),
            hybrid_queries=sum(1 for r in relevant if r.query_type == "hybrid"),
            total_tokens=sum(r.estimated_tokens for r in relevant),
            successful_queries=sum(1 for r in relevant if r.success),
            failed_queries=sum(1 for r in relevant if not r.success)
        )
    
    def get_current_month_count(self, tenant_id: str) -> int:
        """Get query count for current month (for limit checking)."""
        now = datetime.utcnow()
        return sum(
            1 for r in self._records
            if r.tenant_id == tenant_id
            and r.timestamp.year == now.year
            and r.timestamp.month == now.month
        )
    
    def estimate_tokens(self, query: str, response: str) -> int:
        """
        Rough token estimation (4 chars ≈ 1 token).
        In production, use tiktoken for accurate counts.
        """
        total_chars = len(query) + len(response)
        return total_chars // 4

# Singleton
metering_service = MeteringService()
