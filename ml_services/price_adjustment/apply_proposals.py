from __future__ import annotations

import os
import json
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy import text, true

from .config import get_engine


def fetch_candidates(auto_apply_pending: bool=True, limit: int=200) -> pd.DataFrame:
    engine = get_engine()
    if auto_apply_pending:
        q = text(
            """
            SELECT id, sku, old_price, proposed_price, reason_code
            FROM price_adjustments
            WHERE (status = 'approved' OR status = 'pending')
              AND (effective_from IS NULL OR effective_from <= NOW())
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY created_at
            LIMIT :lim
            """
        )
        return pd.read_sql(q, engine, params={"lim": limit})
    else:
        q = text(
            """
            SELECT id, sku, old_price, proposed_price, reason_code
            FROM price_adjustments
            WHERE status = 'approved'
              AND (effective_from IS NULL OR effective_from <= NOW())
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY approved_at NULLS LAST, created_at
            LIMIT :lim
            """
        )
        return pd.read_sql(q, engine, params={"lim": limit})


def apply_one(conn, proposal: Dict[str, Any]) -> str:
    sku = str(proposal["sku"])
    old_price = float(proposal["old_price"]) if proposal["old_price"] is not None else None
    new_price = float(proposal["proposed_price"])
    reason_code = proposal.get("reason_code", "proposal_apply")

    # 1) Update current price, ensure old_price matches if provided
    if old_price is not None:
        res = conn.execute(
            text(
                """
                UPDATE platform_products
                SET current_price = :new_price, updated_at = NOW()
                WHERE sku = :sku AND current_price = :old_price
                """
            ),
            {"sku": sku, "old_price": old_price, "new_price": new_price},
        )
        if res.rowcount != 1:
            # stale or mismatch, reject
            conn.execute(
                text(
                    """
                    UPDATE price_adjustments
                    SET status = 'rejected', notes = COALESCE(notes,'') || ' stale_or_mismatch', applied_at = NOW()
                    WHERE id = :pid
                    """
                ),
                {"pid": int(proposal["id"])},
            )
            return "rejected_stale"
    else:
        # No old_price check; unconditional update
        conn.execute(
            text(
                """
                UPDATE platform_products
                SET current_price = :new_price, updated_at = NOW()
                WHERE sku = :sku
                """
            ),
            {"sku": sku, "new_price": new_price},
        )

    # 2) Insert history
    conn.execute(
        text(
            """
            INSERT INTO platform_product_price_history
                (sku, old_price, adjusted_price, change_type, changed_by, notes)
            VALUES
                (:sku, :old_price, :new_price, 'proposal_apply', :by, :reason)
            """
        ),
        {
            "sku": sku,
            "old_price": old_price,
            "new_price": new_price,
            "by": os.getenv("APPLIED_BY", "price-applier"),
            "reason": reason_code,
        },
    )

    # 3) Mark proposal applied
    conn.execute(
        text(
            """
            UPDATE price_adjustments
            SET status = 'applied', applied_at = NOW()
            WHERE id = :pid
            """
        ),
        {"pid": int(proposal["id"])},
    )
    return "applied"


def main():
    auto_apply_pending = os.getenv("AUTO_APPLY_PENDING", "true") == "true"
    limit = int(os.getenv("APPLY_LIMIT", "200"))
    engine = get_engine()
    df = fetch_candidates(auto_apply_pending, limit)
    if df.empty:
        print(json.dumps({"applied": 0, "rejected": 0, "note": "no proposals"}))
        return

    applied = 0
    rejected = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            outcome = apply_one(conn, row.to_dict())
            if outcome == "applied":
                applied += 1
            elif outcome.startswith("rejected"):
                rejected += 1

    print(json.dumps({"applied": applied, "rejected": rejected}))


if __name__ == "__main__":
    main()


