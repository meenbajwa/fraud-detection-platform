"""
data_masking.py
───────────────
Applies PII masking rules before data reaches the analytics layer.

Rules:
  - account_number  → SHA-256 hashed token (irreversible)
  - user_name       → replaced with anonymous USER_<id_prefix>
  - location        → kept (non-identifying at city level)

This mirrors enterprise data governance and TD's DaaS privacy standards.
"""

import hashlib
import pandas as pd


def mask_account_number(account: str) -> str:
    """
    One-way SHA-256 hash of account number.
    Retains referential integrity (same input → same hash)
    without exposing the real account number.
    """
    return "ACC_" + hashlib.sha256(account.encode()).hexdigest()[:12].upper()


def mask_user_name(transaction_id: str) -> str:
    """
    Replace real name with an anonymous user token derived
    from the transaction ID prefix — non-reversible.
    """
    return "USER_" + transaction_id[:8].upper()


def apply_masking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all PII masking rules to a transactions DataFrame.

    Args:
        df: Raw transactions DataFrame

    Returns:
        DataFrame with PII fields masked
    """
    df = df.copy()

    df["account_number"] = df["account_number"].apply(mask_account_number)
    df["user_name"]      = df["transaction_id"].apply(mask_user_name)

    return df


def validate_masking(original: pd.DataFrame, masked: pd.DataFrame) -> bool:
    """
    Verify masking was applied correctly — no raw PII leaks through.
    """
    assert not any(masked["account_number"] == original["account_number"]), \
        "❌ Account numbers not masked!"
    assert not any(masked["user_name"] == original["user_name"]), \
        "❌ User names not masked!"
    print("✅ Masking validation passed — no PII in analytics layer.")
    return True


# ── Demo ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from generate_transactions import generate_transactions

    raw = generate_transactions(10)
    masked = apply_masking(raw)

    print("\n🔐 Before masking:")
    print(raw[["transaction_id", "user_name", "account_number"]].head(3))

    print("\n✅ After masking:")
    print(masked[["transaction_id", "user_name", "account_number"]].head(3))

    validate_masking(raw, masked)
