"""
QA test for generate_transaction().
Runs without Kafka — just imports the pure data-generation logic.
"""

from producer import generate_transaction


def validate_transaction(txn):
    errors = []

    # ── Required fields ────────────────────────────────────────────────────────
    required_fields = [
        "transaction_id", "event_time", "store_id", "country",
        "product_name", "category", "unit_price", "quantity",
        "total_amount", "payment_method",
    ]
    for field in required_fields:
        if field not in txn:
            errors.append(f"Missing field: {field}")

    # ── Price / quantity checks ────────────────────────────────────────────────
    if txn.get("unit_price") == 0:
        errors.append("Price is exactly zero (possible bug)")

    if txn.get("quantity", 0) <= 0:
        errors.append("Invalid quantity (<= 0)")

    # ── Total amount consistency ───────────────────────────────────────────────
    expected = round(txn["unit_price"] * txn["quantity"], 2)
    if txn["total_amount"] != expected:
        errors.append(
            f"Total mismatch: {txn['unit_price']} x {txn['quantity']} "
            f"= {expected}, got {txn['total_amount']}"
        )

    # ── Fraud sanity: flagged txns should have an extreme price ───────────────
    # Spike fraud = price > 8x the product's max normal price in local currency.
    # Max normal local price ≈ $1599 USD * highest FX rate (JPY 149.5) ≈ 239,000 JPY.
    # 8x spike on cheapest product ($45 MagSafe * 0.79 GBP * 8) ≈ 284 GBP minimum spike.
    # So: price < 1.0  →  zero-fraud
    #     price < 0    →  negative-fraud
    #     price > 284  AND price is flagged could be spike, but we can't know the
    #     product's normal price here without more context.
    # Simplest reliable check: at least one of the three fraud patterns must apply.
    if txn.get("is_fraud_flag"):
        price = txn["unit_price"]
        is_near_zero  = 0 < price < 1.0
        is_negative   = price < 0
        # Spike: price must exceed 8x the most expensive product's max local price.
        # Most expensive = $2499 * highest multiplier (JPY 149.5) = ~373,700 JPY.
        # Lowest spike threshold = $45 (cheapest) * 0.79 (GBP) * 8 = ~284.
        # A flagged transaction with a "normal-looking" price (e.g. £345) means
        # it was generated as spike but happened to land in a plausible range —
        # that's a gap in compute_price(), not the test's fault.
        is_spike = price > 284
        if not (is_near_zero or is_negative or is_spike):
            errors.append(
                f"Fraud flag set but price ({price:.2f}) does not match any "
                "fraud pattern (near-zero, negative, or spike > 284)"
            )

    return errors


def run_test(n=20):
    print(f"\nRunning QA test on {n} generated transactions...\n")
    print(f"{'#':<5} {'Product':<35} {'Country':<6} {'Price':>10}  {'Status'}")
    print("─" * 75)

    fraud_count  = 0
    error_count  = 0
    passed_count = 0

    for i in range(n):
        txn    = generate_transaction()
        errors = validate_transaction(txn)

        if txn["is_fraud_flag"]:
            fraud_count += 1

        if errors:
            error_count += 1
            status = "❌ FAIL"
        else:
            passed_count += 1
            status = "✅ OK"

        fraud_tag = "  ⚠ FRAUD" if txn["is_fraud_flag"] else ""
        print(
            f"{i+1:<5} {txn['product_name']:<35} {txn['country']:<6} "
            f"{txn['currency']} {txn['unit_price']:>8.2f}  {status}{fraud_tag}"
        )

        if errors:
            for err in errors:
                print(f"       → {err}")

    print("─" * 75)
    print(f"\nResults:  {passed_count} passed  |  {error_count} failed  |  {fraud_count} fraud transactions")
    print("QA test complete.\n")


if __name__ == "__main__":
    run_test(50)