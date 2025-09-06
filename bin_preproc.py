#!/usr/bin/env python3
"""
bin_preproc.py - Binance CSV Export to DaLI Manual CSV Converter

This script converts Binance CSV export files to the DaLI manual CSV format.
It processes the input file and creates three output files:
- in_csv_file.csv: Contains crypto acquisitions (buy, deposit, etc.)
- out_csv_file.csv: Contains crypto disposals (sell, withdraw, etc.)
- intra_csv_file.csv: Contains crypto transfers between accounts

Usage:
    python bin_preproc.py input_file.csv [--out-dir OUTPUT_DIRECTORY]

Example:
    python bin_preproc.py input/binance_dummy_prepared.csv

"""

import argparse
import csv
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime


class BinanceTransaction:
    """Represents a processed Binance transaction"""
    
    def __init__(self, timestamp, operation_type, asset=None, amount=None):
        self.timestamp = timestamp
        self.operation_type = operation_type
        self.asset = asset
        self.amount = amount
        self.related_transactions = []
    
    def add_related(self, transaction):
        """Add a related transaction (e.g. fee, buy/sell pair)"""
        self.related_transactions.append(transaction)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert Binance CSV export to DaLI Manual CSV format"
    )
    parser.add_argument(
        "input_file", help="Path to the Binance CSV export file"
    )
    parser.add_argument(
        "--out-dir", 
        dest="output_directory",
        default=".",
        help="Directory to store output files (default: current directory)"
    )
    parser.add_argument(
        "--holder", 
        default="User",
        help="Account holder name (default: 'User')"
    )
    parser.add_argument(
        "--exchange", 
        default="Binance",
        help="Exchange name (default: 'Binance')"
    )

    return parser.parse_args()


def read_binance_csv(file_path):
    """Read Binance CSV export file and return a list of records."""
    records = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Skip empty rows
            if not row["UTC_Time"].strip():
                continue
            records.append(row)
    
    return records


def group_transactions_by_time(records):
    """Group transactions by timestamp."""
    grouped_transactions = defaultdict(list)
    for record in records:
        # Parse timestamp
        timestamp = record["UTC_Time"]
        grouped_transactions[timestamp].append(record)
    
    return grouped_transactions


def process_transaction_group(group, holder, exchange):
    """Process a group of transactions with the same timestamp."""
    timestamp = group[0]["UTC_Time"]
    # Convert timestamp to ISO8601 format with timezone
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    in_transactions = []
    out_transactions = []
    intra_transactions = []
    
    # Determine the transaction type
    operation_types = {rec["Operation"] for rec in group}
    
    # Case: Binance Convert
    # Handle these first as they need to be processed individually, not as a group
    if "Binance Convert" in operation_types:
        convert_recs = [rec for rec in group if rec["Operation"] == "Binance Convert"]
        for rec in convert_recs:
            asset = rec["Coin"]
            amount = float(rec["Change"])
            
            # Determine if this is a buy or sell based on the sign of the amount
            if amount > 0:  # Positive = buy (IN)
                # Create IN transaction
                unique_id = str(uuid.uuid4().hex)
                in_transactions.append({
                    "unique_id": unique_id,
                    "timestamp": iso_timestamp,
                    "asset": asset,
                    "exchange": exchange,
                    "holder": holder,
                    "transaction_type": "BUY",
                    "spot_price": "__unknown",
                    "crypto_in": str(amount),
                    "crypto_fee": "0",  # Fees are not specified in Convert operations
                    "fiat_in_no_fee": "",
                    "fiat_in_with_fee": "",
                    "fiat_fee": "",
                    "notes": f"Converted to {amount} {asset}"
                })
            else:  # Negative = sell (OUT)
                # Create OUT transaction
                unique_id = str(uuid.uuid4().hex)
                abs_amount = abs(amount)
                out_transactions.append({
                    "unique_id": unique_id,
                    "timestamp": iso_timestamp,
                    "asset": asset,
                    "exchange": exchange,
                    "holder": holder,
                    "transaction_type": "SELL",
                    "spot_price": "__unknown",
                    "crypto_out_no_fee": str(abs_amount),
                    "crypto_fee": "0",  # Fees are not specified in Convert operations
                    "crypto_out_with_fee": str(abs_amount),
                    "fiat_out_no_fee": "",
                    "fiat_fee": "",
                    "fiat_ticker": "",
                    "notes": f"Converted {abs_amount} {asset}"
                })
    
    # Case: Deposit - categorize as intra transaction
    if "Deposit" in operation_types:
        deposit_recs = [rec for rec in group if rec["Operation"] == "Deposit"]
        for rec in deposit_recs:
            asset = rec["Coin"]
            amount = float(rec["Change"])
            if amount <= 0:
                continue
                
            # Create INTRA transaction for deposit
            unique_id = str(uuid.uuid4().hex)
            intra_transactions.append({
                "unique_id": unique_id,
                "timestamp": iso_timestamp,
                "asset": asset,
                "from_exchange": "External",  # Deposit comes from an external source
                "from_holder": holder,
                "to_exchange": exchange,
                "to_holder": holder,
                "spot_price": "__unknown",
                "crypto_sent": str(amount),
                "crypto_received": str(amount),
                "notes": f"Deposit of {asset}" + (" (Mining)" if "Mining" in rec.get("Remark", "") else "")
            })
    
    # Case: Withdraw
    if "Withdraw" in operation_types:
        withdraw_recs = [rec for rec in group if rec["Operation"] == "Withdraw"]
        for rec in withdraw_recs:
            asset = rec["Coin"]
            amount = abs(float(rec["Change"]))  # Convert to positive for output
            
            # Create OUT transaction for withdrawal
            out_transactions.append({
                "unique_id": str(uuid.uuid4().hex),
                "timestamp": iso_timestamp,
                "asset": asset,
                "exchange": exchange,
                "holder": holder,
                "transaction_type": "SELL",  # Assuming withdrawal is SELL type
                "spot_price": "__unknown",
                "crypto_out_no_fee": str(amount),
                "crypto_fee": "0",  # Binance includes fees in the withdrawal amount
                "crypto_out_with_fee": str(amount),
                "fiat_out_no_fee": "",
                "fiat_fee": "",
                "fiat_ticker": "USD",
                "notes": rec.get("Remark", "Withdrawal")
            })
    
    # Case: Transaction Buy/Sell
    if "Transaction Buy" in operation_types or "Transaction Sold" in operation_types:
        # Check for buy transaction
        if "Transaction Buy" in operation_types:
            # This is a buy transaction
            buy_recs = [rec for rec in group if rec["Operation"] == "Transaction Buy"]
            spend_recs = [rec for rec in group if rec["Operation"] == "Transaction Spend"]
            fee_recs = [rec for rec in group if rec["Operation"] == "Transaction Fee" and float(rec["Change"]) < 0]
            
            # Get bought asset info
            bought_asset = None
            bought_amount = 0
            fee_amount = 0
            
            for rec in buy_recs:
                if not bought_asset:
                    bought_asset = rec["Coin"]
                if rec["Coin"] == bought_asset:
                    bought_amount += float(rec["Change"])
            
            # Calculate fee
            for rec in fee_recs:
                if rec["Coin"] == bought_asset:
                    fee_amount += abs(float(rec["Change"]))
            
            # Calculate total spent in USDT
            spent_amount = 0
            for rec in spend_recs:
                spent_amount += abs(float(rec["Change"]))
            
            # Adjusted bought amount (after fees)
            adjusted_bought = bought_amount - fee_amount
            
            # Create IN transaction for buy
            in_transactions.append({
                "unique_id": str(uuid.uuid4().hex),
                "timestamp": iso_timestamp,
                "asset": bought_asset,
                "exchange": exchange,
                "holder": holder,
                "transaction_type": "BUY",
                "spot_price": "__unknown",
                "crypto_in": str(adjusted_bought),
                "crypto_fee": str(fee_amount),
                "fiat_in_no_fee": str(spent_amount),
                "fiat_in_with_fee": str(spent_amount),
                "fiat_fee": "",
                "fiat_ticker": "USDT",
                "notes": f"Bought {adjusted_bought} {bought_asset} for {spent_amount} USDT"
            })
        
        # Check for sell transaction
        if "Transaction Sold" in operation_types:
            # This is a sell transaction
            sold_recs = [rec for rec in group if rec["Operation"] == "Transaction Sold"]
            revenue_recs = [rec for rec in group if rec["Operation"] == "Transaction Revenue"]
            fee_recs = [rec for rec in group if rec["Operation"] == "Transaction Fee" and float(rec["Change"]) < 0]
            
            # Get sold asset info
            sold_asset = None
            sold_amount = 0
            
            for rec in sold_recs:
                if not sold_asset:
                    sold_asset = rec["Coin"]
                if rec["Coin"] == sold_asset:
                    sold_amount += abs(float(rec["Change"]))
            
            # Calculate revenue in USDT
            revenue_amount = 0
            revenue_asset = "USDT"
            fee_amount = 0
            
            for rec in revenue_recs:
                revenue_amount += float(rec["Change"])
                revenue_asset = rec["Coin"]
            
            # Calculate fee in USDT
            for rec in fee_recs:
                if rec["Coin"] == revenue_asset:
                    fee_amount += abs(float(rec["Change"]))
            
            # Adjusted revenue amount (after fees)
            adjusted_revenue = revenue_amount - fee_amount
            
            # Create OUT transaction for sell
            out_transactions.append({
                "unique_id": str(uuid.uuid4().hex),
                "timestamp": iso_timestamp,
                "asset": sold_asset,
                "exchange": exchange,
                "holder": holder,
                "transaction_type": "SELL",
                "spot_price": "__unknown",
                "crypto_out_no_fee": str(sold_amount),
                "crypto_fee": "0",
                "crypto_out_with_fee": str(sold_amount),
                "fiat_out_no_fee": str(adjusted_revenue),
                "fiat_fee": str(fee_amount),
                "fiat_ticker": revenue_asset,
                "notes": f"Sold {sold_amount} {sold_asset} for {adjusted_revenue} {revenue_asset}"
            })
    
    # Note: Token Swap/Rebranding is now handled separately before grouping by timestamp
    
    return in_transactions, out_transactions, intra_transactions


def write_output_files(in_transactions, out_transactions, intra_transactions, output_dir):
    """Write transactions to the appropriate output files."""
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Write in_csv_file.csv
    with open(os.path.join(output_dir, "in_csv_file.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Unique ID", "Timestamp", "Asset", "Exchange", "Holder", "Transaction Type", 
            "Spot Price", "Crypto In", "Crypto Fee", "Fiat In No Fee", "Fiat In With Fee", 
            "Fiat Fee", "Notes"
        ])
        for tx in in_transactions:
            writer.writerow([
                tx["unique_id"], tx["timestamp"], tx["asset"], tx["exchange"], tx["holder"],
                tx["transaction_type"], tx["spot_price"], tx["crypto_in"], tx["crypto_fee"],
                tx["fiat_in_no_fee"], tx["fiat_in_with_fee"], tx["fiat_fee"], tx["notes"]
            ])
    
    # Write out_csv_file.csv
    with open(os.path.join(output_dir, "out_csv_file.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Unique ID", "Timestamp", "Asset", "Exchange", "Holder", "Transaction Type",
            "Spot Price", "Crypto Out No Fee", "Crypto Fee", "Crypto Out With Fee",
            "Fiat Out No Fee", "Fiat Fee", "Fiat Ticker", "Notes"
        ])
        for tx in out_transactions:
            writer.writerow([
                tx["unique_id"], tx["timestamp"], tx["asset"], tx["exchange"], tx["holder"],
                tx["transaction_type"], tx["spot_price"], tx["crypto_out_no_fee"], 
                tx["crypto_fee"], tx["crypto_out_with_fee"], tx["fiat_out_no_fee"],
                tx["fiat_fee"], tx["fiat_ticker"], tx["notes"]
            ])
    
    # Write intra_csv_file.csv
    with open(os.path.join(output_dir, "intra_csv_file.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Unique ID", "Timestamp", "Asset", "From Exchange", "From Holder", 
            "To Exchange", "To Holder", "Spot Price", "Crypto Sent", "Crypto Received", "Notes"
        ])
        for tx in intra_transactions:
            writer.writerow([
                tx["unique_id"], tx["timestamp"], tx["asset"], tx["from_exchange"],
                tx["from_holder"], tx["to_exchange"], tx["to_holder"], tx["spot_price"],
                tx["crypto_sent"], tx["crypto_received"], tx["notes"]
            ])


def process_rebranding_pairs(swap_records, distribution_records, holder, exchange):
    """Process token swap and distribution records to identify rebranding pairs."""
    intra_transactions = []
    
    # Match distribution records with swap records
    for dist_rec in distribution_records:
        remark = dist_rec.get("Remark", "")
        if " to " in remark:
            # Extract old and new asset names
            from_asset, to_asset = remark.split(" to ")
            # Try to find matching swap record
            for swap_rec in swap_records:
                if swap_rec["Coin"] == from_asset:
                    # Found a matching pair
                    amount = abs(float(swap_rec["Change"]))
                    # Convert timestamp to ISO8601
                    timestamp = swap_rec["UTC_Time"]
                    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Create an intra transaction for the rebranding
                    unique_id = str(uuid.uuid4().hex)
                    intra_transactions.append({
                        "unique_id": unique_id,
                        "timestamp": iso_timestamp,
                        "asset": from_asset,
                        "from_exchange": exchange,
                        "from_holder": holder,
                        "to_exchange": exchange,
                        "to_holder": holder,
                        "spot_price": "__unknown",
                        "crypto_sent": str(amount),
                        "crypto_received": str(amount),
                        "notes": f"Rebranding from {from_asset} to {to_asset}"
                    })
    
    return intra_transactions


def main():
    """Main function to process Binance CSV and output DaLI format."""
    args = parse_arguments()
    
    # Read input CSV file
    try:
        records = read_binance_csv(args.input_file)
        print(f"Read {len(records)} records from {args.input_file}")
    except Exception as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        return 1
    
    # Extract token swap and distribution records for special processing
    swap_records = [rec for rec in records if rec["Operation"] == "Token Swap - Redenomination/Rebranding"]
    distribution_records = [rec for rec in records if rec["Operation"] == "Distribution" and " to " in rec.get("Remark", "")]
    
    # Process rebranding pairs across timestamps
    rebranding_intra_txs = process_rebranding_pairs(swap_records, distribution_records, args.holder, args.exchange)
    
    # Filter out already processed records
    if swap_records or distribution_records:
        # Keep only records that aren't part of token swap/rebranding
        filtered_records = []
        for rec in records:
            if rec["Operation"] == "Token Swap - Redenomination/Rebranding":
                continue
            if rec["Operation"] == "Distribution" and " to " in rec.get("Remark", ""):
                continue
            filtered_records.append(rec)
        records = filtered_records
    
    # Group transactions by timestamp
    grouped_transactions = group_transactions_by_time(records)
    print(f"Grouped into {len(grouped_transactions)} transaction groups")
    
    all_in_transactions = []
    all_out_transactions = []
    all_intra_transactions = rebranding_intra_txs
    
    # Process each transaction group
    for timestamp, group in grouped_transactions.items():
        in_txs, out_txs, intra_txs = process_transaction_group(group, args.holder, args.exchange)
        all_in_transactions.extend(in_txs)
        all_out_transactions.extend(out_txs)
        all_intra_transactions.extend(intra_txs)
    
    # Write output files
    write_output_files(all_in_transactions, all_out_transactions, all_intra_transactions, args.output_directory)
    
    print(f"Generated output files in {args.output_directory}:")
    print(f"  - in_csv_file.csv: {len(all_in_transactions)} transactions")
    print(f"  - out_csv_file.csv: {len(all_out_transactions)} transactions")
    print(f"  - intra_csv_file.csv: {len(all_intra_transactions)} transactions")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
