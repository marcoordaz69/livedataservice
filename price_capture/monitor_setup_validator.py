#!/usr/bin/env python3
"""
Monitor the trade setup validation process from simple_ohlcv.py
This script follows the log file and highlights entry and exit triggers.
"""

import os
import time
import re
import sys
from datetime import datetime
import argparse
from colorama import init, Fore, Style

# Initialize colorama for colored terminal output
init()

def monitor_log_file(log_file="simple_ohlcv.log", follow=True):
    """Monitor the log file for trade setup validation events"""
    
    # Get the absolute path to the log file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file_path = os.path.join(script_dir, log_file) if not os.path.isabs(log_file) else log_file
    
    print(f"{Fore.YELLOW}Looking for log file at: {log_file_path}{Style.RESET_ALL}")
    
    if not os.path.exists(log_file_path):
        print(f"{Fore.RED}Log file {log_file_path} does not exist.{Style.RESET_ALL}")
        print(f"Waiting for log file to be created...")
        print(f"Current working directory: {os.getcwd()}")
        
        # Wait for log file to be created with a timeout
        start_time = time.time()
        wait_time = 10  # Wait up to 10 seconds
        
        while not os.path.exists(log_file_path):
            if time.time() - start_time > wait_time:
                print(f"{Fore.RED}Warning: Log file {log_file_path} not found after waiting {wait_time}s. Monitor may not work correctly.{Style.RESET_ALL}")
                # Try alternate location
                alternate_path = os.path.join(os.getcwd(), log_file)
                print(f"{Fore.YELLOW}Checking alternate path: {alternate_path}{Style.RESET_ALL}")
                if os.path.exists(alternate_path):
                    print(f"{Fore.GREEN}Found log file at alternate location: {alternate_path}{Style.RESET_ALL}")
                    log_file_path = alternate_path
                    break
                else:
                    print(f"{Fore.RED}Alternate path not found either. Continuing to monitor original path...{Style.RESET_ALL}")
                break
            time.sleep(1)
    
    print(f"{Fore.GREEN}Monitoring trade setup validator log file: {log_file_path}{Style.RESET_ALL}")
    print("Press Ctrl+C to exit")
    print("-" * 100)
    
    # Regex patterns to match important lines
    setup_trigger_pattern = re.compile(r'SETUP TRIGGERED')
    entry_pattern = re.compile(r'Entry triggered at')
    entry_zone_pattern = re.compile(r'Zone:')
    exit_pattern = re.compile(r'(Take profit|Stop loss) hit at')
    price_check_pattern = re.compile(r'Checking trade setups against price')
    price_debug_pattern = re.compile(r'(entry_zone_low|entry_zone_high|entry_low|entry_high)')
    
    # Trade completion patterns
    trade_profit_pattern = re.compile(r'TRADE COMPLETED: PROFIT')
    trade_loss_pattern = re.compile(r'TRADE COMPLETED: LOSS')
    trade_manual_pattern = re.compile(r'TRADE COMPLETED: MANUAL EXIT')
    
    # Enhanced patterns for setup counts
    setup_count_pattern = re.compile(r'tracking (\d+) setups for NQ\.FUT \(Open: (\d+), Active: (\d+)\)')
    loaded_setups_pattern = re.compile(r'Loaded (\d+) trade setups for NQ\.FUT \(Open: (\d+), Active: (\d+)\)')
    
    # Open the file and seek to the end
    with open(log_file_path, 'r') as f:
        # If not following, start from the beginning
        if not follow:
            current_position = 0
        else:
            # Start at the end of the file
            f.seek(0, 2)
            current_position = f.tell()
            
        while True:
            f.seek(current_position)
            new_lines = f.readlines()
            
            if new_lines:
                for line in new_lines:
                    # Extract timestamp if present
                    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
                    timestamp = timestamp_match.group(1) if timestamp_match else ""
                    
                    # Check for setup counts (enhanced version)
                    setup_counts_match = setup_count_pattern.search(line)
                    loaded_counts_match = loaded_setups_pattern.search(line)
                    
                    # Highlight important lines with different colors
                    if setup_counts_match:
                        total, open_count, active_count = setup_counts_match.groups()
                        print(f"{Fore.CYAN}{timestamp} {Fore.MAGENTA}{Style.BRIGHT}")
                        print(f"╔═══════════════════════════════╗")
                        print(f"║ TRADE SETUPS STATUS           ║")
                        print(f"║ Total: {total.ljust(23)}║")
                        print(f"║ Open: {open_count.ljust(24)}║")
                        print(f"║ Active: {active_count.ljust(22)}║")
                        print(f"╚═══════════════════════════════╝{Style.RESET_ALL}")
                    elif loaded_counts_match:
                        total, open_count, active_count = loaded_counts_match.groups()
                        print(f"{Fore.CYAN}{timestamp} {Fore.MAGENTA}{Style.BRIGHT}")
                        print(f"╔═══════════════════════════════╗")
                        print(f"║ LOADED TRADE SETUPS           ║")
                        print(f"║ Total: {total.ljust(23)}║")
                        print(f"║ Open: {open_count.ljust(24)}║")
                        print(f"║ Active: {active_count.ljust(22)}║")
                        print(f"╚═══════════════════════════════╝{Style.RESET_ALL}")
                    elif trade_profit_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.GREEN}{Style.BRIGHT}")
                        print(f"╔═══════════════════════════════╗")
                        print(f"║ TRADE COMPLETED: PROFIT! ✓    ║")
                        print(f"║ {line.strip()[-20:].ljust(27)}║")
                        print(f"╚═══════════════════════════════╝{Style.RESET_ALL}")
                    elif trade_loss_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.RED}{Style.BRIGHT}")
                        print(f"╔═══════════════════════════════╗")
                        print(f"║ TRADE COMPLETED: LOSS! ✗      ║")
                        print(f"║ {line.strip()[-20:].ljust(27)}║")
                        print(f"╚═══════════════════════════════╝{Style.RESET_ALL}")
                    elif trade_manual_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.YELLOW}{Style.BRIGHT}")
                        print(f"╔═══════════════════════════════╗")
                        print(f"║ TRADE COMPLETED: MANUAL EXIT  ║")
                        print(f"║ {line.strip()[-20:].ljust(27)}║")
                        print(f"╚═══════════════════════════════╝{Style.RESET_ALL}")
                    elif setup_trigger_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.YELLOW}{Style.BRIGHT}>>> {line.strip()}{Style.RESET_ALL}")
                    elif entry_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.GREEN}{Style.BRIGHT}>>> ENTRY: {line.strip()}{Style.RESET_ALL}")
                    elif entry_zone_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.GREEN}    {line.strip()}{Style.RESET_ALL}")
                    elif exit_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.RED}{Style.BRIGHT}>>> EXIT: {line.strip()}{Style.RESET_ALL}")
                    elif price_check_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.BLUE}{line.strip()}{Style.RESET_ALL}")
                    elif price_debug_pattern.search(line):
                        print(f"{Fore.CYAN}{timestamp} {Fore.CYAN}{Style.BRIGHT}{line.strip()}{Style.RESET_ALL}")
                    else:
                        print(f"{Fore.CYAN}{timestamp} {Style.DIM}{line.strip()}{Style.RESET_ALL}")
                
                current_position = f.tell()
            
            # If not following, exit after reading all lines
            if not follow:
                break
                
            time.sleep(0.1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Monitor the trade setup validation log file')
    parser.add_argument('--log-file', type=str, default='simple_ohlcv.log',
                        help='Path to the log file (default: simple_ohlcv.log)')
    parser.add_argument('--no-follow', action='store_true',
                        help='Do not follow the log file, just display existing entries and exit')
    args = parser.parse_args()
    
    try:
        monitor_log_file(log_file=args.log_file, follow=not args.no_follow)
    except KeyboardInterrupt:
        print(f"\n{Fore.GREEN}Monitor stopped.{Style.RESET_ALL}")
        sys.exit(0) 