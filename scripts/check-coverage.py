#!/usr/bin/env python3
"""
Coverage checker script for vSphere CSI Driver.
Analyzes coverage excluding 0% functions and enforces quality threshold.
"""

import re
import sys
import os


def main():
    if len(sys.argv) < 3:
        print("Usage: check-coverage.py <coverage-summary-file> <threshold>")
        sys.exit(1)
    
    coverage_file = sys.argv[1]
    threshold = float(sys.argv[2])
    
    if not os.path.exists(coverage_file):
        print(f"ERROR: Coverage file {coverage_file} not found")
        sys.exit(1)
    
    # Read the coverage summary file
    with open(coverage_file, 'r') as f:
        content = f.read()
    
    # Extract all coverage percentages
    lines = content.strip().split('\n')
    coverage_data = []
    
    for line in lines:
        match = re.search(r'(\d+\.\d+)%$', line)
        if match:
            percentage = float(match.group(1))
            coverage_data.append(percentage)
    
    # Calculate statistics
    total_functions = len(coverage_data)
    non_zero_functions = [p for p in coverage_data if p > 0.0]
    zero_functions = len([p for p in coverage_data if p == 0.0])
    total_non_zero = len(non_zero_functions)
    
    # Calculate averages
    overall_avg = sum(coverage_data) / total_functions if total_functions > 0 else 0
    non_zero_avg = sum(non_zero_functions) / total_non_zero if total_non_zero > 0 else 0
    
    # Print statistics
    print(f'Total functions: {total_functions}')
    print(f'Functions with 0% coverage: {zero_functions} ({zero_functions/total_functions*100:.1f}%)')
    print(f'Functions with >0% coverage: {total_non_zero} ({total_non_zero/total_functions*100:.1f}%)')
    print(f'Overall coverage (including 0%): {overall_avg:.1f}%')
    print(f'Quality coverage (excluding 0%): {non_zero_avg:.1f}%')
    
    # Check threshold
    if non_zero_avg < threshold:
        print(f'ERROR: Quality coverage {non_zero_avg:.1f}% is below threshold {threshold}%')
        print(f'The functions that have tests are not well-tested enough.')
        print(f'Please improve test quality for existing tested functions.')
        sys.exit(1)
    else:
        print(f'SUCCESS: Quality coverage {non_zero_avg:.1f}% meets threshold {threshold}%')
        if zero_functions > 0:
            print(f'NOTE: Consider adding tests for {zero_functions} untested functions to improve overall coverage.')


if __name__ == '__main__':
    main()
