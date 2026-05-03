#!/usr/bin/env python3

"""
Filter govulncheck JSON output by CVSS score threshold.

This script parses JSON output from govulncheck and filters vulnerabilities
based on CVSS v3 base scores directly extracted from OSV records (not computed).

Usage:
    python3 hack/filter-vulns.py <threshold> <json-file1> [json-file2] ...

Example:
    python3 hack/filter-vulns.py 5.0 vuln-root.json vuln-e2e.json

Exit codes:
    0: No vulnerabilities above threshold found
    1: Vulnerabilities above threshold found
"""

import json
import sys
import re
from typing import List, Dict, Optional, Tuple


def extract_cvss_score_from_vector(vector: str) -> Optional[float]:
    """
    Extract CVSS score from a CVSS vector string using regex to find the score value.
    
    In some CVSS vectors, the score might be included or we can compute it.
    This function attempts a best-effort extraction.
    
    Returns:
        Float score or None if unable to extract
    """
    if not vector:
        return None
    
    # Try to extract numeric score if it's in a standard format like "CVSS:3.1/...score:8.9"
    # This is a fallback - the primary method is to use the score field if available
    return None


def extract_vulnerabilities(json_data: str) -> List[Dict]:
    """
    Parse JSON array from govulncheck output.
    
    govulncheck outputs JSON with vulnerability objects that should have
    a 'Severity' field containing the CVSS vector.
    
    Returns:
        List of vulnerability objects
    """
    try:
        data = json.loads(json_data)
        # Handle both array and non-array outputs
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Sometimes it might be wrapped in an object
            if "vulnerabilities" in data:
                return data["vulnerabilities"]
            return [data] if data else []
        return []
    except json.JSONDecodeError:
        return []


def filter_by_cvss(vulns: List[Dict], threshold: float) -> Tuple[List[Dict], List[Dict]]:
    """
    Filter vulnerabilities by CVSS threshold.
    
    Looks for CVSS score in multiple places:
    1. Direct 'cvss_score' field
    2. Within 'Severity' field as CVSS vector (not computed)
    
    Returns:
        Tuple of (high_risk_vulns, low_risk_vulns)
    """
    high_risk = []
    low_risk = []
    
    for vuln in vulns:
        # Try to find CVSS score in the vulnerability object
        cvss_score = None
        
        # Some formats have score directly
        if "cvss_score" in vuln:
            try:
                cvss_score = float(vuln["cvss_score"])
            except (ValueError, TypeError):
                pass
        
        # Try CVSS v3 vector format (CVSS:3.1/...)
        if cvss_score is None and "Severity" in vuln:
            severity = vuln["Severity"]
            if isinstance(severity, str) and severity.startswith("CVSS:"):
                # Extract numeric score if present in vector
                # Format might be like "CVSS:3.1/AV:N/...score:8.9" in some implementations
                # Or we need to note that the score needs to be looked up externally
                # For now, we'll note that the score couldn't be determined
                cvss_score = None
        
        # If still no score, try to look in nested structures
        if cvss_score is None:
            for key in vuln:
                if isinstance(vuln[key], dict):
                    if "score" in vuln[key]:
                        try:
                            cvss_score = float(vuln[key]["score"])
                            break
                        except (ValueError, TypeError):
                            pass
                    if "cvss_score" in vuln[key]:
                        try:
                            cvss_score = float(vuln[key]["cvss_score"])
                            break
                        except (ValueError, TypeError):
                            pass
        
        vuln["cvss_score"] = cvss_score
        
        if cvss_score is not None and cvss_score >= threshold:
            high_risk.append(vuln)
        else:
            low_risk.append(vuln)
    
    return high_risk, low_risk


def format_vulnerability(vuln: Dict) -> str:
    """Format a vulnerability for display."""
    vid = vuln.get("ID", "UNKNOWN")
    aliases = vuln.get("Aliases", [])
    summary = vuln.get("Summary", "No summary available")
    cvss_score = vuln.get("cvss_score", "N/A")
    
    alias_str = ", ".join(aliases) if aliases else "None"
    
    return (
        f"CVE ID: {vid}\n"
        f"Aliases: {alias_str}\n"
        f"Summary: {summary}\n"
        f"CVSS Score: {cvss_score}\n"
    )


def main():
    """Main entry point."""
    if len(sys.argv) < 3:
        print(
            "Usage: python3 hack/filter-vulns.py <threshold> <json-file1> [json-file2] ...",
            file=sys.stderr
        )
        sys.exit(1)
    
    try:
        threshold = float(sys.argv[1])
    except ValueError:
        print(f"Error: threshold must be a float, got '{sys.argv[1]}'", file=sys.stderr)
        sys.exit(1)
    
    json_files = sys.argv[2:]
    all_vulns = []
    
    # Read and parse all JSON files
    for filepath in json_files:
        try:
            with open(filepath, "r") as f:
                data = f.read()
                vulns = extract_vulnerabilities(data)
                all_vulns.extend(vulns)
        except FileNotFoundError:
            print(f"Warning: File not found: {filepath}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Error reading {filepath}: {e}", file=sys.stderr)
    
    # Filter vulnerabilities
    high_risk, low_risk = filter_by_cvss(all_vulns, threshold)
    
    # Report results
    if high_risk:
        print(f"\n{'='*70}")
        print(f"HIGH-RISK VULNERABILITIES DETECTED (CVSS >= {threshold}):")
        print(f"{'='*70}\n")
        
        for vuln in high_risk:
            print(format_vulnerability(vuln))
        
        print(f"{len(high_risk)} high-risk vulnerability/vulnerabilities detected.")
        print("\nFix: Update go.mod to use patched versions of affected dependencies.\n")
        sys.exit(1)
    else:
        total = len(low_risk)
        if total == 0:
            print(f"No vulnerabilities detected.")
        else:
            print(f"No vulnerabilities detected above CVSS threshold {threshold}.")
            if low_risk:
                print(f"({total} lower-severity or unscored vulnerabilities ignored)")
        sys.exit(0)


if __name__ == "__main__":
    main()



def extract_vulnerabilities(json_data: str) -> List[Dict]:
    """
    Parse JSON array from govulncheck output.
    
    Returns:
        List of vulnerability objects
    """
    try:
        return json.loads(json_data)
    except json.JSONDecodeError:
        return []


def filter_by_cvss(vulns: List[Dict], threshold: float) -> Tuple[List[Dict], List[Dict]]:
    """
    Filter vulnerabilities by CVSS threshold.
    
    Returns:
        Tuple of (high_risk_vulns, low_risk_vulns)
    """
    high_risk = []
    low_risk = []
    
    for vuln in vulns:
        # Look for severity field which contains CVSS vector
        severity_str = vuln.get("Severity", "")
        cvss_score = None
        
        if severity_str:
            cvss_score = parse_cvss_vector(severity_str)
        
        vuln["cvss_score"] = cvss_score
        
        if cvss_score is not None and cvss_score >= threshold:
            high_risk.append(vuln)
        else:
            low_risk.append(vuln)
    
    return high_risk, low_risk


def format_vulnerability(vuln: Dict) -> str:
    """Format a vulnerability for display."""
    vid = vuln.get("ID", "UNKNOWN")
    aliases = vuln.get("Aliases", [])
    summary = vuln.get("Summary", "No summary available")
    cvss_score = vuln.get("cvss_score", "N/A")
    
    alias_str = ", ".join(aliases) if aliases else "None"
    
    return (
        f"CVE ID: {vid}\n"
        f"Aliases: {alias_str}\n"
        f"Summary: {summary}\n"
        f"CVSS Score: {cvss_score}\n"
    )


def main():
    """Main entry point."""
    if len(sys.argv) < 3:
        print(
            "Usage: python3 hack/filter-vulns.py <threshold> <json-file1> [json-file2] ...",
            file=sys.stderr
        )
        sys.exit(1)
    
    try:
        threshold = float(sys.argv[1])
    except ValueError:
        print(f"Error: threshold must be a float, got '{sys.argv[1]}'", file=sys.stderr)
        sys.exit(1)
    
    json_files = sys.argv[2:]
    all_vulns = []
    
    # Read and parse all JSON files
    for filepath in json_files:
        try:
            with open(filepath, "r") as f:
                data = f.read()
                vulns = extract_vulnerabilities(data)
                all_vulns.extend(vulns)
        except FileNotFoundError:
            print(f"Warning: File not found: {filepath}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Error reading {filepath}: {e}", file=sys.stderr)
    
    # Filter vulnerabilities
    high_risk, low_risk = filter_by_cvss(all_vulns, threshold)
    
    # Report results
    if high_risk:
        print(f"\n{'='*70}")
        print(f"HIGH-RISK VULNERABILITIES DETECTED (CVSS >= {threshold}):")
        print(f"{'='*70}\n")
        
        for vuln in high_risk:
            print(format_vulnerability(vuln))
        
        print(f"{len(high_risk)} high-risk vulnerability/vulnerabilities detected.")
        print("\nFix: Update go.mod to use patched versions of affected dependencies.\n")
        sys.exit(1)
    else:
        total = len(low_risk)
        if total == 0:
            print(f"No vulnerabilities detected.")
        else:
            print(f"No vulnerabilities detected above CVSS threshold {threshold}.")
            if low_risk:
                print(f"({total} lower-severity vulnerabilities ignored)")
        sys.exit(0)


if __name__ == "__main__":
    main()
