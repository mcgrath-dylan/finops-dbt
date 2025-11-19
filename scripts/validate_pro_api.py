#!/usr/bin/env python3
"""Validate that paths listed in a PRO_PUBLIC_API.yml exist in the Pro repo.

Usage: python scripts/validate_pro_api.py --pro-root ../snowflake-finops-pro
"""
import argparse
import sys
from pathlib import Path
import yaml


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--pro-root', default='../snowflake-finops-pro')
    args = p.parse_args()

    root = Path(args.pro_root)
    manifest = root / 'PRO_PUBLIC_API.yml'
    if not manifest.exists():
        print(f'PRO_PUBLIC_API.yml not found at {manifest} — skipping validation')
        return 0

    data = yaml.safe_load(manifest.read_text())
    missing = []
    for section in ('models', 'macros'):
        for entry in data.get(section, []) or []:
            f = root / entry
            if not f.exists():
                missing.append(str(entry))

    if missing:
        print('ERROR: The following API entries are missing in the Pro repo:')
        for m in missing:
            print(f' - {m}')
        return 2

    print('OK: All PRO_PUBLIC_API.yml entries exist in the Pro repo.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
