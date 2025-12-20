#!/usr/bin/env python3
"""
Market Intelligence Tool - Development Server

Run this script to start the local development server.

Usage:
    python run.py

Or with uvicorn directly:
    uvicorn app.main:app --reload --port 8000
"""

import uvicorn
from pathlib import Path


def main():
    """Start the development server."""
    print("=" * 60)
    print("  Market Intelligence Tool")
    print("  ASU Learning Enterprise")
    print("=" * 60)
    print()
    print("Starting development server...")
    print("Open http://localhost:8000 in your browser")
    print()
    print("Press Ctrl+C to stop the server")
    print("-" * 60)

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[str(Path(__file__).parent / "app")],
    )


if __name__ == "__main__":
    main()
