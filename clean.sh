#!/bin/bash -

echo "========================================"
echo "Cleaning build artifacts..."
echo "========================================"

if [ -d "dist" ]; then
    echo "Removing dist directory..."
    rm -rf dist
    echo "Cleaned: dist directory removed"
else
    echo "Nothing to clean: dist directory does not exist"
fi

echo "========================================"
echo "Clean completed!"
echo "========================================" 