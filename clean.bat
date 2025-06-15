@echo off
echo ========================================
echo Cleaning build artifacts...
echo ========================================

if exist "dist" (
    echo Removing dist directory...
    rmdir /s /q dist
    echo Cleaned: dist directory removed
) else (
    echo Nothing to clean: dist directory does not exist
)

echo ========================================
echo Clean completed!
echo ======================================== 