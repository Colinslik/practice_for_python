@echo off

set topdir=.

if exist "%topdir%\dist" rmdir "%topdir%\dist" /s /q
if exist "%topdir%\build" rmdir "%topdir%\build" /s /q
if exist "%topdir%\pda.spec" del "%topdir%\pda.spec" /F

for /f "skip=3 delims=" %%x in (%topdir%\..\buildenv.mk) do (set %%x)

if not defined tag_name (
set version_numbers=%CONFIG_VERSION_MAJOR%.%CONFIG_VERSION_MINOR%.%CONFIG_BUILD_NO%
) else (
set version_numbers=%tag_name%
)

echo Arcserve>%topdir%\etc\application.conf
copy /Y "%topdir%\etc\pda.conf.arcserve" "%topdir%\etc\pda.conf"
pyinstaller -F --add-data "%topdir%\etc\pda.conf;etc" --add-data "%topdir%\etc\pda.conf.user.template;etc" --add-data "%topdir%\etc\application.conf;etc" %topdir%\src\pda.py
rename "%topdir%\dist\pda.exe" "pda_arc_%version_numbers%.exe"

echo DRProphet>%topdir%\etc\application.conf
copy /Y "%topdir%\etc\pda.conf.drprophet" "%topdir%\etc\pda.conf"
pyinstaller -F --add-data "%topdir%\etc\pda.conf;etc" --add-data "%topdir%\etc\pda.conf.user.template;etc" --add-data "%topdir%\etc\application.conf;etc" %topdir%\src\pda.py
rename "%topdir%\dist\pda.exe" "pda_drp_%version_numbers%.exe"


rmdir "%topdir%\build" /s /q
del "%topdir%\pda.spec" /F
del "%topdir%\etc\application.conf" /F
del "%topdir%\etc\pda.conf" /F
