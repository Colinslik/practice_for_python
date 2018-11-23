@echo on

SET CURRENT=%~dp0
SET BUILD_SCRIPT=build-service.bat

IF [%1]==[] ( 
	SET root=C:\pda
) ELSE ( 
	SET root=%1%
)

SET PDA_PATH=%root%\service
SET ORIGIN_BATCH=%PDA_PATH%\%BUILD_SCRIPT%
SET NEW_BATCH=%PDA_PATH%\new_%BUILD_SCRIPT%

SET BUILD_FILE_DRP=%PDA_PATH%\dist\pda_drp.exe
SET BUILD_FILE_ARC=%PDA_PATH%\dist\pda_arc.exe

CALL %CURRENT%scripts\env.bat

ECHO =====Environment variables=====
ECHO %PATH%
ECHO ===============================

CMD /C pip install pyinstaller
@echo off
ECHO set topdir=%PDA_PATH%> %NEW_BATCH%
(FOR /f "skip=3 delims=]" %%i IN ('findstr "^" "%ORIGIN_BATCH%"') DO (
	ECHO %%i
)) >> %NEW_BATCH%
@echo on
CD /D %PDA_PATH%
CALL %NEW_BATCH%
DEL %NEW_BATCH%
CD /D %CURRENT%

CMD /C pip uninstall -y pyinstaller

IF NOT EXIST %BUILD_FILE_DRP% GOTO ERROR
IF NOT EXIST %BUILD_FILE_ARC% GOTO ERROR

ECHO COMPLETE
EXIT 0

:ERROR
	ECHO FAILED
	EXIT 1
