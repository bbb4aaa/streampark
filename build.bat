@echo off
setlocal enabledelayedexpansion

:: 设置基础变量
set "PRG_DIR=%~dp0"
set "VERSION=2.1.7"

:: 简单的颜色处理（兼容性最高）
set "YELLOW=[33m"
set "GREEN=[32m"
set "RED=[31m"
set "BLUE=[34m"
set "PRIMARY=[38;5;082m"
set "RESET=[0m"

:: 打印 Logo
echo.
echo       %PRIMARY%    _____ __                                             __       %RESET%
echo       %PRIMARY%   / ___// /_________  ____ _____ ___  ____  ____ ______/ /__     %RESET%
echo       %PRIMARY%   \__ \/ __/ ___/ _ \/ __ `/ __ `__ \/ __ \  __ `/ ___/ //_/     %RESET%
echo       %PRIMARY%  ___/ / /_/ /  /  __/ /_/ / / / / / / /_/ / /_/ / /  / ,^<        %RESET%
echo       %PRIMARY% /____/\__/_/   \___/\__,_/_/ /_/ /_/ ____/\__,_/_/  /_/^|_^|       %RESET%
echo       %PRIMARY%                                   /_/                            %RESET%
echo.
echo       %BLUE%   Version:  %VERSION% %RESET%
echo       %BLUE%   WebSite:  https://streampark.apache.org%RESET%
echo       %BLUE%   GitHub :  http://github.com/apache/streampark%RESET%
echo.
echo       %PRIMARY%   -------- Apache StreamPark, Make stream processing easier o~o!%RESET%
echo.

:select_mode
echo %YELLOW%StreamPark supports front-end and server-side mixed/detached build mode, Which mode do you need ?%RESET%
echo   [1] mixed mode (recommend)
echo   [2] detached mode
set "mode_choice="
set /p mode_choice="Please select (1-2): "

if "%mode_choice%"=="1" goto :mode_mixed
if "%mode_choice%"=="2" goto :mode_detached
echo %RED%Invalid selection, please try again.%RESET%
goto :select_mode

:mode_mixed
set "MODE_ARG=1"
set "MODE_NAME=mixed"
echo %GREEN%mixed mode selected.%RESET%
goto :select_scala

:mode_detached
set "MODE_ARG=2"
set "MODE_NAME=detached"
echo %GREEN%detached mode selected.%RESET%
goto :select_scala

:select_scala
echo.
echo %YELLOW%StreamPark supports Scala 2.11 and 2.12. Which version do you need ?%RESET%
echo   [1] 2.11
echo   [2] 2.12
set "scala_choice="
set /p scala_choice="Please select (1-2): "

if "%scala_choice%"=="1" set "SCALA_VER=scala-2.11" && goto :start_build
if "%scala_choice%"=="2" set "SCALA_VER=scala-2.12" && goto :start_build
echo %RED%Invalid selection, please try again.%RESET%
goto :select_scala

:start_build
if not exist "%PRG_DIR%mvnw.cmd" (
    echo %RED%Error: mvnw.cmd not found in %PRG_DIR%%RESET%
    pause
    exit /b 1
)

set "PROFILE=-P%SCALA_VER%,shaded,dist"
if "%MODE_ARG%"=="1" set "PROFILE=%PROFILE%,webapp"

echo %GREEN%[StreamPark] build info: mode @ %MODE_NAME%, %SCALA_VER%, starting...%RESET%
echo.

:: 执行 Maven 构建
call "%PRG_DIR%mvnw.cmd" %PROFILE% -DskipTests clean install

if %ERRORLEVEL% NEQ 0 (
    echo %RED%StreamPark build failed!%RESET%
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo %GREEN%StreamPark project build successful!%RESET%
echo info: package mode @ %MODE_NAME%, %SCALA_VER%
echo dist: "%PRG_DIR%dist"

if "%MODE_ARG%"=="2" (
    echo.
    echo %YELLOW%Next, you need to build front-end by yourself.%RESET%
    echo   1^) cd streampark-console\streampark-console-webapp
    echo   2^) pnpm install ^&^& pnpm build
)

pause
