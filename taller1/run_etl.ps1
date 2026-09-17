# run_etl.ps1 — Lanza el ETL con las variables de entorno correctas
# Usa rutas relativas al repo para no depender de la ubicación absoluta

$repoRoot  = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$pythonExe = Join-Path $repoRoot "venv\Scripts\python.exe"
$etlScript = Join-Path $repoRoot "taller1\etl.py"

$env:JAVA_HOME             = "C:\Program Files\Java\jdk-21.0.10"
$env:HADOOP_HOME           = Join-Path $repoRoot "winutils"
$env:PYSPARK_PYTHON        = $pythonExe
$env:PYSPARK_DRIVER_PYTHON = $pythonExe
$env:PATH                  = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;" + $env:PATH

Write-Host "Repo root   : $repoRoot"
Write-Host "JAVA_HOME   : $env:JAVA_HOME"
Write-Host "HADOOP_HOME : $env:HADOOP_HOME"
Write-Host "Python      : $pythonExe"
Write-Host ""

& $pythonExe $etlScript
