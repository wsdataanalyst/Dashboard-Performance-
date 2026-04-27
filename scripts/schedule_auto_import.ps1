$ErrorActionPreference = "Stop"

# -----------------------------------------------------------------------------
# Config (ajuste se quiser)
# -----------------------------------------------------------------------------
$TaskName = "DashboardPerformance-AutoImport"
$ProjectDir = "C:\Users\wsana\Projeto python\Dasboard Performance"
$PythonExe  = "$ProjectDir\.venv\Scripts\python.exe"
$ImportScript = "$ProjectDir\scripts\auto_import.py"

# Pasta de onde o robô lê os arquivos (seus 7 arquivos)
$InboxDir = "C:\Users\wsana\Downloads\Base de Dados"

# Hora diária (24h). Ex.: "08:10"
$DailyTime = "08:10"

# -----------------------------------------------------------------------------
# Scheduled task
# -----------------------------------------------------------------------------
$Args = @(
  "-ExecutionPolicy", "Bypass",
  "-NoProfile",
  "-Command",
  "`$env:AUTO_IMPORT_DIR='$InboxDir'; `$env:AUTO_IMPORT_MOVE_PROCESSED='1'; & `"$PythonExe`" `"$ImportScript`""
) -join " "

$Action  = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $Args -WorkingDirectory $ProjectDir
$Trigger = New-ScheduledTaskTrigger -Daily -At ([DateTime]::ParseExact($DailyTime, "HH:mm", $null))
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

# Rodar no usuário atual (vai pedir permissão se necessário)
try { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false | Out-Null } catch {}
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Description "Importa planilhas e salva no histórico automaticamente" | Out-Null

Write-Host "OK: tarefa agendada criada/atualizada: $TaskName"
Write-Host "Vai rodar todo dia às $DailyTime, lendo de: $InboxDir"

