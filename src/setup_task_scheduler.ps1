$Action = New-ScheduledTaskAction -Execute "python.exe" -Argument "c:\Users\christian\Code\crypto-elt\src\extract_load.py"
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 10)
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType Interactive
$Settings = New-ScheduledTaskSettingsSet
$TaskName = "CryptoPayloadRun"

Register-ScheduledTask -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings -TaskName $TaskName -Description "Runs Crypto ELT pipeline every 10 minutes"

Write-Host "Task '$TaskName' created successfully. It will run every 10 minutes."
