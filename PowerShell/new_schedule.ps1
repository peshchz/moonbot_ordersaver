$file_name = "ordersaver.exe"
$work_dir = "C:\_saver"

$task_name = "ordersaver"
$username = "alex"

# Check sheduled task exist
if (Get-ScheduledTask -TaskName $task_name -ErrorAction SilentlyContinue) {
    Write-Host "Job exist. Terminating."
    exit
}

$dt = get-date
 
$A = New-ScheduledTaskAction -Execute $file_name -WorkingDirectory $work_dir
$T = New-ScheduledTaskTrigger -Once -At ($dt.addminutes(5-($dt.minute % 5)).AddSeconds(–$dt.Second)) -RepetitionInterval (New-TimeSpan -Minutes 5)
$P = New-ScheduledTaskPrincipal -UserId $username -RunLevel Highest
$S = New-ScheduledTaskSettingsSet
$D = New-ScheduledTask -Action $A -Principal $P -Trigger $T -Settings $S
Register-ScheduledTask $task_name -InputObject $D