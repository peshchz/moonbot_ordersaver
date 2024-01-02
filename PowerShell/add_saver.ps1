$url1 = "http://yourdomain.ru/test-file/ordersaver.exe"
$url2 = "http://yourdomain.ru/test-file/config.ini"
$url3 = "http://yourdomain.ru/test-file/logging_config.ini"
$url4 = "http://yourdomain.ru/test-file/new_schedule.ps1"
$url5 = "http://yourdomain.ru/test-file/2updater_schedule.ps1"
$url6 = "http://yourdomain.ru/test-file/updater.exe"

$work_dir = "C:\_saver"
$file_name = "ordersaver.exe"
$executable_file1 = $work_dir + "\" + $file_name
$config_file2 = $work_dir + "\" + "config.ini"
$config_file3 = $work_dir + "\" + "logging_config.ini"
$config_file4 = $work_dir + "\" + "new_schedule.ps1"
$config_file5 = $work_dir + "\" + "2updater_schedule.ps1"
$executable_file2 = $work_dir + "\" + "updater.exe"


if(!(Test-Path -path $work_dir))  
{  
  try {
        New-Item -ItemType directory -Force -Path $work_dir 
  }
  catch {
         throw $_.Exception.Message
  }
}

if((Test-Path -path $work_dir)) {
# download executable_file1
  try {
         Invoke-WebRequest -Uri $url1 -OutFile $executable_file1
  }
  catch {
         throw $_.Exception.Message
  }

}

if (!(Test-Path -Path $config_file2 -PathType Leaf)) {
# download config_file
  try {
         Invoke-WebRequest -Uri $url2 -OutFile $config_file2
  }
  catch {
         throw $_.Exception.Message
  }

}

if (!(Test-Path -Path $config_file3 -PathType Leaf)) {
# download config_file
  try {
         Invoke-WebRequest -Uri $url3 -OutFile $config_file3
  }
  catch {
         throw $_.Exception.Message
  }

}

if (!(Test-Path -Path $config_file4 -PathType Leaf)) {
# download config_file
  try {
         Invoke-WebRequest -Uri $url4 -OutFile $config_file4
  }
  catch {
         throw $_.Exception.Message
  }

}

if (!(Test-Path -Path $config_file5 -PathType Leaf)) {
# download config_file
  try {
         Invoke-WebRequest -Uri $url5 -OutFile $config_file5
  }
  catch {
         throw $_.Exception.Message
  }

}

if((Test-Path -path $work_dir)) {
# download config_file
  try {
         Invoke-WebRequest -Uri $url6 -OutFile $executable_file2
  }
  catch {
         throw $_.Exception.Message
  }

}