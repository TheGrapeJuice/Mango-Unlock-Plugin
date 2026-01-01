cls
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Vibrant ANSI Colors
$Orange = "$([char]0x1b)[38;2;255;165;0m"  # Mango orange
$Cyan   = "$([char]0x1b)[96m"
$Yellow = "$([char]0x1b)[93m"
$Green  = "$([char]0x1b)[92m"
$Red    = "$([char]0x1b)[91m"
$Gray   = "$([char]0x1b)[90m"
$Reset  = "$([char]0x1b)[0m"

# Your original MANGO ASCII banner - EXACTLY as you gave it
$RawBanner = @"
__/\\\\____________/\\\\_____/\\\\\\\\\_____/\\\\\_____/\\\_____/\\\\\\\\\\\\_______/\\\\\______
 _\/\\\\\\________/\\\\\\___/\\\\\\\\\\\\\__\/\\\\\\___\/\\\___/\\\//////////______/\\\///\\\____
  _\/\\\//\\\____/\\\//\\\__/\\\/////////\\\_\/\\\/\\\__\/\\\__/\\\_______________/\\\/__\///\\\__
   _\/\\\\///\\\/\\\/_\/\\\_\/\\\_______\/\\\_\/\\\//\\\_\/\\\_\/\\\____/\\\\\\\__/\\\______\//\\\_
    _\/\\\__\///\\\/___\/\\\_\/\\\\\\\\\\\\\\\_\/\\\\//\\\\/\\\_\/\\\___\/////\\\_\/\\\_______\/\\\_
     _\/\\\____\///_____\/\\\_\/\\\/////////\\\_\/\\\_\//\\\/\\\_\/\\\_______\/\\\_\//\\\______/\\\__
      _\/\\\_____________\/\\\_\/\\\_______\/\\\_\/\\\__\//\\\\\\_\/\\\_______\/\\\__\///\\\___/\\\___
       _\/\\\_____________\/\\\_\/\\\_______\/\\\_\/\\\___\//\\\\\_\//\\\\\\\\\\\\/_____\///\\\\\/_____
        _\///______________\///__\///________\///__\///_____\/////___\////////////_________\/////_______
"@

function Center-Text {
    param(
        [string]$Text,
        [string]$Color = $Reset
    )
    $width = $Host.UI.RawUI.WindowSize.Width
    $lines = $Text -split "`n"
    foreach ($line in $lines) {
        $line = $line.TrimEnd()
        $padding = [Math]::Max(0, ($width - $line.Length) / 2)
        Write-Host (" " * $padding) "$Color$line$Reset"
    }
}

function Show-Banner {
    Clear-Host
    Write-Host ""
    Center-Text $RawBanner $Orange
    Write-Host ""
    Center-Text "Steam + Millennium + MangoUnlock Setup" $Cyan
    Write-Host ""
    Center-Text "Mango Installer" $Cyan
    Write-Host ""
    Center-Text ("-" * 70) $Cyan
    Write-Host ""
}

function Log-Message {
    param(
        [string]$Message,
        [string]$Color = $Reset
    )
    $width = $Host.UI.RawUI.WindowSize.Width
    $prefix = "[$([DateTime]::Now.ToString('HH:mm:ss'))] "
    $full = $prefix + $Message
    $padding = [Math]::Max(0, ($width - $full.Length) / 2)
    Write-Host (" " * $padding) "$Color$full$Reset"
}

function Start-Section {
    param([string]$Title)
    Write-Host ""
    Center-Text ">>> $Title <<<" $Yellow
    Center-Text ("=" * ($Title.Length + 8)) $Yellow
    Write-Host ""
}

Show-Banner

$steamRegPath = 'HKCU:\Software\Valve\Steam'
$steamToolsRegPath = 'HKCU:\Software\Valve\Steamtools'
$steamPath = ""

function Remove-ItemIfExists {
    param([string]$Path)
    if (Test-Path $Path) {
        Remove-Item -Path $Path -Force -Recurse -ErrorAction SilentlyContinue
        Log-Message "Deleted: $Path" $Gray
    }
}

function ForceStopProcess {
    param([string]$ProcessName)
    $proc = Get-Process $ProcessName -ErrorAction SilentlyContinue
    if ($proc) {
        Log-Message "Force stopping $ProcessName process(es)..." $Yellow
        $proc | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        Get-Process $ProcessName -ErrorAction SilentlyContinue | ForEach-Object {
            Start-Process cmd -ArgumentList "/c taskkill /f /im $ProcessName.exe" -WindowStyle Hidden -ErrorAction SilentlyContinue
        }
    }
}

$oldScript = Join-Path $env:USERPROFILE "get.ps1"
Remove-ItemIfExists $oldScript

ForceStopProcess "steam"

if (Get-Process "steam" -ErrorAction SilentlyContinue) {
    Start-Section "Closing Steam"
    Log-Message "Please close Steam manually if still running..." $Red
    while (Get-Process "steam" -ErrorAction SilentlyContinue) {
        Center-Text ">>> Waiting for Steam to exit... <<<" $Red
        Start-Sleep 1.5
    }
    Log-Message "Steam closed successfully." $Green
}

Start-Section "Detecting Steam Installation"
if (Test-Path $steamRegPath) {
    $props = Get-ItemProperty -Path $steamRegPath -ErrorAction SilentlyContinue
    if ($props -and 'SteamPath' -in $props.PSObject.Properties.Name) {
        $steamPath = $props.SteamPath
        Log-Message "Steam found at: $steamPath" $Green
    }
}
if ([string]::IsNullOrWhiteSpace($steamPath) -or -not (Test-Path $steamPath -PathType Container)) {
    Log-Message "Steam installation not found!" $Red
    Center-Text "Press any key to exit..." $Red
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
    exit
}
Log-Message "Steam installation verified." $Green

$hidPath = Join-Path $steamPath "xinput1_4.dll"
$localPath = Join-Path $env:LOCALAPPDATA "steam"

if (-not (Test-Path $localPath)) {
    New-Item $localPath -ItemType Directory -Force | Out-Null
    Log-Message "Created folder: $localPath" $Gray
}

Start-Section "Applying Initial Modifications"

Remove-ItemIfExists (Join-Path $steamPath "steam.cfg")
Remove-ItemIfExists (Join-Path $steamPath "package\beta")
Remove-ItemIfExists (Join-Path $env:LOCALAPPDATA "Microsoft\Tencent")
Remove-ItemIfExists (Join-Path $steamPath "version.dll")

try {
    Add-MpPreference -ExclusionPath $hidPath -ErrorAction SilentlyContinue
    Log-Message "Added Windows Defender exclusion for xinput1_4.dll" $Gray
} catch {}

# Create mangoplugin folder for multiplayer fix downloads and add exclusion
$mangoPluginPath = Join-Path $env:APPDATA "mangoplugin"
if (-not (Test-Path $mangoPluginPath)) {
    New-Item $mangoPluginPath -ItemType Directory -Force | Out-Null
    Log-Message "Created folder: $mangoPluginPath" $Gray
}
try {
    Add-MpPreference -ExclusionPath $mangoPluginPath -ErrorAction SilentlyContinue
    Log-Message "Added Windows Defender exclusion for mangoplugin folder" $Gray
} catch {}

Log-Message "Downloading required DLL..." $Yellow
try {
    Invoke-RestMethod "https://update.wudrm.com/xinput1_4.dll" -OutFile $hidPath -ErrorAction Stop
    Log-Message "DLL downloaded and placed successfully" $Green
} catch {
    Log-Message "Download failed - retrying..." $Red
    if (Test-Path $hidPath) {
        Move-Item $hidPath "$hidPath.old" -Force -ErrorAction SilentlyContinue
        Log-Message "Backed up old DLL" $Gray
    }
    Invoke-RestMethod "https://update.wudrm.com/xinput1_4.dll" -OutFile $hidPath -ErrorAction SilentlyContinue
    if (Test-Path $hidPath) { Log-Message "Retry successful" $Green } else { Log-Message "Retry failed" $Red }
}

if (-not (Test-Path $steamToolsRegPath)) {
    New-Item -Path $steamToolsRegPath -Force | Out-Null
    Log-Message "Created registry key" $Gray
}
Remove-ItemProperty -Path $steamToolsRegPath -Name "ActivateUnlockMode" -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $steamToolsRegPath -Name "AlwaysStayUnlocked" -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $steamToolsRegPath -Name "notUnlockDepot" -ErrorAction SilentlyContinue
Set-ItemProperty -Path $steamToolsRegPath -Name "iscdkey" -Value "true" -Type String
Log-Message "Registry modifications applied" $Green

Log-Message "All initial modifications completed successfully!" $Cyan

function Install-MangoUnlockPlugin {
    Start-Section "Installing MangoUnlock Plugin"

    Log-Message "Fetching latest release info..." $Yellow
    try {
        $release = Invoke-RestMethod "https://api.github.com/repos/TheGrapeJuice/Mango-Unlock-Plugin/releases/latest" -ErrorAction Stop
        $tag = $release.tag_name
        $zipUrl = "https://github.com/TheGrapeJuice/Mango-Unlock-Plugin/releases/download/$tag/MangoUnlock.zip"
        Log-Message "Latest version: $tag" $Green
    } catch {
        Log-Message "Failed to fetch latest - using fallback v2.0.0" $Red
        $tag = "v2.0.0"
        $zipUrl = "https://github.com/TheGrapeJuice/Mango-Unlock-Plugin/releases/download/v2.0.0/MangoUnlock.zip"
    }

    $pluginsPath = Join-Path $steamPath "plugins"
    $targetFolder = Join-Path $pluginsPath "MangoUnlock"
    $tempZip = Join-Path $env:TEMP "MangoUnlock.zip"
    $pluginJsonPath = Join-Path $targetFolder "plugin.json"

    if (-not (Test-Path $pluginsPath)) {
        New-Item $pluginsPath -ItemType Directory -Force | Out-Null
        Log-Message "Created plugins folder" $Gray
    }

    # Check if plugin already exists and compare versions
    $latestVersion = $tag -replace '^v', ''
    if (Test-Path $pluginJsonPath) {
        try {
            $pluginJson = Get-Content $pluginJsonPath -Raw | ConvertFrom-Json
            $installedVersion = $pluginJson.version
            Log-Message "Installed version: v$installedVersion" $Cyan

            # Compare versions
            try {
                $installedVer = [Version]$installedVersion
                $latestVer = [Version]$latestVersion
                
                if ($installedVer -ge $latestVer) {
                    Log-Message "MangoUnlock is already up to date (v$installedVersion)" $Green
                    Log-Message "Skipping update..." $Gray
                    return
                } else {
                    Log-Message "Update available: v$installedVersion -> v$latestVersion" $Yellow
                }
            } catch {
                # If version comparison fails, do string comparison
                if ($installedVersion -eq $latestVersion) {
                    Log-Message "MangoUnlock is already up to date (v$installedVersion)" $Green
                    Log-Message "Skipping update..." $Gray
                    return
                } else {
                    Log-Message "Update available: v$installedVersion -> v$latestVersion" $Yellow
                }
            }
        } catch {
            Log-Message "Could not read installed version - will update" $Yellow
        }
    } else {
        Log-Message "MangoUnlock not found - installing fresh" $Yellow
    }

    Log-Message "Downloading MangoUnlock $tag..." $Yellow
    try {
        Invoke-WebRequest $zipUrl -OutFile $tempZip -UseBasicParsing
        Log-Message "Download complete" $Green
    } catch {
        Log-Message "Download failed - aborting plugin install" $Red
        return
    }

    Log-Message "Extracting plugin (updating files in place)..." $Yellow
    try {
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zip = [System.IO.Compression.ZipFile]::OpenRead($tempZip)
        $filesUpdated = 0
        $filesFailed = 0
        
        foreach ($entry in $zip.Entries) {
            $destPath = Join-Path $targetFolder $entry.FullName
            $destDir = Split-Path $destPath -Parent
            
            # Create directory if needed
            if (-not (Test-Path $destDir)) { 
                New-Item $destDir -ItemType Directory -Force | Out-Null 
            }
            
            # Only process files (not directories)
            if ($entry.Name -ne "") {
                try {
                    # Extract to a temp file first
                    $tempFile = Join-Path $env:TEMP ("MangoUnlock_" + [System.IO.Path]::GetRandomFileName())
                    [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $tempFile, $true)
                    
                    # Try to move/replace the file
                    if (Test-Path $destPath) {
                        try {
                            # Try to replace the existing file
                            Move-Item $tempFile $destPath -Force -ErrorAction Stop
                            $filesUpdated++
                        } catch {
                            # File might be in use, try copying instead
                            try {
                                Copy-Item $tempFile $destPath -Force -ErrorAction Stop
                                Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
                                $filesUpdated++
                            } catch {
                                Log-Message "Could not update: $($entry.FullName) (file in use)" $Red
                                Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
                                $filesFailed++
                            }
                        }
                    } else {
                        # New file, just move it
                        Move-Item $tempFile $destPath -Force
                        $filesUpdated++
                    }
                } catch {
                    Log-Message "Failed to extract: $($entry.FullName)" $Red
                    $filesFailed++
                }
            }
        }
        $zip.Dispose()
        
        if ($filesFailed -eq 0) {
            Log-Message "MangoUnlock installed/updated successfully! ($filesUpdated files)" $Green
        } else {
            Log-Message "MangoUnlock updated with warnings: $filesUpdated files OK, $filesFailed files skipped" $Yellow
            Log-Message "Some files were in use - restart Steam and re-run if needed" $Yellow
        }
        Log-Message "Location: $targetFolder" $Cyan
    } catch {
        Log-Message "Extraction failed: $_" $Red
    } finally {
        Remove-Item $tempZip -Force -ErrorAction SilentlyContinue
    }
}

$millenniumPath = Join-Path $steamPath "millennium.dll"
$millenniumInstalled = $false

if (Test-Path $millenniumPath) {
    try {
        $versionStr = (Get-Item $millenniumPath).VersionInfo.FileVersion
        $versionStr = $versionStr -replace '^v\.?', ''
        $currentVersion = [Version]$versionStr
        $requiredVersion = [Version]"2.34.0"
        
        if ($currentVersion -ge $requiredVersion) {
            $millenniumInstalled = $true
            Log-Message "Millennium already installed (v$versionStr)" $Green
        } else {
            Log-Message "Millennium version $versionStr is outdated (requires v2.34.0+)" $Yellow
        }
    } catch {
        Log-Message "Could not determine Millennium version - will reinstall" $Yellow
    }
}

if ($millenniumInstalled) {
    Install-MangoUnlockPlugin

    Start-Section "Launching Steam"
    Log-Message "Starting enhanced Steam..." $Cyan
    Start-Process (Join-Path $steamPath "steam.exe")

    Write-Host ""
    Center-Text ">>> NOTE: Steam may take 30+ seconds to launch <<<" $Yellow
    Center-Text "This is normal - Millennium & MangoUnlock are loading." $Yellow
    Center-Text "Please wait patiently and do not close or restart Steam." $Yellow
    Write-Host ""

    Log-Message "Setup complete! Enjoy!" $Cyan
    $width = $Host.UI.RawUI.WindowSize.Width
    for ($i = 12; $i -ge 1; $i--) {
        $msg = "Window closing in $i second" + $(if ($i -ne 1) { "s" } else { "" }) + "..."
        $padding = [Math]::Max(0, [int](($width - $msg.Length) / 2))
        $paddedMsg = (" " * $padding) + $Gray + $msg + $Reset + (" " * $padding)
        Write-Host "`r$paddedMsg" -NoNewline
        Start-Sleep 1
    }
    Write-Host ""
    [Environment]::Exit(0)
}

Start-Section "Installing Millennium"

$downloads = (New-Object -ComObject Shell.Application).NameSpace('shell:Downloads').Self.Path
$installer = Join-Path $downloads "MillenniumInstaller-Windows.exe"
$url = "https://github.com/SteamClientHomebrew/Installer/releases/latest/download/MillenniumInstaller-Windows.exe"

Log-Message "Downloading Millennium installer..." $Yellow
try {
    Invoke-WebRequest $url -OutFile $installer -UseBasicParsing
    Log-Message "Installer downloaded" $Green
} catch {
    Log-Message "Download failed!" $Red
    Start-Sleep 10
    exit
}

Log-Message "Launching installer - DO NOT CLOSE THIS WINDOW!" $Red -BackgroundColor Yellow
Center-Text ">>> Please complete the installation wizard <<<" $Cyan
Center-Text ">>> Script will continue automatically when installer closes <<<" $Cyan

$proc = Start-Process $installer -PassThru
Log-Message "Installer started (PID: $($proc.Id))" $Gray

while (-not $proc.HasExited) { Start-Sleep 2 }

Log-Message "Millennium installation finished!" $Green

ForceStopProcess "steam"
Start-Sleep 5
Log-Message "Ensured Steam closed for plugin install" $Gray

Install-MangoUnlockPlugin

Start-Section "Final Launch"
Log-Message "Launching fully enhanced Steam..." $Cyan
Start-Process (Join-Path $steamPath "steam.exe")

Write-Host ""
Center-Text ">>> NOTE: Steam may take 30+ seconds to launch <<<" $Yellow
Center-Text "This is normal - Millennium & MangoUnlock are loading." $Yellow
Center-Text "Please wait patiently and do not close or restart Steam." $Yellow
Write-Host ""

Log-Message "Everything complete! Enjoy your unlocked Steam!" $Cyan
$width = $Host.UI.RawUI.WindowSize.Width
for ($i = 12; $i -ge 1; $i--) {
    $msg = "Window closing in $i second" + $(if ($i -ne 1) { "s" } else { "" }) + "..."
    $padding = [Math]::Max(0, [int](($width - $msg.Length) / 2))
    $paddedMsg = (" " * $padding) + $Gray + $msg + $Reset + (" " * $padding)
    Write-Host "`r$paddedMsg" -NoNewline
    Start-Sleep 1
}
Write-Host ""
[Environment]::Exit(0)
