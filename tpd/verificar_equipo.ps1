# verificar_equipo.ps1 — revisa si este equipo tiene todo lo necesario para el Taller 1
# (Hadoop, Spark, Kafka y la app de viajes). Solo lee: no instala, no descarga ni enciende contenedores.
# Uso (PowerShell, desde cualquier carpeta):  powershell -ExecutionPolicy Bypass -File C:\tpd\verificar_equipo.ps1
param([string]$Tpd = "C:\tpd")

$script:fallos = 0; $script:avisos = 0
function Ok($m)    { Write-Host "  [OK]    $m" -ForegroundColor Green }
function Aviso($m) { Write-Host "  [AVISO] $m" -ForegroundColor Yellow; $script:avisos++ }
function Falla($m) { Write-Host "  [FALTA] $m" -ForegroundColor Red; $script:fallos++ }
function Titulo($m){ Write-Host ""; Write-Host $m -ForegroundColor Cyan }

Write-Host "Verificación del equipo para el Taller 1 · Tecnologías de Procesamiento de Datos"
Write-Host "Carpeta de trabajo revisada: $Tpd"

Titulo "1. Equipo"
$os = Get-CimInstance Win32_OperatingSystem
if ($os.OSArchitecture -match "64") { Ok "$($os.Caption) de 64 bits (compilación $($os.BuildNumber))" } else { Falla "Windows de 32 bits: Docker Desktop no funciona; trabaja en pareja con otro estudiante" }
if ([int]$os.BuildNumber -lt 19045) { Aviso "Windows 10 anterior a 22H2: actualiza Windows antes del taller" }
$ram = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
if ($ram -ge 15) { Ok "RAM: ${ram} GB (alcanza para el mes completo)" }
elseif ($ram -ge 7.5) { Aviso "RAM: ${ram} GB: haz el taller con la semana (fhvhv_2025-06_semana.csv), no con el mes" }
else { Falla "RAM: ${ram} GB: menos de 8 GB; trabaja en pareja con otro estudiante" }
$disco = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
if ($disco -ge 15) { Ok "Disco C: libre ${disco} GB" } else { Falla "Disco C: solo ${disco} GB libres; libera espacio (en el taller Hadoop copia 3,8 GB más)" }
$cs = Get-CimInstance Win32_ComputerSystem
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
if ($cs.HypervisorPresent -or $cpu.VirtualizationFirmwareEnabled) { Ok "Virtualización activa ($($cpu.NumberOfLogicalProcessors) núcleos lógicos)" }
else { Falla "Virtualización desactivada en la BIOS/UEFI (Intel VT-x o AMD-V): Docker Desktop no arranca" }

Titulo "2. Docker"
$dockerOk = $false
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Falla "Docker Desktop no está instalado (sección 1 de la Preparación Previa)" }
else {
  $dv = (& docker version --format "{{.Server.Version}}" 2>$null)
  if ($LASTEXITCODE -eq 0 -and $dv) {
    $dockerOk = $true
    Ok "Docker Desktop encendido (motor $dv)"
    $imagenes = & docker images --format "{{.Repository}}:{{.Tag}}" 2>$null
    foreach ($img in "apache/hadoop:3.4.3", "spark:3.5.9-python3", "confluentinc/cp-kafka:7.6.0", "confluentinc/cp-zookeeper:7.6.0") {
      if ($imagenes -contains $img) { Ok "Imagen $img" } else { Falla "Falta la imagen $img (se descarga en las secciones 4 a 6 de la Preparación Previa; en clase no hay tiempo)" }
    }
  } else { Falla "Docker está instalado pero no encendido: abre Docker Desktop, espera «Engine running» y vuelve a ejecutar este comando" }
}

Titulo "3. Python y librerías"
# En un Windows nuevo, «python» puede ser un atajo que abre la Tienda de Microsoft: se descarta y se prueba «py».
$pyExe = $null
foreach ($c in "python", "py") {
  $cmd = Get-Command $c -ErrorAction SilentlyContinue
  if ($cmd -and $cmd.Source -notmatch "WindowsApps") { $pyExe = $c; break }
}
$py = if ($pyExe) { (& $pyExe --version 2>&1) | Out-String } else { "" }
if ($py -match "Python (\d+)\.(\d+)") {
  if ([int]$Matches[1] -eq 3 -and [int]$Matches[2] -ge 10) { Ok "$($py.Trim()) (comando: $pyExe)" } else { Falla "$($py.Trim()): se necesita 3.10 o superior" }
  if ($pyExe -eq "py") { Aviso "El comando «python» no responde y se usó «py»: en las guías escribe py donde diga python" }
  $prueba = @"
import importlib.metadata as m
for p in ["pyarrow","pandas","streamlit","kafka-python"]:
    try: print(p + "=" + m.version(p))
    except Exception: print(p + "=NO")
"@
  $libs = $prueba | & $pyExe - 2>$null
  foreach ($l in $libs) {
    $n, $v = $l.Trim() -split "=", 2
    $instalar = if ($n -eq "kafka-python") { "kafka-python==2.3.2" } else { $n }
    if ($v -eq "NO") { Falla "Falta la librería ${n}: pip install $instalar" }
    elseif ($n -eq "kafka-python" -and $v -ne "2.3.2") { Aviso "kafka-python ${v}: la guía fija la 2.3.2 (pip install kafka-python==2.3.2)" }
    else { Ok "$n $v" }
  }
} else { Falla "Python no está instalado: instálalo desde python.org marcando «Add python.exe to PATH» y abre una ventana nueva de PowerShell" }

Titulo "4. Carpeta de trabajo $Tpd"
$archivos = "datos\ensayo_python.py", "datos\preparar_datos.py", "hadoop\docker-compose.yml", "hadoop\config", "hadoop\mapper.py", "hadoop\reducer.py",
  "hadoop\mapper_espera.py", "hadoop\reducer_espera.py", "hadoop\zonas.csv", "spark\job_spark.py", "spark\verificar_spark.py",
  "kafka\docker-compose.yml", "kafka\app_viajes.py", "kafka\kafka_conexion.py", "kafka\simulador.py", "kafka\consumidor_consola.py",
  "kafka\verificar_flujo.py", "kafka\streaming_viajes.py", "kafka\verificar_conector.py", "kafka\zonas.csv", "kafka\zonas_centros.csv"
if (-not (Test-Path $Tpd)) { Falla "No existe ${Tpd}: copia la carpeta tpd en el disco C: (sección 2 de la Preparación Previa)" }
else {
  $faltan = @($archivos | Where-Object { -not (Test-Path (Join-Path $Tpd $_)) })
  if ($faltan.Count -eq 0) { Ok "Los $($archivos.Count) archivos de la práctica están en su lugar" } else { foreach ($f in $faltan) { Falla "Falta $Tpd\$f" } }
}

Titulo "5. Datos del caso"
$datos = @(
  @{ n = "fhvhv_tripdata_2025-06.parquet"; b = 490819474;  obligatorio = $true },
  @{ n = "fhvhv_2025-06_semana.csv";       b = 932199265;  obligatorio = $true },
  @{ n = "fhvhv_2025-06.csv";              b = 4042229115; obligatorio = ($ram -ge 15) })
foreach ($d in $datos) {
  $p = Join-Path $Tpd "datos\$($d.n)"
  if (Test-Path $p) {
    $t = (Get-Item $p).Length
    if ($t -eq $d.b) { Ok "$($d.n) ($([math]::Round($t / 1e9, 2)) GB, tamaño exacto)" } else { Falla "$($d.n) incompleto: $t bytes, debería tener $($d.b); repite preparar_datos.py" }
  } elseif ($d.obligatorio) { Falla "Falta $($d.n): ejecuta preparar_datos.py (sección 3 de la Preparación Previa)" }
  else { Aviso "Falta $($d.n): con 8 GB de RAM puedes trabajar con la semana" }
}

Titulo "6. Puertos que usa el taller"
# Contenedor de Docker que publica cada puerto (si hay alguno).
$porPuerto = @{}
if ($dockerOk) {
  foreach ($linea in (& docker ps --format "{{.Names}}|{{.Ports}}" 2>$null)) {
    $nombre, $puertos = $linea -split "\|", 2
    foreach ($m in [regex]::Matches($puertos, ":(\d+)->")) { $porPuerto[[int]$m.Groups[1].Value] = $nombre }
  }
}
$practica = "^(hadoop-|kafka$|zookeeper$|spark$)"
$escuchando = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue
foreach ($pt in 9870, 8088, 19888, 9092, 4040, 8501) {
  $cont = $porPuerto[$pt]
  if ($cont -and $cont -match $practica) { Ok "Puerto $pt en uso por la práctica (contenedor $cont, encendido)" }
  elseif ($cont) { Falla "Puerto $pt ocupado por el contenedor $cont, de otro proyecto: detenlo antes del taller (docker stop $cont)" }
  else {
    $c = $escuchando | Where-Object LocalPort -eq $pt | Select-Object -First 1
    if (-not $c) { Ok "Puerto $pt libre" }
    else {
      $proc = (Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue).ProcessName
      if ($proc -match "wslrelay|com.docker") { Aviso "Puerto $pt ocupado por Docker ($proc); enciende Docker Desktop y vuelve a ejecutar para saber qué contenedor lo usa" }
      else { Falla "Puerto $pt ocupado por el programa ${proc}: ciérralo antes del taller" }
    }
  }
}

Write-Host ""
if ($script:fallos -eq 0) { Write-Host "RESULTADO: LISTO para el Taller 1 ($($script:avisos) avisos)." -ForegroundColor Green }
else { Write-Host "RESULTADO: FALTAN $($script:fallos) cosas ($($script:avisos) avisos). Corrígelas con la Preparación Previa y vuelve a ejecutar este comando." -ForegroundColor Red }
