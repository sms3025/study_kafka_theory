# Run from any directory. Example: .\실습\study.ps1 run producer 12
[CmdletBinding(PositionalBinding=$false)]
param(
    [Parameter(Position=0)][ValidateSet('up','build','init','run','status','stop','start','down','sql','topics','groups')][string]$Action='status',
    [switch]$Cluster,
    [Parameter(Position=1,ValueFromRemainingArguments=$true)][string[]]$LabArgs=@()
)
$ErrorActionPreference = 'Stop'
$compose = Join-Path $PSScriptRoot 'compose.yaml'
$base = @('compose','-f',$compose,'-p','kafka-field-guide')
$bootstrap = 'kafka:9092'; $broker = 'kafka'; $replicas = '1'
if ($Cluster) { $bootstrap='kafka1:9092,kafka2:9092,kafka3:9092'; $broker='kafka1'; $replicas='3' }
function Invoke-Compose([string[]]$CommandArgs) {
    & docker @base @CommandArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed (exit $LASTEXITCODE). See the output above." }
}
switch ($Action) {
    'up' {
        if ($Cluster) { Invoke-Compose @('--profile','cluster','up','-d','--wait','kafka1','kafka2','kafka3','postgres') }
        else { Invoke-Compose @('--profile','single','up','-d','--wait','kafka','postgres') }
    }
    'build' { Invoke-Compose @('--profile','tools','run','--rm','--no-deps','build') }
    'init' { Invoke-Compose @('--profile','tools','run','--rm','--no-deps','-e',"BOOTSTRAP_SERVERS=$bootstrap",'-e',"REPLICATION_FACTOR=$replicas",'cli','init') }
    'run' {
        if ($LabArgs.Count -eq 0) { $LabArgs=@('help') }
        Invoke-Compose (@('--profile','tools','run','--rm','--no-deps','-e',"BOOTSTRAP_SERVERS=$bootstrap",'-e',"REPLICATION_FACTOR=$replicas",'cli') + $LabArgs)
    }
    'status' { Invoke-Compose @('--profile','single','--profile','cluster','ps','-a') }
    'stop' {
        if ($LabArgs.Count -eq 0) { throw 'Specify the service to stop: kafka1, kafka2, kafka3 or kafka.' }
        foreach ($name in $LabArgs) { if ($name -notin @('kafka','kafka1','kafka2','kafka3')) { throw "Unknown broker: $name" } }
        Invoke-Compose (@('--profile','single','--profile','cluster','stop') + $LabArgs)
    }
    'start' {
        if ($LabArgs.Count -eq 0) { throw 'Specify the service to start.' }
        foreach ($name in $LabArgs) { if ($name -notin @('kafka','kafka1','kafka2','kafka3')) { throw "Unknown broker: $name" } }
        Invoke-Compose (@('--profile','single','--profile','cluster','start') + $LabArgs)
    }
    'down' { Invoke-Compose @('--profile','single','--profile','cluster','--profile','tools','down','--remove-orphans') }
    'sql' { Invoke-Compose @('exec','-T','postgres','psql','-U','study','-d','kafka_study','-c','SELECT * FROM order_totals ORDER BY order_id; SELECT event_id, published_at FROM outbox ORDER BY created_at;') }
    'topics' { Invoke-Compose @('exec','-T',$broker,'/opt/kafka/bin/kafka-topics.sh','--bootstrap-server',$bootstrap,'--describe') }
    'groups' { Invoke-Compose (@('exec','-T',$broker,'/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server',$bootstrap) + $LabArgs) }
}
