unit MWQ.ThreadPool.CPUUsage;

interface

type
  TCPUUsageMonitor = class
  private
    FEnabled: Boolean;
    FInitialized: Boolean;
    FLastWallTick: UInt64;
    FLastCpuTick: UInt64;

    function PlatformSample(out CpuTick: UInt64): Boolean;
  public
    constructor Create;

    function TrySample(out UsagePercent: Single): Boolean;

    property Enabled: Boolean read FEnabled write FEnabled;
  end;

implementation
{$IFDEF MSWINDOWS}
uses
  Winapi.Windows;
{$ENDIF}

{ TCPUUsageMonitor }

constructor TCPUUsageMonitor.Create;
begin
  FEnabled := True;
  FInitialized := False;
end;

{$IFDEF MSWINDOWS}
function TCPUUsageMonitor.PlatformSample(out CpuTick: UInt64): Boolean;
var
  Creation, ExitTime, Kernel, User: TFileTime;
begin
  Result := False;

  if not GetProcessTimes(GetCurrentProcess, Creation, ExitTime, Kernel, User) then
    Exit;

  CpuTick :=
      ((UInt64(Kernel.dwHighDateTime) shl 32)
              or Kernel.dwLowDateTime
              + (UInt64(User.dwHighDateTime) shl 32)
              or User.dwLowDateTime)
          div 10_000;

  Result := True;
end;
{$ENDIF}

{$IFDEF LINUX}
uses
  Posix.Time;

function TCPUUsageMonitor.PlatformSample(out CpuTick: UInt64): Boolean;
var
  TS: timespec;
begin
  Result := False;

  if clock_gettime(CLOCK_PROCESS_CPUTIME_ID, TS) <> 0 then
    Exit;

  CpuTick := UInt64(TS.tv_sec) * 1000 + UInt64(TS.tv_nsec) div 1_000_000;

  Result := True;
end;
{$ENDIF}

{$IFDEF MACOS}
uses
  Posix.SysResource,
  Posix.Time;

function TCPUUsageMonitor.PlatformSample(out CpuTick: UInt64): Boolean;
var
  R: rusage;
begin
  Result := False;

  if getrusage(RUSAGE_SELF, R) <> 0 then
    Exit;

  CpuTick :=
      UInt64(R.ru_utime.tv_sec + R.ru_stime.tv_sec) * 1000 + UInt64(R.ru_utime.tv_usec + R.ru_stime.tv_usec) div 1000;

  Result := True;
end;
{$ENDIF}

{$IFNDEF MSWINDOWS}
  {$IFNDEF LINUX}
    {$IFNDEF MACOS}
function TCPUUsageMonitor.PlatformSample(out CpuTick: UInt64): Boolean;
begin
  Result := False; // auto-disable
end;
    {$ENDIF}
  {$ENDIF}
{$ENDIF}

function TCPUUsageMonitor.TrySample(out UsagePercent: Single): Boolean;
var
  NowWall, NowCpu: UInt64;
  DeltaWall, DeltaCpu: UInt64;
begin
  Result := False;
  UsagePercent := 0;

  if not FEnabled then
    Exit;

  NowWall := GetTickCount64;

  if not PlatformSample(NowCpu) then begin
    FEnabled := False; // 🚨 auto-disable
    Exit;
  end;

  if not FInitialized then begin
    FLastWallTick := NowWall;
    FLastCpuTick := NowCpu;
    FInitialized := True;
    Exit;
  end;

  DeltaWall := NowWall - FLastWallTick;
  DeltaCpu := NowCpu - FLastCpuTick;

  if DeltaWall = 0 then
    Exit;

  UsagePercent := (DeltaCpu / DeltaWall) * 100.0;

  FLastWallTick := NowWall;
  FLastCpuTick := NowCpu;

  Result := True;
end;

end.
