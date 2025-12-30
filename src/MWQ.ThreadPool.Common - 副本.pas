unit MWQ.ThreadPool.Common;

interface

uses
  System.Classes,
  System.SysUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Winapi.Windows,
  System.Math;

const
  PRIORITY_MAX = 7;

type
  TWorkerKind = (wkBase, wkDynamic, wkBurst);

  TWorkerInfo = record
    Kind: TWorkerKind;
    LastActiveTick: UInt64;
  end;

  TTaskKindConfig = record
    Name: string;
    MaxWorkers: Integer; // quota
    RateCapacity: Int64; // token bucket
    RateRefillPerSec: Int64;
  end;

  TSimpleTaskProc = reference to procedure;
  TMetricKind = (mkEnqueued, mkStarted, mkCompleted, mkFailed, mkExpired);

  { ================= Task Interfaces ================= }

  IThreadTask = interface
    ['{E43A9C91-0B5F-4D99-AF13-7EAC4D4F0001}']

    { identity / grouping }
    function Key: UIntPtr; // optional grouping key (0 = none)
    function Owner: UIntPtr; // cancellation owner (0 = none)
    function Kind: Integer; // scheduling kind (>= 0)

    { scheduling }
    function Priority: Byte; // 0..PRIORITY_MAX
    function DeadlineTick: UInt64; // 0 = no deadline

    { execution }
    procedure Execute;
    function TaskDone: Boolean; // True = success, False = failed
    function CanRetry: Boolean;

    { cancellation }
    procedure Cancel;
    function IsCancelled: Boolean;

    { lifecycle }
    procedure Cleanup;
  end;

  { ================= Metrics ================= }

  TThreadPoolMetrics = record
    Enqueued: Int64;
    Started: Int64;
    Completed: Int64;
    Failed: Int64;
    Retried: Int64;
    Canceled: Int64;
    Throttled: Int64;
    QuotaDenied: Int64;
    Expired: Int64;
    QueueDepth: Int64;
    ExecTimeUsTotal: Int64;
  end;

  TThreadPoolWorkerStats = record
    TotalWorkers: Integer;
    ActiveWorkers: Integer;
    IdleWorkers: Integer;
    BaseWorkers: Integer;
    DynamicWorkers: Integer;
    BurstWorkers: Integer;
  end;

  PThreadPoolMetrics = ^TThreadPoolMetrics;

  { ================= Rate Limiter ================= }

  TKindRateLimiter = record
    Capacity: Int64;
    Tokens: Int64;
    RefillPerSec: Int64;
    LastTick: UInt64;
  end;

  { ================= Thread Pool ================= }

  TCommonThreadPool = class
  private
    class var
      FInstance: TCommonThreadPool;

  private
    type
      TWorker = class(TThread)
      private
        FOwner: TCommonThreadPool;
      protected
        procedure Execute; override;
      public
        constructor Create(AOwner: TCommonThreadPool; CPU: Integer);
      end;

  private
    FTaskKinds: TDictionary<Integer, TTaskKindConfig>;
    { workers }
    FWorkers: TArray<TWorker>;
    FStop: Boolean;

    { priority queues }
    FQueues: array[0..PRIORITY_MAX] of TQueue<IThreadTask>;
    FQueueLocks: array[0..PRIORITY_MAX] of TCriticalSection;
    FQueueEvent: TEvent;

    { per-key mutex }
    FKeyLocks: TDictionary<UIntPtr, TLightweightMREW>;
    FKeyLockCS: TCriticalSection;

    { metrics }
    FMetrics: TDictionary<Integer, PThreadPoolMetrics>;
    FMetricsCS: TCriticalSection;

    { rate limiting }
    FRateLimiters: TDictionary<Integer, TKindRateLimiter>;
    FRateCS: TCriticalSection;

    { quotas }
    FActiveByKind: TDictionary<Integer, Integer>;
    FQuotaByKind: TDictionary<Integer, Integer>;
    FQuotaCS: TCriticalSection;

    FMinWorkers: Integer;
    FMaxWorkers: Integer;
    FBurstLimit: Integer; // safety cap
    FIdleTimeoutMs: Cardinal;

    FWorkersTotal: Integer;
    FWorkersActive: Integer;
    FWorkersIdle: Integer;

    FBaseCount, FDynamicCount, FBurstCount: Integer;

    FWorkerCS: TCriticalSection;

    function DequeueTask: IThreadTask;
    function GetKind(const Task: IThreadTask): Integer;

    procedure EnterKey(Key: UIntPtr);
    procedure LeaveKey(Key: UIntPtr);

    function AllowByRate(Kind: Integer): Boolean;
    function TryEnterKind(Kind: Integer): Boolean;
    procedure LeaveKind(Kind: Integer);

    procedure IncMetric(Kind: Integer; var Field: Int64; Delta: Int64 = 1);
    procedure _RegisterTaskKind(
        AKind: Integer;
        const AName: string;
        MaxWorkers: Integer;
        RateCapacity: Int64;
        RateRefillPerSec: Int64
    );
    procedure _CancelByOwner(Owner: UIntPtr);
    function _GetWorkerStats: TThreadPoolWorkerStats;
    function IsKindRegistered(AKind: Integer): Boolean;
  protected

  public
    class function GetInstance: TCommonThreadPool;

    { === class-level helpers === }
    class procedure EnqueueTask(const Task: IThreadTask); static;
    class procedure EnqueueProc(
        const Proc: TSimpleTaskProc;
        APriority: Byte = 0;
        AKind: Integer = 0;
        ATaskOwner: UIntPtr = 0;
        AKey: UIntPtr = 0;
        ADeadlineTick: UInt64 = 0;
        ACanRetry: Boolean = False
    ); static;
    class procedure RegisterTaskKind(
        AKind: Integer;
        const AName: string;
        MaxWorkers: Integer;
        RateCapacity: Int64;
        RateRefillPerSec: Int64
    ); static;
    class procedure CancelByOwner(Owner: UIntPtr); static;
    class function GetWorkerStats: TThreadPoolWorkerStats;

  public
    constructor Create(WorkerCount: Integer);
    destructor Destroy; override;

    procedure Enqueue(const Task: IThreadTask);

    { configuration }
    procedure SetRateLimit(Kind: Integer; Capacity, RefillPerSec: Int64);
    procedure SetKindQuota(Kind: Integer; MaxWorkers: Integer);

    { metrics }
    function MetricsSnapshot: TDictionary<Integer, PThreadPoolMetrics>;

    procedure CancelByKey(Key: UIntPtr);
    procedure AddMetrics(var Dst: TThreadPoolMetrics; const Src: TThreadPoolMetrics);
    function GetMetricsSummary: TThreadPoolMetrics;
    function GetMetricsByKind: TDictionary<Integer, TThreadPoolMetrics>;
    function GetMetricsForKind(Kind: Integer; out Metrics: TThreadPoolMetrics): Boolean;
    function AvgExecTimeUs(const M: TThreadPoolMetrics): Double;
    function DumpThreadPoolStats: string;
  end;

  { ================= Anonymous Thread Task ================= }

  TAnonymousThreadTask = class(TInterfacedObject, IThreadTask)
  private
    FProc: TSimpleTaskProc;
    FPriority: Byte;
    FKey: UIntPtr;
    FKind: Integer;
    FOwner: Integer;
    FCanceled: Boolean;
    FCanRetry: Boolean;
    FDone: Boolean;
    FDeadlineTick: UInt64;
  public
    constructor Create(
        const AProc: TSimpleTaskProc;
        APriority: Byte;
        AKind: Integer;
        AOwner: UIntPtr;
        AKey: UIntPtr;
        ADeadlineTick: UInt64;
        ACanRetry: Boolean
    );

    //    function _AddRef: Integer; stdcall;
    //    function _Release: Integer; stdcall;

    { IThreadTask }
    function Key: UIntPtr;
    function Owner: UIntPtr;
    function Priority: Byte;
    function DeadlineTick: UInt64;
    procedure Execute;
    function TaskDone: Boolean;
    function CanRetry: Boolean;
    procedure Cancel;
    function IsCancelled: Boolean;
    procedure Cleanup;

    { ITaskInfo }
    function Kind: Integer;
  end;

  TMWQThreadPool = TCommonThreadPool;

function BackoffMs(Retry: Integer): Integer;

implementation

{ ================= Utilities ================= }

function BackoffMs(Retry: Integer): Integer;
begin
  Result := Min(5 + Retry * Retry * 10, 500);
end;

{ ================= Worker ================= }

constructor TCommonThreadPool.TWorker.Create(AOwner: TCommonThreadPool; CPU: Integer);
begin
  inherited Create(False);
  FreeOnTerminate := False;
  FOwner := AOwner;
  SetThreadAffinityMask(GetCurrentThread, UIntPtr(1) shl CPU);
end;

procedure TCommonThreadPool.TWorker.Execute;
var
  Task: IThreadTask;
  Kind: Integer;
  Retry: Integer;
  StartTick, Latency: UInt64;
begin
  while not Terminated and not FOwner.FStop do begin
    Task := FOwner.DequeueTask;
    if Task = nil then begin
      FOwner.FQueueEvent.WaitFor(10);
      Continue;
    end;

    if Task.IsCancelled then
      Continue;

    Kind := FOwner.GetKind(Task);

    if (Task.DeadlineTick > 0) and (GetTickCount64 > Task.DeadlineTick) then begin
      Task.Cancel;
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Expired);
      Continue;
    end;

    if not FOwner.TryEnterKind(Kind) then begin
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].QuotaDenied);
      FOwner.EnqueueTask(Task);
      Sleep(1);
      Break;
    end;

    FOwner.EnterKey(Task.Key);
    try
      Retry := 0;
      StartTick := GetTickCount64;
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Started);

      while True do begin
        Task.Execute;

        if Task.TaskDone then
          Break;

        if not Task.CanRetry or (Retry >= 5) then begin
          FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Failed);
          Break;
        end;

        Inc(Retry);
        FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Retried);
        Sleep(BackoffMs(Retry));
      end;

      Latency := (GetTickCount64 - StartTick) * 1000;
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].ExecTimeUsTotal, Latency);

      if Task.TaskDone then
        FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Completed);

    finally
      Task.Cleanup;
      FOwner.LeaveKey(Task.Key);
      FOwner.LeaveKind(Kind);
    end;
  end;
end;

procedure TCommonThreadPool.CancelByKey(Key: UIntPtr);
var
  P: Integer;
  Task: IThreadTask;
  TempQueue: TQueue<IThreadTask>;
begin
  if Key = 0 then
    Exit;

  for P := 0 to PRIORITY_MAX do begin
    FQueueLocks[P].Enter;
    try
      TempQueue := TQueue<IThreadTask>.Create;
      try
        while FQueues[P].Count > 0 do begin
          Task := FQueues[P].Dequeue;
          if Task.Key = Key then
            Task.Cancel
          else
            TempQueue.Enqueue(Task);
        end;

        // swap back
        FQueues[P].Free;
        FQueues[P] := TempQueue;
        TempQueue := nil;
      finally
        TempQueue.Free;
      end;
    finally
      FQueueLocks[P].Leave;
    end;
  end;
end;

class procedure TCommonThreadPool.CancelByOwner(Owner: UIntPtr);
begin
  GetInstance._CancelByOwner(Owner);
end;

{ ================= Thread Pool ================= }

constructor TCommonThreadPool.Create(WorkerCount: Integer);
var
  I, P: Integer;
begin
  FTaskKinds := TDictionary<Integer, TTaskKindConfig>.Create;
  FQueueEvent := TEvent.Create(nil, False, False, '');

  for P := 0 to PRIORITY_MAX do begin
    FQueues[P] := TQueue<IThreadTask>.Create;
    FQueueLocks[P] := TCriticalSection.Create;
  end;

  FKeyLocks := TDictionary<UIntPtr, TLightweightMREW>.Create;
  FKeyLockCS := TCriticalSection.Create;
  FWorkerCS := TCriticalSection.Create;

  FMetrics := TDictionary<Integer, PThreadPoolMetrics>.Create;
  FMetricsCS := TCriticalSection.Create;

  FRateLimiters := TDictionary<Integer, TKindRateLimiter>.Create;
  FRateCS := TCriticalSection.Create;

  FActiveByKind := TDictionary<Integer, Integer>.Create;
  FQuotaByKind := TDictionary<Integer, Integer>.Create;
  FQuotaCS := TCriticalSection.Create;

  SetLength(FWorkers, WorkerCount);
  for I := 0 to WorkerCount - 1 do
    FWorkers[I] := TWorker.Create(Self, I);
end;

destructor TCommonThreadPool.Destroy;
var
  W: TWorker;
  I: Integer;
  P: PThreadPoolMetrics;
begin
  FStop := True;
  FQueueEvent.SetEvent;

  for W in FWorkers do begin
    W.Terminate;
    W.WaitFor;
    W.Free;
  end;

  for I := 0 to PRIORITY_MAX do begin
    FQueues[I].Free;
    FQueueLocks[I].Free;
  end;

  FMetricsCS.Enter;
  try
    for P in FMetrics.Values do
      Dispose(P);
    FMetrics.Free;
  finally
    FMetricsCS.Leave;
  end;
  FMetricsCS.Free;

  FRateLimiters.Free;
  FRateCS.Free;

  FActiveByKind.Free;
  FQuotaByKind.Free;
  FQuotaCS.Free;

  FQueueEvent.Free;
  FTaskKinds.Free;
  FKeyLocks.Free;
  FKeyLockCS.Free;
  FWorkerCS.Free;
  inherited;
end;

function TCommonThreadPool.DumpThreadPoolStats: string;
var
  Kind: Integer;
  Cfg: TTaskKindConfig;
  M: PThreadPoolMetrics;
begin
  Result := '';
  for Kind in FTaskKinds.Keys do begin
    Cfg := FTaskKinds[Kind];
    if FMetrics.TryGetValue(Kind, M) then begin
      if Result = '' then
        Result :=
            Format('[%s] Q:%d Run:%d Done:%d Fail:%d', [Cfg.Name, M^.QueueDepth, M^.Started, M^.Completed, M^.Failed])
      else
        Result :=
            Result
                + sLineBreak
                + Format(
                    '[%s] Q:%d Run:%d Done:%d Fail:%d',
                    [Cfg.Name, M^.QueueDepth, M^.Started, M^.Completed, M^.Failed]);
    end;
  end;
end;

procedure TCommonThreadPool.Enqueue(const Task: IThreadTask);
var
  P, Kind: Integer;
  PriorityValue: Byte;
  Metrics: PThreadPoolMetrics;
begin
  if Task = nil then
    Exit;

  Kind := GetKind(Task);
  if not IsKindRegistered(Kind) then
    raise Exception.CreateFmt('Task kind %d is not registered', [Kind]);

  // Ensure metrics entry exists
  FMetricsCS.Enter;
  try
    if not FMetrics.TryGetValue(Kind, Metrics) then begin
      New(Metrics);
      FillChar(Metrics^, SizeOf(TThreadPoolMetrics), 0);
      FMetrics.Add(Kind, Metrics);
    end;
  finally
    FMetricsCS.Leave;
  end;

  // Rate limiting check
  if not AllowByRate(Kind) then begin
    //    Inc(Metrics.Throttled);
    IncMetric(Kind, FMetrics[Kind].Throttled);
    Exit;
  end;

  // Validate priority
  PriorityValue := Task.Priority;
  if PriorityValue > PRIORITY_MAX then
    P := PRIORITY_MAX
  else
    P := PriorityValue;

  // Enqueue task
  FQueueLocks[P].Enter;
  try
    FQueues[P].Enqueue(Task);
    //    Inc(Metrics.QueueDepth);
    //    Inc(Metrics.Enqueued);
    IncMetric(Kind, FMetrics[Kind].QueueDepth);
    IncMetric(Kind, FMetrics[Kind].Enqueued);
  finally
    FQueueLocks[P].Leave;
  end;

  FQueueEvent.SetEvent;
end;

class procedure TCommonThreadPool.EnqueueProc(
    const Proc: TSimpleTaskProc;
    APriority: Byte = 0;
    AKind: Integer = 0;
    ATaskOwner: UIntPtr = 0;
    AKey: UIntPtr = 0;
    ADeadlineTick: UInt64 = 0;
    ACanRetry: Boolean = False
);
begin
  EnqueueTask(TAnonymousThreadTask.Create(Proc, APriority, AKind, ATaskOwner, AKey, ADeadlineTick, ACanRetry));
end;

class procedure TCommonThreadPool.EnqueueTask(const Task: IThreadTask);
begin
  GetInstance.Enqueue(Task);
end;

function TCommonThreadPool.DequeueTask: IThreadTask;
var
  P, i: Integer;
begin
  Result := nil;
  for P := PRIORITY_MAX downto 0 do begin
    // for i := 1 to (1 shl P) do begin
    FQueueLocks[P].Enter;
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Exit;
      end;
    finally
      FQueueLocks[P].Leave;
    end;
    // end;
  end;
end;

class function TCommonThreadPool.GetInstance: TCommonThreadPool;
begin
  Result := FInstance;
end;

function TCommonThreadPool.GetKind(const Task: IThreadTask): Integer;
begin
  Result := Task.Kind
end;

function TCommonThreadPool.GetMetricsByKind: TDictionary<Integer, TThreadPoolMetrics>;
var
  K: Integer;
  P: PThreadPoolMetrics;
begin
  Result := TDictionary<Integer, TThreadPoolMetrics>.Create;

  FMetricsCS.Enter;
  try
    for K in FMetrics.Keys do begin
      P := FMetrics[K];
      Result.Add(K, P^); // copy
    end;
  finally
    FMetricsCS.Leave;
  end;

end;

function TCommonThreadPool.GetMetricsForKind(Kind: Integer; out Metrics: TThreadPoolMetrics): Boolean;
var
  P: PThreadPoolMetrics;
begin
  Result := False;
  FillChar(Metrics, SizeOf(Metrics), 0);

  FMetricsCS.Enter;
  try
    if FMetrics.TryGetValue(Kind, P) then begin
      Metrics := P^;
      Result := True;
    end;
  finally
    FMetricsCS.Leave;
  end;
end;

function TCommonThreadPool.GetMetricsSummary: TThreadPoolMetrics;
var
  P: PThreadPoolMetrics;
begin
  FillChar(Result, SizeOf(Result), 0);

  FMetricsCS.Enter;
  try
    for P in FMetrics.Values do
      AddMetrics(Result, P^);
  finally
    FMetricsCS.Leave;
  end;
end;

class function TCommonThreadPool.GetWorkerStats: TThreadPoolWorkerStats;
begin
  Result := GetInstance._GetWorkerStats;
end;

procedure TCommonThreadPool.EnterKey(Key: UIntPtr);
var
  L: TLightweightMREW;
begin
  FKeyLockCS.Enter;
  try
    if not FKeyLocks.TryGetValue(Key, L) then begin
      L := Default(TLightweightMREW);
      FKeyLocks.Add(Key, L);
    end;
  finally
    FKeyLockCS.Leave;
  end;
  L.BeginWrite;
end;

procedure TCommonThreadPool.LeaveKey(Key: UIntPtr);
var
  L: TLightweightMREW;
begin
  FKeyLockCS.Enter;
  try
    if FKeyLocks.TryGetValue(Key, L) then
      L.EndWrite;
  finally
    FKeyLockCS.Leave;
  end;
end;

procedure TCommonThreadPool.AddMetrics(var Dst: TThreadPoolMetrics; const Src: TThreadPoolMetrics);
begin
  Inc(Dst.Enqueued, Src.Enqueued);
  Inc(Dst.Started, Src.Started);
  Inc(Dst.Completed, Src.Completed);
  Inc(Dst.Failed, Src.Failed);
  Inc(Dst.Retried, Src.Retried);
  Inc(Dst.Canceled, Src.Canceled);
  Inc(Dst.Throttled, Src.Throttled);
  Inc(Dst.QuotaDenied, Src.QuotaDenied);
  Inc(Dst.Expired, Src.Expired);
  Inc(Dst.QueueDepth, Src.QueueDepth);
  Inc(Dst.ExecTimeUsTotal, Src.ExecTimeUsTotal);
end;

function TCommonThreadPool.AllowByRate(Kind: Integer): Boolean;
var
  L: TKindRateLimiter;
  Now: UInt64;
begin
  Result := True;
  FRateCS.Enter;
  try
    if not FRateLimiters.TryGetValue(Kind, L) then
      Exit;

    if (L.Capacity <= 0) or (L.RefillPerSec <= 0) then
      Exit; // explicitly disabled

    Now := GetTickCount64;
    Inc(L.Tokens, ((Now - L.LastTick) * L.RefillPerSec) div 1000);
    if L.Tokens > L.Capacity then
      L.Tokens := L.Capacity;

    L.LastTick := Now;

    if L.Tokens <= 0 then
      Result := False
    else
      Dec(L.Tokens);

    FRateLimiters[Kind] := L;
  finally
    FRateCS.Leave;
  end;
end;

function TCommonThreadPool.AvgExecTimeUs(const M: TThreadPoolMetrics): Double;
begin
  if M.Completed = 0 then
    Result := 0
  else
    Result := M.ExecTimeUsTotal / M.Completed;
end;

function TCommonThreadPool.TryEnterKind(Kind: Integer): Boolean;
var
  Active, Quota: Integer;
begin
  Result := True;
  FQuotaCS.Enter;
  try
    if not FQuotaByKind.TryGetValue(Kind, Quota) then
      Exit;

    Active := FActiveByKind[Kind];
    if Active >= Quota then
      Exit(False);

    FActiveByKind[Kind] := Active + 1;
  finally
    FQuotaCS.Leave;
  end;
end;

procedure TCommonThreadPool._CancelByOwner(Owner: UIntPtr);
var
  P: Integer;
  Task: IThreadTask;
  Tmp: TQueue<IThreadTask>;
begin
  if Owner = 0 then
    Exit;

  for P := 0 to PRIORITY_MAX do begin
    FQueueLocks[P].Enter;
    try
      Tmp := TQueue<IThreadTask>.Create;
      try
        while FQueues[P].Count > 0 do begin
          Task := FQueues[P].Dequeue;
          if Task.Owner = Owner then begin
            Task.Cancel;
            IncMetric(Task.Kind, FMetrics[Task.Kind].Canceled);
          end
          else
            Tmp.Enqueue(Task);
        end;

        while Tmp.Count > 0 do
          FQueues[P].Enqueue(Tmp.Dequeue);
      finally
        Tmp.Free;
      end;
    finally
      FQueueLocks[P].Leave;
    end;
  end;

end;

function TCommonThreadPool._GetWorkerStats: TThreadPoolWorkerStats;
begin
  FWorkerCS.Enter;
  try
    Result.TotalWorkers := FWorkersTotal;
    Result.ActiveWorkers := FWorkersActive;
    Result.IdleWorkers := FWorkersIdle;
    Result.BaseWorkers := FBaseCount;
    Result.DynamicWorkers := FDynamicCount;
    Result.BurstWorkers := FBurstCount;
  finally
    FWorkerCS.Leave;
  end;
end;

procedure TCommonThreadPool._RegisterTaskKind(
    AKind: Integer;
    const AName: string;
    MaxWorkers: Integer;
    RateCapacity, RateRefillPerSec: Int64
);
var
  Cfg: TTaskKindConfig;
begin
  Cfg.Name := AName;
  Cfg.MaxWorkers := MaxWorkers;
  Cfg.RateCapacity := RateCapacity;
  Cfg.RateRefillPerSec := RateRefillPerSec;

  // store config
  FTaskKinds.AddOrSetValue(AKind, Cfg);

  // apply runtime controls
  SetKindQuota(AKind, MaxWorkers);
  SetRateLimit(AKind, RateCapacity, RateRefillPerSec);

end;

procedure TCommonThreadPool.LeaveKind(Kind: Integer);
var
  V: Integer;
begin
  FQuotaCS.Enter;
  try
    if not FActiveByKind.TryGetValue(Kind, V) then
      Exit;

    Dec(V);
    if V < 0 then
      V := 0;

    FActiveByKind[Kind] := V;
  finally
    FQuotaCS.Leave;
  end;
end;

procedure TCommonThreadPool.SetRateLimit(Kind: Integer; Capacity, RefillPerSec: Int64);
var
  L: TKindRateLimiter;
begin
  FRateCS.Enter;
  try
    // Disable rate limiting
    if (Capacity <= 0) or (RefillPerSec <= 0) then begin
      FRateLimiters.Remove(Kind);
      Exit;
    end;

    // Enable / update rate limiting
    L.Capacity := Capacity;
    L.Tokens := Capacity; // start full
    L.RefillPerSec := RefillPerSec;
    L.LastTick := GetTickCount64;

    FRateLimiters.AddOrSetValue(Kind, L);
  finally
    FRateCS.Leave;
  end;
end;

procedure TCommonThreadPool.SetKindQuota(Kind: Integer; MaxWorkers: Integer);
begin
  FQuotaCS.Enter;
  try
    if MaxWorkers <= 0 then begin
      // unlimited
      FQuotaByKind.Remove(Kind);
      FActiveByKind.Remove(Kind);
      Exit;
    end;

    FQuotaByKind.AddOrSetValue(Kind, MaxWorkers);

    // Ensure active counter exists
    if not FActiveByKind.ContainsKey(Kind) then
      FActiveByKind.Add(Kind, 0);
  finally
    FQuotaCS.Leave;
  end;
end;

procedure TCommonThreadPool.IncMetric(Kind: Integer; var Field: Int64; Delta: Int64);
begin
  FMetricsCS.Enter;
  try
    if not FMetrics.ContainsKey(Kind) then
      FMetrics.Add(Kind, New(PThreadPoolMetrics));
    Inc(Field, Delta);
  finally
    FMetricsCS.Leave;
  end;
end;

function TCommonThreadPool.IsKindRegistered(AKind: Integer): Boolean;
begin
  Result := FTaskKinds.ContainsKey(AKind);
end;

function TCommonThreadPool.MetricsSnapshot: TDictionary<Integer, PThreadPoolMetrics>;
begin
  FMetricsCS.Enter;
  try
    Result := TDictionary<Integer, PThreadPoolMetrics>.Create(FMetrics);
  finally
    FMetricsCS.Leave;
  end;
end;

class procedure TCommonThreadPool.RegisterTaskKind(
    AKind: Integer;
    const AName: string;
    MaxWorkers: Integer;
    RateCapacity, RateRefillPerSec: Int64
);
var
  Cfg: TTaskKindConfig;
begin
  GetInstance._RegisterTaskKind(AKind, AName, MaxWorkers, RateCapacity, RateRefillPerSec);
end;

{ ================= Anonymous Thread Task ================= }

constructor TAnonymousThreadTask.Create(
    const AProc: TSimpleTaskProc;
    APriority: Byte;
    AKind: Integer;
    AOwner: UIntPtr;
    AKey: UIntPtr;
    ADeadlineTick: UInt64;
    ACanRetry: Boolean
);
begin
  inherited Create;
  FProc := AProc;
  FPriority := APriority;
  FKind := AKind;
  FOwner := AOwner;
  FKey := AKey;
  FDeadlineTick := ADeadlineTick;
  FCanRetry := ACanRetry;
  FDone := False;
end;

function TAnonymousThreadTask.DeadlineTick: UInt64;
begin
  Result := FDeadlineTick;
end;

procedure TAnonymousThreadTask.Execute;
begin
  if FCanceled then
    Exit;
  FProc();
  FDone := True;
end;

function TAnonymousThreadTask.TaskDone: Boolean;
begin
  Result := FDone;
end;

// function TAnonymousThreadTask._AddRef: Integer;
// begin
//  Result := -1; // disable reference counting
// end;
//
// function TAnonymousThreadTask._Release: Integer;
// begin
//  Result := -1; // disable reference counting
// end;

procedure TAnonymousThreadTask.Cancel;
begin
  FCanceled := True;
end;

function TAnonymousThreadTask.IsCancelled: Boolean;
begin
  Result := FCanceled;
end;

procedure TAnonymousThreadTask.Cleanup;
begin
end;

function TAnonymousThreadTask.Key: UIntPtr;
begin
  Result := FKey;
end;

function TAnonymousThreadTask.Priority: Byte;
begin
  Result := FPriority;
end;

function TAnonymousThreadTask.Kind: Integer;
begin
  Result := FKind;
end;

function TAnonymousThreadTask.Owner: UIntPtr;
begin
  Result := FOwner;
end;

function TAnonymousThreadTask.CanRetry: Boolean;
begin
  Result := FCanRetry;
end;

{ ================= Initialization / Finalization ================= }

initialization
  if not Assigned(TCommonThreadPool.FInstance) then
    TCommonThreadPool.FInstance := TCommonThreadPool.Create(1);

finalization
  FreeAndNil(TCommonThreadPool.FInstance);

end.
