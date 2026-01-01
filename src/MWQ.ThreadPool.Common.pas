unit MWQ.ThreadPool.Common;

interface

uses
  System.Classes,
  System.SysUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Winapi.Windows;

const
  PRIORITY_MAX = 7;

type
  TWorkerKind = (wkBase, wkDynamic, wkBurst, wkQuota);
  TWorkerState = (wsIdle, wsBusy, wsStopping);

  TWorkerInfo = record
    Kind: TWorkerKind;
    LastActiveTick: UInt64;
  end;

  TTaskKindConfig = record
    Name: string;
    MaxWorkers: Integer;
    RateCapacity: Int64;
    RateRefillPerSec: Int64;
  end;

  TTaskStatus = (tsPending, tsRunning, tsSucceeded, tsFailed, tsCanceled);

  TTaskResult = (trSucceeded, trFailed, trCanceled);

  TSimpleTaskFunc = reference to function: Boolean;
  TMetricKind = (mkEnqueued, mkStarted, mkCompleted, mkFailed, mkExpired);

  IThreadTask = interface
    ['{E43A9C91-0B5F-4D99-AF13-7EAC4D4F0001}']
    function Key: UIntPtr;
    function Owner: UIntPtr;
    function Kind: Integer;
    function Priority: Byte;
    function DeadlineTick: UInt64;
    procedure Run;
    function TaskDone: Boolean;
    function CanRetry: Boolean;
    procedure Cancel;
    function IsCancelled: Boolean;
    procedure Cleanup;
    function Context: TObject;
  end;

  TThreadPoolMetrics = record
    Enqueued: Int64;
    Started: Int64;
    Completed: Int64;
    Failed: Int64;
    Retried: Int64;
    Canceled: Int64;
    Dropped: Int64;
    Throttled: Int64;
    QuotaDenied: Int64;
    Expired: Int64;
    QueueDepth: Int64; // FIXED: tasks still in queue
    Requeued: Int64;
    QuotaQueue: Int64;
    ExecTimeUsTotal: Int64;
  end;

  TThreadPoolWorkerStats = record
    MinWorkers, MaxWorkers: Integer;
    TotalWorkers: Integer;
    BusyWorkers: Integer;
    IdleWorkers: Integer;
    BaseWorkers: Integer;
    QuotaWorkers: Integer;
    DynamicWorkers: Integer;
    BurstWorkers: Integer;
  end;

  PThreadPoolMetrics = ^TThreadPoolMetrics;

  TKindRateLimiter = record
    Capacity: Int64;
    Tokens: Int64;
    RefillPerSec: Int64;
    LastTick: UInt64;
  end;

  TOnTaskFinished =
      procedure(
          const ATask: IThreadTask;
          const AResult: TTaskResult;
          const ARetryCount: Integer;
          const AExecTimeUs: UInt64
      ) of object;

  TCommonThreadPool = class
  private
    class var
      FInstance: TCommonThreadPool;

  private
    type
      TWorker = class(TThread)
      private
        FOwner: TCommonThreadPool;
        FState: TWorkerState;
        FKind: TWorkerKind;
        FLastActiveTick: UInt64;
        procedure SetState(AState: TWorkerState);
      protected
        procedure Execute; override;
      public
        constructor Create(AOwner: TCommonThreadPool);
      public
        property State: TWorkerState read FState write FState;
        property LastActiveTick: UInt64 read FLastActiveTick;
      end;

  private
    FTaskKinds: TDictionary<Integer, TTaskKindConfig>;
    FBurstWorkers: TArray<TWorker>;
    FWorkers: TArray<TWorker>;
    FStop: Boolean;

    FQueues: array[0..PRIORITY_MAX] of TQueue<IThreadTask>;
    FQueueLocks: array[0..PRIORITY_MAX] of TCriticalSection;
    FQueueEvent: TEvent;
    FQueueDepth: Integer;
    FQueueQuota: Integer;

    FKeyLocks: TDictionary<UIntPtr, TLightweightMREW>;
    FKeyLockCS: TCriticalSection;

    FMetrics: TDictionary<Integer, PThreadPoolMetrics>;
    FMetricsCS: TCriticalSection;

    FRateLimiters: TDictionary<Integer, TKindRateLimiter>;
    FRateCS: TCriticalSection;

    FActiveByKind: TDictionary<Integer, Integer>;
    FQuotaByKind: TDictionary<Integer, Integer>;
    FQuotaCS: TCriticalSection;
    FQuotaQueues: TDictionary<Integer, TQueue<IThreadTask>>;
    FQuotaQueueCS: TDictionary<Integer, TCriticalSection>;

    FMinWorkers: Integer;
    FMaxWorkers: Integer;
    FBurstLimit: Integer;
    FBurstIdleTimeoutMs: Cardinal;
    FIdleTimeoutMs: Cardinal;

    FWorkersBurst: Integer;
    FWorkersTotal: Integer;
    FWorkersBusy: Integer;
    FWorkersIdle: Integer;

    FBaseCount, FDynamicCount, FBurstCount, FQuotaCount: Integer;
    FWorkerCS: TCriticalSection;
    FDropTaskOnThrottle: Boolean;
    FMaxRetry: Integer;

    FOnTaskFinished: TOnTaskFinished;

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
    procedure EnqueueRunnableTask(const Task: IThreadTask);
    function DequeueRunnableTask: IThreadTask;
    function DequeueRunnableTaskMinPriority(MinPriority: Integer): IThreadTask;
    function TryTakeQuotaTaskAny: IThreadTask;
    function TryTakeQuotaTaskByKind(Kind: Integer): IThreadTask;
    procedure EnqueueQuotaTask(const Task: IThreadTask);
    procedure OnBurstWorkerExit(Worker: TWorker);
    function HasKindQuota(Kind: Integer): Boolean;
    procedure CheckScale;
    procedure SpawnWorker;
    procedure RetireIdleWorker;
    procedure SpawnBurstWorker;
    procedure DoTaskFinished(
        const ATask: IThreadTask;
        const AResult: TTaskResult;
        const ARetryCount: Integer;
        const AExecTimeUs: UInt64
    );
  protected
  public
    class function GetInstance: TCommonThreadPool;

    class procedure EnqueueTask(const Task: IThreadTask); static;
    class procedure EnqueueProc(
        const AFunc: TFunc<Boolean>;
        APriority: Byte = 0;
        AKind: Integer = 0;
        ATaskOwner: UIntPtr = 0;
        AKey: UIntPtr = 0;
        ADeadlineTick: UInt64 = 0;
        ACanRetry: Boolean = False;
        AContext: TObject = nil
    ); static;
    class procedure RegisterTaskKind(
        AKind: Integer;
        const AName: string;
        MaxWorkers: Integer;
        RateCapacity, RateRefillPerSec: Int64
    ); static;
    class procedure CancelByOwner(Owner: UIntPtr); static;
    class function GetWorkerStats: TThreadPoolWorkerStats;

  public
    constructor Create(WorkerCount: Integer);
    destructor Destroy; override;

    procedure Enqueue(const Task: IThreadTask; const IsRequeue: Boolean = False);

    procedure SetRateLimit(Kind: Integer; Capacity, RefillPerSec: Int64);
    procedure SetKindQuota(Kind: Integer; MaxWorkers: Integer);

    function MetricsSnapshot: TDictionary<Integer, PThreadPoolMetrics>;
    procedure CancelByKey(Key: UIntPtr);
    procedure AddMetrics(var Dst: TThreadPoolMetrics; const Src: TThreadPoolMetrics);
    function GetMetricsSummary: TThreadPoolMetrics;
    function GetMetricsByKind: TDictionary<Integer, TThreadPoolMetrics>;
    function GetKindConfig(const AKind: Integer; var KindCfg: TTaskKindConfig): Boolean;
    function GetMetricsForKind(Kind: Integer; out Metrics: TThreadPoolMetrics): Boolean;
    function AvgExecTimeUs(const M: TThreadPoolMetrics): Double;
    function DumpThreadPoolStats: string;

    property QueueDepth: Integer read FQueueDepth;
    property BurstLimit: Integer read FBurstLimit write FBurstLimit;
    property IdleTimeoutMs: Cardinal read FIdleTimeoutMs write FIdleTimeoutMs;
    property MaxRetry: Integer read FMaxRetry write FMaxRetry;
    {**
  FDropTaskOnThrottle:

  Controls behavior when a task is rejected by rate limiting.

  When FALSE (DEFAULT, SAFE):
    - Tasks are NEVER dropped.
    - Rate-limited tasks will be delayed or re-queued.
    - Guarantees at-least-once execution.
    - QueueDepth and metrics remain accurate.
    - Suitable for business logic, persistence, messaging, and trading systems.

  When TRUE (DANGEROUS):
    - Tasks rejected by rate limiting are SILENTLY DROPPED.
    - Dropped tasks are NOT executed, NOT retried, and NOT queued.
    - QueueDepth will NOT reflect dropped tasks.
    - Can cause data loss, missing jobs, and inconsistent system state.
    - Use ONLY for best-effort, fire-and-forget workloads
      (e.g. telemetry, statistics sampling, debug logging).

  ⚠ WARNING:
    Enabling this flag changes throttling into dropping.
    This can look like worker starvation or deadlock during debugging.

  ⚠ NEVER enable this for:
    - Financial transactions
    - Database writes
    - Message delivery
    - User-visible actions
    - Any task requiring reliability

  Default: FALSE
**}
    property DropTaskOnThrottle: Boolean read FDropTaskOnThrottle write FDropTaskOnThrottle;
    property OnTaskFinished: TOnTaskFinished read FOnTaskFinished write FOnTaskFinished;
  end;

  TAnonymousThreadTask = class(TInterfacedObject, IThreadTask)
  private
    FSuccess: Boolean;
    FFunc: TFunc<Boolean>;
    FPriority: Byte;
    FKey: UIntPtr;
    FKind: Integer;
    FOwner: Integer;
    FCanceled: Boolean;
    FCanRetry: Boolean;
    FDone: Boolean;
    FDeadlineTick: UInt64;
    FContext: TObject;
  public
    constructor Create(
        const AFunc: TFunc<Boolean>;
        APriority: Byte;
        AKind: Integer;
        AOwner: UIntPtr;
        AKey: UIntPtr;
        ADeadlineTick: UInt64;
        ACanRetry: Boolean;
        AContext: TObject
    );
    destructor Destroy; override;
    function Key: UIntPtr;
    function Owner: UIntPtr;
    function Priority: Byte;
    function DeadlineTick: UInt64;
    procedure Run;
    function TaskDone: Boolean;
    function CanRetry: Boolean;
    procedure Cancel;
    function IsCancelled: Boolean;
    procedure Cleanup;
    function Kind: Integer;
    function Context: TObject;
  end;

  TMWQThreadPool = TCommonThreadPool;

function BackoffMs(Retry: Integer): Integer;

implementation

uses
  Quick.Logger,
  System.Math;

// ---------------------------- UTILITIES ----------------------------

function BackoffMs(Retry: Integer): Integer;
begin
  Result := Min(5 + Retry * Retry * 10, 500);
end;

function GetLogicalCPUCount: Integer;
begin
  Result := GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
end;

// ---------------------------- WORKER ----------------------------

constructor TCommonThreadPool.TWorker.Create(AOwner: TCommonThreadPool);
begin
  inherited Create(False);
  FreeOnTerminate := False;
  FOwner := AOwner;
  FState := wsIdle;
  FKind := wkBase;
end;

procedure TCommonThreadPool.TWorker.SetState(AState: TWorkerState);
begin
  if FState = AState then
    Exit;

  FOwner.FWorkerCS.Enter;
  try
    if FState = wsIdle then
      Dec(FOwner.FWorkersIdle)
    else
      Dec(FOwner.FWorkersBusy);

    if AState = wsIdle then
      Inc(FOwner.FWorkersIdle)
    else
      Inc(FOwner.FWorkersBusy);

    FState := AState;
    if FState = wsIdle then begin
      if Self.FKind = wkQuota then begin
        Self.FKind := wkBase;
        Dec(FOwner.FQuotaCount);
        Inc(FOwner.FBaseCount);
      end;
    end;
  finally
    FOwner.FWorkerCS.Leave;
  end;
end;

{$IFDEF DEBUG}
function WorkerKindToStr(K: TWorkerKind): string;
begin
  case K of
    wkBase: Result := 'Base';
    wkQuota: Result := 'Quota';
    wkBurst: Result := 'Burst';
  else
    Result := 'Unknown';
  end;
end;
{$ENDIF}

procedure TCommonThreadPool.TWorker.Execute;
var
  Task: IThreadTask;
  Kind: Integer;
  LRetry: Integer;
  StartTick, Latency: UInt64;
  Tid: Cardinal;
  LResult: TTaskResult;
begin
  Tid := GetCurrentThreadId;
  SetState(wsBusy);

{$IFDEF DEBUG}
  Log(Format('Worker start [TID=%d Kind=%s]', [Tid, WorkerKindToStr(Self.FKind)]), etDebug);
{$ENDIF}

  while not Terminated and not FOwner.FStop do begin
    Task := nil;

    { STEP 1: Runnable task }
    if Self.FKind <> wkQuota then begin
      if Self.FKind = wkBurst then begin
        Task := FOwner.DequeueRunnableTaskMinPriority(5);
        if Task = nil then begin
{$IFDEF DEBUG}
          Log(Format('Worker exit burst [TID=%d]', [Tid]), etDebug);
{$ENDIF}
          Break; // burst worker intentionally exits
        end;
      end
      else
        Task := FOwner.DequeueRunnableTask;
    end;

    { STEP 2: Quota task }
    if Task = nil then begin
      if Self.FKind = wkQuota then
        Task := FOwner.TryTakeQuotaTaskByKind(Kind)
      else
        Task := FOwner.TryTakeQuotaTaskAny;

      if Task <> nil then begin
{$IFDEF DEBUG}
        Log(
            Format(
                'Worker took quota task [TID=%d Worker=%s Kind=%d Key=%x]',
                [Tid, WorkerKindToStr(Self.FKind), Task.Kind, Task.Key]
            ),
            etDebug
        );
{$ENDIF}

        if Self.FKind = wkBase then begin
          Self.FKind := wkQuota;
          Inc(FOwner.FQuotaCount);
          Dec(FOwner.FBaseCount);
        end;
      end;
    end;

    { STEP 3: Wait }
    if Task = nil then begin
{$IFDEF DEBUG}
      Log(Format('Worker idle wait [TID=%d]', [Tid]), etDebug);
{$ENDIF}

      //      FLastActiveTick := GetTickCount64;
      SetState(wsIdle);
      FOwner.FQueueEvent.ResetEvent;
      FOwner.FQueueEvent.WaitFor(INFINITE);
      SetState(wsBusy);
      Continue;
    end;

    { STEP 4: Cancelled }
    if Task.IsCancelled then begin
{$IFDEF DEBUG}
      Log(Format('Task cancelled skip [TID=%d Kind=%d Key=%x]', [Tid, Task.Kind, Task.Key]), etDebug);
{$ENDIF}
      FOwner.DoTaskFinished(Task, TTaskResult.trCanceled, 0, 0);
      Continue;
    end;

    Kind := Task.Kind;

    { STEP 5: Deadline }
    if (Task.DeadlineTick > 0) and (GetTickCount64 > Task.DeadlineTick) then begin
{$IFDEF DEBUG}
      Log(Format('Task expired [TID=%d Kind=%d Key=%x]', [Tid, Kind, Task.Key]), etDebug);
{$ENDIF}

      Task.Cancel;
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Expired);
      FOwner.DoTaskFinished(Task, TTaskResult.trCanceled, 0, 0);
      Continue;
    end;

    //    { Rate limited }
    //    if not FOwner.AllowByRate(Kind) then begin
    //      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Throttled);
    //
    // {$IFDEF DEBUG}
    //      Log(Format('Rate limited → requeue [TID=%d Kind=%d Key=%x]', [GetCurrentThreadId, Kind, Task.Key]), etDebug);
    // {$ENDIF}
    //
    //      FOwner.Enqueue(Task, True);
    //      Sleep(1);
    //      Continue;
    //    end;

    { STEP 6: Quota }
    if (Self.FKind = wkQuota) or FOwner.HasKindQuota(Kind) then begin
      if not FOwner.TryEnterKind(Kind) then begin
{$IFDEF DEBUG}
        Log(Format('Quota denied → requeue [TID=%d Kind=%d Key=%x]', [Tid, Kind, Task.Key]), etDebug);
{$ENDIF}

        FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].QuotaDenied);
        FOwner.EnqueueQuotaTask(Task);

        // Quota Full. Change from wkQuota to wkBase;
        if Self.FKind = wkQuota then begin
          Self.FKind := wkBase;
          Dec(FOwner.FQuotaCount);
          Inc(FOwner.FBaseCount);
        end;

        Continue; // ✅ IMPORTANT: continue, never break
      end;

      // Deal Quota, Change from wkBase to wkQuota;
      if Self.FKind = wkBase then begin
        Self.FKind := wkQuota;
        Inc(FOwner.FQuotaCount);
        Dec(FOwner.FBaseCount);
      end;
    end;

    { STEP 7: Execute }
{$IFDEF DEBUG}
    Log(
        Format('Execute task [TID=%d Worker=%s Kind=%d Key=%x]', [Tid, WorkerKindToStr(Self.FKind), Kind, Task.Key]),
        etDebug
    );
{$ENDIF}

    FOwner.EnterKey(Task.Key);
    try
      LRetry := 0;
      StartTick := GetTickCount64;
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Started);

      while True do begin
        Task.Run;

        if Task.TaskDone then begin
          if Task.IsCancelled then
            LResult := TTaskResult.trCanceled
          else
            LResult := TTaskResult.trSucceeded;
          Break;
        end;

        if not Task.CanRetry or (LRetry >= FOwner.FMaxRetry) then begin
          FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Failed);
          LResult := TTaskResult.trFailed;
          Break;
        end;

        Inc(LRetry);
        FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Retried);
        Sleep(BackoffMs(LRetry));
      end;

      Latency := (GetTickCount64 - StartTick) * 1000;
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].ExecTimeUsTotal, Latency);

      //      if Task.TaskDone then
      FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].Completed);

      FOwner.DoTaskFinished(Task, LResult, LRetry, Latency);

    finally
      Task.Cleanup;
      FOwner.LeaveKey(Task.Key);

      if Self.FKind = wkQuota then begin
        FOwner.LeaveKind(Kind);

        if FOwner.FWorkersIdle <= 0 then begin // When FWorkersIdle = 0, Try Next task from runnable queue firstly.
          Self.FKind := wkBase;
          Dec(FOwner.FQuotaCount);
          Inc(FOwner.FBaseCount);
        end;
      end;
      FLastActiveTick := GetTickCount64;
      Self.FOwner.CheckScale;
    end;
  end;

  SetState(wsStopping);

{$IFDEF DEBUG}
  Log(Format('Worker exit [TID=%d]', [Tid]), etDebug);
{$ENDIF}

  if Self.FKind = wkBurst then
    FOwner.OnBurstWorkerExit(Self);
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

procedure TCommonThreadPool.CheckScale;
begin
  // Scale UP
  if ((FQueueDepth - FQueueQuota) > FBaseCount * 2) and (FWorkersTotal < FMaxWorkers) then
    SpawnWorker;

  // Scale DOWN
  if (FWorkersIdle > 0) and (FWorkersTotal > FMinWorkers) then
    RetireIdleWorker;
end;

{ ================= Thread Pool ================= }

constructor TCommonThreadPool.Create(WorkerCount: Integer);
var
  I, P: Integer;
  CPU: Integer;
begin
  FTaskKinds := TDictionary<Integer, TTaskKindConfig>.Create;
  FQueueEvent := TEvent.Create(nil, True, False, '');

  FDropTaskOnThrottle := False;

  FQuotaQueues := TDictionary<Integer, TQueue<IThreadTask>>.Create;
  FQuotaQueueCS := TDictionary<Integer, TCriticalSection>.Create;

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

  CPU := System.CPUCount;
  if CPU < 1 then
    CPU := 1;
  FMinWorkers := Max(1, CPU div 2);
  FMaxWorkers := CPU * 2;

  FBurstLimit := Max(2, CPU div 2);
  FIdleTimeoutMs := 60_000; // 1 minutes idle timeout
  FMaxRetry := 2;

  // CLAMP WorkerCount HERE
  WorkerCount := EnsureRange(WorkerCount, FMinWorkers, FMaxWorkers);

  SetLength(FWorkers, WorkerCount);
  FWorkersIdle := Length(FWorkers);
  FWorkersTotal := 0;
  FBaseCount := 0;
  for I := 0 to WorkerCount - 1 do begin
    Inc(FWorkersTotal);
    Inc(FBaseCount);
    FWorkers[I] := TWorker.Create(Self);
    FWorkers[I].FreeOnTerminate := false;
  end;
end;

function TCommonThreadPool.DequeueRunnableTask: IThreadTask;
var
  P: Integer;
begin
  Result := nil;

  for P := PRIORITY_MAX downto 0 do begin
    FQueueLocks[P].Enter;
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Dec(FQueueDepth);
        Exit;
      end;
    finally
      FQueueLocks[P].Leave;
    end;
  end;
end;

function TCommonThreadPool.DequeueRunnableTaskMinPriority(MinPriority: Integer): IThreadTask;
var
  P, Kind: Integer;
begin
  Result := nil;

  for P := PRIORITY_MAX downto MinPriority do begin
    FQueueLocks[P].Enter;
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Kind := GetKind(Result);
        IncMetric(Kind, FMetrics[Kind].QueueDepth, -1); // ✅ FIX
        Exit;
      end;
    finally
      FQueueLocks[P].Leave;
    end;
  end;
end;

// ---------------------------- DEQUEUE TASK (FIX QUEUEDEPTH) ----------------------------

function TCommonThreadPool.DequeueTask: IThreadTask;
var
  P, Kind: Integer;
begin
  Result := nil;
  for P := PRIORITY_MAX downto 0 do begin
    FQueueLocks[P].Enter;
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Kind := GetKind(Result);
        IncMetric(Kind, FMetrics[Kind].QueueDepth, -1); // ✅ FIX
        Exit;
      end;
    finally
      FQueueLocks[P].Leave;
    end;
  end;
end;
// ---------------------------- TAnonymousThreadTask ----------------------------

constructor TAnonymousThreadTask.Create(
    const AFunc: TFunc<Boolean>;
    APriority: Byte;
    AKind: Integer;
    AOwner, AKey: UIntPtr;
    ADeadlineTick: UInt64;
    ACanRetry: Boolean;
    AContext: TObject
);
begin
  inherited Create;
  FFunc := AFunc;
  FPriority := APriority;
  FKind := AKind;
  FOwner := AOwner;
  FKey := AKey;
  FDeadlineTick := ADeadlineTick;
  FCanRetry := ACanRetry;
  FDone := False;
  FContext := AContext;
end;

function TAnonymousThreadTask.Key: UIntPtr;
begin
  Result := FKey;
end;

function TAnonymousThreadTask.Owner: UIntPtr;
begin
  Result := FOwner;
end;

function TAnonymousThreadTask.Priority: Byte;
begin
  Result := FPriority;
end;

function TAnonymousThreadTask.DeadlineTick: UInt64;
begin
  Result := FDeadlineTick;
end;

destructor TAnonymousThreadTask.Destroy;
begin
  FContext := nil;
  inherited;
end;

procedure TAnonymousThreadTask.Run;
begin
  if FCanceled then begin
    FSuccess := False;
    Exit;
  end;

  try
    FSuccess := FFunc();
  except
    on E: Exception do begin
      FSuccess := False;
      // store E.Message if needed
    end;
  end;
end;

function TAnonymousThreadTask.TaskDone: Boolean;
begin
  Result := FSuccess or FCanceled;
end;

function TAnonymousThreadTask.CanRetry: Boolean;
begin
  Result := FCanRetry and (not FSuccess) and (not FCanceled);
end;

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
  FFunc := nil; // release closure
end;

function TAnonymousThreadTask.Context: TObject;
begin
  Result := FContext;
end;

function TAnonymousThreadTask.Kind: Integer;
begin
  Result := FKind;
end;

// ---------------------------- Initialization ----------------------------
destructor TCommonThreadPool.Destroy;
var
  W: TWorker;
  I: Integer;
  P: PThreadPoolMetrics;
  Q: TQueue<IThreadTask>;
begin
  // 1. Stop workers
  FStop := True;
  FQueueEvent.SetEvent;

  FWorkerCS.Enter;
  try
    for W in FWorkers do begin
      W.Terminate;
      W.WaitFor;
      W.Free;
    end;
    //    FWorkers.F; // MISSING
  finally
    FWorkerCS.Leave;
  end;
  if length(FBurstWorkers) > 0 then begin
    for W in FBurstWorkers do begin
      W.Terminate;
      W.WaitFor;
      W.Free;
    end;
  end;

  // 2. Free runnable queues and locks
  for I := 0 to PRIORITY_MAX do begin
    FQueues[I].Free;
    FQueueLocks[I].Free;
  end;

  // 3. Free quota queues (🔴 MISSING ENTIRELY)
  FQuotaCS.Enter;
  try
    for Q in FQuotaQueues.Values do
      Q.Free;
    FQuotaQueues.Free;
  finally
    FQuotaCS.Leave;
  end;

  // 4. Free metrics
  FMetricsCS.Enter;
  try
    for P in FMetrics.Values do
      Dispose(P);
    FMetrics.Free;
  finally
    FMetricsCS.Leave;
  end;
  FMetricsCS.Free;

  // 5. Free rate limiters
  FRateLimiters.Free;
  FRateCS.Free;

  // 6. Free quota bookkeeping
  FActiveByKind.Free;
  FQuotaByKind.Free;
  FQuotaCS.Free;

  // 7. Free synchronization + misc
  FQueueEvent.Free;
  FTaskKinds.Free;
  FKeyLocks.Free;
  FKeyLockCS.Free;
  FWorkerCS.Free;

  inherited;
end;

procedure TCommonThreadPool.DoTaskFinished(
    const ATask: IThreadTask;
    const AResult: TTaskResult;
    const ARetryCount: Integer;
    const AExecTimeUs: UInt64
);
var
  LTask: IThreadTask;
begin
  if Assigned(FOnTaskFinished) then begin
    LTask := ATask; // pin interface reference
    TThread.Queue(nil, procedure begin FOnTaskFinished(LTask, AResult, ARetryCount, AExecTimeUs); end);
  end;
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

procedure TCommonThreadPool.Enqueue(const Task: IThreadTask; const IsRequeue: Boolean = False);
var
  P, Kind: Integer;
  PriorityValue: Byte;
  Metrics: PThreadPoolMetrics;
begin
  if Task = nil then begin
{$IFDEF DEBUG}
    Log('ThreadPool.Enqueue skipped: Task=nil', etDebug);
{$ENDIF}
    Exit;
  end;

  Kind := GetKind(Task);

  if not IsKindRegistered(Kind) then begin
{$IFDEF DEBUG}
    Log(
        Format(
            'ThreadPool.Enqueue rejected: Kind not registered (Kind=%d Key=%x Priority=%d)',
            [Kind, Task.Key, Task.Priority]
        ),
        etError
    );
{$ENDIF}
    raise Exception.CreateFmt('Task kind %d is not registered', [Kind]);
  end;

  { Ensure metrics entry exists }
  FMetricsCS.Enter;
  try
    if not FMetrics.TryGetValue(Kind, Metrics) then begin
      New(Metrics);
      FillChar(Metrics^, SizeOf(TThreadPoolMetrics), 0);
      FMetrics.Add(Kind, Metrics);

{$IFDEF DEBUG}
      Log(Format('ThreadPool.Metrics created (Kind=%d)', [Kind]), etDebug);
{$ENDIF}
    end;
  finally
    FMetricsCS.Leave;
  end;

  { Rate limiting }
  if not AllowByRate(Kind) then begin
    IncMetric(Kind, FMetrics[Kind].Throttled);

{$IFDEF DEBUG}
    Log(
        Format(
            'Rate limit hit (Kind=%d Key=%x Priority=%d Drop=%s)',
            [Kind, Task.Key, Task.Priority, BoolToStr(FDropTaskOnThrottle, True)]
        ),
        etDebug
    );
{$ENDIF}

    if FDropTaskOnThrottle then begin
      IncMetric(Kind, FMetrics[Kind].Dropped);
{$IFDEF DEBUG}
      Log(Format('TASK DROPPED due to throttle (Kind=%d Key=%x)', [Kind, Task.Key]), etWarning);
{$ENDIF}
      Exit;
    end;

    // Safe path: requeue / delay
    //    Enqueue(Task);
    //    Sleep(1);
    //    Exit;
  end;

  { Normalize priority }
  PriorityValue := Task.Priority;
  if PriorityValue > PRIORITY_MAX then
    P := PRIORITY_MAX
  else
    P := PriorityValue;

  { Enqueue task }
  FQueueLocks[P].Enter;
  try
    FQueues[P].Enqueue(Task);
    //    IncMetric(Kind, FMetrics[Kind].QueueDepth);
    //    IncMetric(Kind, FMetrics[Kind].Enqueued);
    if not IsRequeue then begin
      IncMetric(Kind, FMetrics[Kind].Enqueued);
      IncMetric(Kind, FMetrics[Kind].QueueDepth);
    end
    else begin
      IncMetric(Kind, FMetrics[Kind].Requeued);
      // QueueDepth unchanged (already decremented)
    end;
  finally
    FQueueLocks[P].Leave;
  end;

{$IFDEF DEBUG}
  Log(
      Format(
          'ThreadPool.Enqueue OK (Kind=%d Key=%x Priority=%d QueuePrio=%d QueueDepth=%d)',
          [Kind, Task.Key, Task.Priority, P, FMetrics[Kind].QueueDepth]
      ),
      etDebug
  );
{$ENDIF}

  { Burst worker }
  if not IsRequeue then begin
    if (Task.Priority >= 5) and (FWorkersIdle = 0) and (FWorkersTotal < FMaxWorkers + FBurstLimit) then begin
{$IFDEF DEBUG}
      Log(
          Format(
              'ThreadPool.SpawnBurstWorker (Reason=HighPriority Kind=%d Key=%x Workers=%d Idle=%d)',
              [Kind, Task.Key, FWorkersTotal, FWorkersIdle]
          ),
          etDebug
      );
{$ENDIF}

      SpawnBurstWorker;
    end
    else begin
      CheckScale;
    end;
  end;

  FQueueEvent.SetEvent;
end;

class procedure TCommonThreadPool.EnqueueProc(
    const AFunc: TFunc<Boolean>;
    APriority: Byte = 0;
    AKind: Integer = 0;
    ATaskOwner: UIntPtr = 0;
    AKey: UIntPtr = 0;
    ADeadlineTick: UInt64 = 0;
    ACanRetry: Boolean = False;
    AContext: TObject = nil
);
begin
  EnqueueTask(
      TAnonymousThreadTask.Create(AFunc, APriority, AKind, ATaskOwner, AKey, ADeadlineTick, ACanRetry, AContext)
  );
end;

procedure TCommonThreadPool.EnqueueQuotaTask(const Task: IThreadTask);
var
  Q: TQueue<IThreadTask>;
  Kind: Integer;
begin
  if Task = nil then
    Exit;

  Kind := GetKind(Task);

  // Ensure the quota queue exists
  FQuotaCS.Enter;
  try
    if not FQuotaQueues.TryGetValue(Kind, Q) then begin
      Q := TQueue<IThreadTask>.Create;
      FQuotaQueues.Add(Kind, Q);
    end;
  finally
    FQuotaCS.Leave;
  end;

  // Enqueue the task into the quota queue
  FQuotaCS.Enter;
  try
    Q.Enqueue(Task);
  finally
    FQuotaCS.Leave;
  end;
end;

procedure TCommonThreadPool.EnqueueRunnableTask(const Task: IThreadTask);
var
  P: Integer;
begin
  if Task = nil then
    Exit;

  P := Task.Priority;

  if (P < 0) or (P > PRIORITY_MAX) then
    P := 0;

  // Enqueue into runnable queue
  FQueueLocks[P].Enter;
  try
    FQueues[P].Enqueue(Task);
    Inc(FQueueDepth); // global runnable depth
  finally
    FQueueLocks[P].Leave;
  end;

  // Wake one sleeping worker
  FQueueEvent.SetEvent;
end;

class procedure TCommonThreadPool.EnqueueTask(const Task: IThreadTask);
begin
  GetInstance.Enqueue(Task);
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

class function TCommonThreadPool.GetInstance: TCommonThreadPool;
begin
  if not Assigned(TCommonThreadPool.FInstance) then
    TCommonThreadPool.FInstance := TCommonThreadPool.Create(4); // default 4 workers

  Result := FInstance;
end;

function TCommonThreadPool.GetKind(const Task: IThreadTask): Integer;
begin
  Result := Task.Kind
end;

function TCommonThreadPool.GetKindConfig(const AKind: Integer; var KindCfg: TTaskKindConfig): Boolean;
begin
  Result := false;
  if Self.FTaskKinds <> nil then begin
    if Self.FTaskKinds.ContainsKey(AKind) then begin
      KindCfg := Self.FTaskKinds[AKind];
      Result := true;
    end;
  end;
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
  I: Integer;
  Pair: TPair<Integer, TQueue<IThreadTask>>;
begin
  FillChar(Result, SizeOf(Result), 0);

  FMetricsCS.Enter;
  try
    // Aggregate per-kind execution metrics
    for P in FMetrics.Values do
      AddMetrics(Result, P^);
  finally
    FMetricsCS.Leave;
  end;

  // -----------------------------
  // Runnable queue depth
  // -----------------------------
  Result.QueueDepth := 0;
  for I := Low(FQueues) to High(FQueues) do
    Inc(Result.QueueDepth, FQueues[I].Count);

  // -----------------------------
  // Quota queue depth
  // -----------------------------
  Result.QuotaQueue := 0;
  FQuotaCS.Enter;
  try
    for Pair in FQuotaQueues do begin
      Inc(Result.QuotaQueue, Pair.Value.Count);
      Inc(Result.QueueDepth, Pair.Value.Count); // important!
    end;
  finally
    FQuotaCS.Leave;
  end;
end;

class function TCommonThreadPool.GetWorkerStats: TThreadPoolWorkerStats;
begin
  Result := GetInstance._GetWorkerStats;
end;

function TCommonThreadPool.HasKindQuota(Kind: Integer): Boolean;
begin
  FQuotaCS.Enter;
  try
    Result := FQuotaByKind.ContainsKey(Kind);
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

function TCommonThreadPool.MetricsSnapshot: TDictionary<Integer, PThreadPoolMetrics>;
begin
  FMetricsCS.Enter;
  try
    Result := TDictionary<Integer, PThreadPoolMetrics>.Create(FMetrics);
  finally
    FMetricsCS.Leave;
  end;
end;

procedure TCommonThreadPool.OnBurstWorkerExit(Worker: TWorker);
var
  I, N: Integer;
begin
  FWorkerCS.Enter;
  try
    // Remove from FBurstWorkers array
    for I := 0 to High(FBurstWorkers) do begin
      if FBurstWorkers[I] = Worker then begin
        for N := I to High(FBurstWorkers) - 1 do
          FBurstWorkers[N] := FBurstWorkers[N + 1];

        SetLength(FBurstWorkers, Length(FBurstWorkers) - 1);
        Break;
      end;
    end;

    Dec(FWorkersTotal);
    Dec(FWorkersBurst);
  finally
    FWorkerCS.Leave;
  end;

  // Now safe to free (outside lock)
  Worker.Free;
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

procedure TCommonThreadPool.RetireIdleWorker;
var
  I, J: Integer;
  Worker: TWorker;
  NowTick: UInt64;
  Len: Integer;
begin
  NowTick := GetTickCount64;

  FWorkerCS.Enter;
  try
    if FWorkersTotal <= FMinWorkers then
      Exit;

    Len := Length(FWorkers);

    for I := Len - 1 downto 0 do begin
      Worker := FWorkers[I];

      if (Worker.State = wsIdle) and (NowTick - Worker.LastActiveTick >= FIdleTimeoutMs) then begin
        Worker.SetState(wsStopping);

        Dec(FWorkersIdle);
        Dec(FWorkersTotal);

        // Shift elements down
        for J := I to Len - 2 do
          FWorkers[J] := FWorkers[J + 1];

        SetLength(FWorkers, Len - 1);

        // Terminate and free the worker
        Worker.Terminate;
        FQueueEvent.SetEvent;
        Worker.WaitFor;
        Worker.Free;

        Exit; // retire ONE at a time
      end;
    end;
  finally
    FWorkerCS.Leave;
  end;
end;

procedure TCommonThreadPool.SetKindQuota(Kind: Integer; MaxWorkers: Integer);
var
  Q: TQueue<IThreadTask>;
  CS: TCriticalSection;
  Task: IThreadTask;
begin
  FQuotaCS.Enter;
  try
    if MaxWorkers <= 0 then begin
      // Disable quota
      FQuotaByKind.Remove(Kind);
      FActiveByKind.Remove(Kind);

      // Drain quota queue back to runnable queue
      if FQuotaQueues.TryGetValue(Kind, Q) and FQuotaQueueCS.TryGetValue(Kind, CS) then begin
        CS.Enter;
        try
          while Q.Count > 0 do begin
            Task := Q.Dequeue;
            EnqueueRunnableTask(Task); // <-- must wake workers
          end;
        finally
          CS.Leave;
        end;

        FQuotaQueues.Remove(Kind);
        Q.Free;
      end;

      if FQuotaQueueCS.TryGetValue(Kind, CS) then begin
        FQuotaQueueCS.Remove(Kind);
        CS.Free;
      end;

      Exit;
    end;

    // Enable / update quota
    FQuotaByKind.AddOrSetValue(Kind, MaxWorkers);

    if not FActiveByKind.ContainsKey(Kind) then
      FActiveByKind.Add(Kind, 0);

    if not FQuotaQueues.ContainsKey(Kind) then
      FQuotaQueues.Add(Kind, TQueue<IThreadTask>.Create);

    if not FQuotaQueueCS.ContainsKey(Kind) then
      FQuotaQueueCS.Add(Kind, TCriticalSection.Create);

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

procedure TCommonThreadPool.SpawnBurstWorker;
var
  W: TWorker;
begin
  FWorkerCS.Enter;
  try
    if FWorkersTotal >= FMaxWorkers + FBurstLimit then
      Exit;

    W := TWorker.Create(Self);
    W.FKind := wkBurst;
    Inc(FWorkersTotal);
    Inc(FWorkersBurst);

    SetLength(FBurstWorkers, Length(FBurstWorkers) + 1);
    FBurstWorkers[High(FBurstWorkers)] := W;
    W.Start;
  finally
    FWorkerCS.Leave;
  end;
end;

procedure TCommonThreadPool.SpawnWorker;
var
  Worker: TWorker;
begin
  FWorkerCS.Enter;
  try
    if FWorkersTotal >= FMaxWorkers then
      Exit;

    Worker := TWorker.Create(Self);
    Inc(FWorkersTotal);
    Inc(FWorkersIdle);

    Worker.FreeOnTerminate := False;

    SetLength(FWorkers, Length(FWorkers) + 1);
    FWorkers[High(FWorkers)] := Worker;

    Worker.Start;
  finally
    FWorkerCS.Leave;
  end;
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

function TCommonThreadPool.TryTakeQuotaTaskAny: IThreadTask;
var
  Pair: TPair<Integer, TQueue<IThreadTask>>;
  Kind, Active, Quota: Integer;
begin
  Result := nil;

  // Only inspect quota state here
  FQuotaCS.Enter;
  try
    for Pair in FQuotaQueues do begin
      if Pair.Value.Count = 0 then
        Continue;

      Kind := Pair.Key;

      if not FQuotaByKind.TryGetValue(Kind, Quota) then
        Continue;

      if not FActiveByKind.TryGetValue(Kind, Active) then
        Active := 0;

      // quota full → skip
      if Active >= Quota then
        Continue;

      // Take the task atomically
      Result := Pair.Value.Dequeue;
      Exit;
    end;
  finally
    FQuotaCS.Leave;
  end;
end;

function TCommonThreadPool.TryTakeQuotaTaskByKind(Kind: Integer): IThreadTask;
var
  Q: TQueue<IThreadTask>;
begin
  Result := nil;

  // Only proceed if a quota queue exists for this kind
  FQuotaCS.Enter;
  try
    if not FQuotaQueues.TryGetValue(Kind, Q) then
      Exit;

    // Take the first task if the queue is not empty
    if Q.Count > 0 then
      Result := Q.Dequeue;

    // Optional: remove empty queue
    if Q.Count = 0 then begin
      FQuotaQueues.Remove(Kind);
      Q.Free;
    end;
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
    Result.MinWorkers := FMinWorkers;
    Result.MaxWorkers := FMaxWorkers;
    Result.TotalWorkers := FWorkersTotal;
    Result.BusyWorkers := FWorkersBusy;
    Result.IdleWorkers := FWorkersIdle;
    Result.BaseWorkers := FBaseCount;
    Result.QuotaWorkers := FQuotaCount;
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

initialization
  //  if not Assigned(TCommonThreadPool.FInstance) then
  //    TCommonThreadPool.FInstance := TCommonThreadPool.Create(4); // default 4 workers

finalization
  if Assigned(TCommonThreadPool.FInstance) then
    FreeAndNil(TCommonThreadPool.FInstance);

end.
