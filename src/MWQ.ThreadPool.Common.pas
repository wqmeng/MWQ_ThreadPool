unit MWQ.ThreadPool.Common;

interface

uses
  System.Classes,
  System.SysUtils,
  System.SyncObjs,
  System.Generics.Collections,
  MWQ.ThreadPool.CPUUsage,
  MWQ.LogBridge,
  MWQ.LogBridge.ICommonLogger,
  MWQ.LogBridge.LockDiagnostics;

const
  PRIORITY_MAX = 7;
  CPU_BASE_LIMIT = 75; // %
  CPU_BURST_LIMIT = 60; // %

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
    DefaultPriority: Byte;
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
    Requeued: Int64;

    RunnableQueue: Integer; // Normal
    QuotaQueue: Int64; // Quota
    QueueDepth: Int64; // Total

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
    RefillPerSec: UInt64;
    LastTick: UInt64;
  end;

  TOnTaskFinished =
      procedure(
          const ATask: IThreadTask;
          const AResult: TTaskResult;
          const ARetryCount: Integer;
          const AExecTimeUs: UInt64
      ) of object;

  TOnTaskExcept = procedure(Sender: TObject; const ATask: IThreadTask) of object;

  TCommonThreadPool = class
  private
    class var
      FInstance: TCommonThreadPool;
      FGlobalStopping: Integer;

  private
    type
      TWorker = class(TThread)
      private
        FOwner: TCommonThreadPool;
        FState: TWorkerState;
        FKind: TWorkerKind;
        FTaskKind: Integer;
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
    FStopping: Boolean; // transition state
    FStopped: Boolean; // fully stopped
    FAcceptingStopped: Boolean; // reject new work while existing work drains

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

    FCPU: TCPUUsageMonitor;
    FUseCpuUsage: Boolean;
    FCachedCpu: Single;
    FLastCpuSampleTick: UInt64;
    FOnTaskExcept: TOnTaskExcept;

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
        RateRefillPerSec: Int64;
        DefaultPriority: Byte
    );
    procedure _CancelByOwner(Owner: UIntPtr);
    function _GetWorkerStats: TThreadPoolWorkerStats;
    function IsKindRegistered(AKind: Integer): Boolean;
    procedure EnqueueRunnableTask(const Task: IThreadTask);
    function DequeueRunnableTask: IThreadTask;
    function DequeueRunnableTaskMinPriority(MinPriority: Integer): IThreadTask;
    function TryTakeQuotaTaskAny(out AReservedKind: Integer): IThreadTask;
    function TryTakeQuotaTaskByKind(Kind: Integer; out AReservedKind: Integer): IThreadTask;
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
    procedure DoTaskExcept(const ATask: IThreadTask);

    procedure UpdateCpuUsage;
    function CpuAllowsScaleUp(Limit: Integer): Boolean;
    procedure SetUseCpuUsage(const Value: Boolean);
    procedure _Stop(Wait: Boolean = True);
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
        RateCapacity, RateRefillPerSec: Int64;
        DefaultPriority: Byte = 0
    ); static;
    class procedure CancelByOwner(Owner: UIntPtr); static;
    class function GetWorkerStats: TThreadPoolWorkerStats;
    class procedure StopAccepting;
    class procedure Stop(Wait: Boolean = True);
    class function IsStopped: Boolean;
    class function GetTaskKindName(AKind: Integer): string;
    class function GetTaskKindConfig(AKind: Integer; out Cfg: TTaskKindConfig): Boolean;
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
    function GetKindConfig(const AKind: Integer; out KindCfg: TTaskKindConfig): Boolean;
    function GetMetricsForKind(Kind: Integer; out Metrics: TThreadPoolMetrics): Boolean;
    function AvgExecTimeUs(const M: TThreadPoolMetrics): Double;
    function DumpThreadPoolStats: string;

    property QueueDepth: Integer read FQueueDepth;
    property BurstLimit: Integer read FBurstLimit write FBurstLimit;
    property IdleTimeoutMs: Cardinal read FIdleTimeoutMs write FIdleTimeoutMs;
    property MaxRetry: Integer read FMaxRetry write FMaxRetry;
    property CurrentCpuUsage: Single read FCachedCpu;
    property UseCpuUsage: Boolean read FUseCpuUsage write SetUseCpuUsage;
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

  WARNING:
    Enabling this flag changes throttling into dropping.
    This can look like worker starvation or deadlock during debugging.

  NEVER enable this for:
    - Financial transactions
    - Database writes
    - Message delivery
    - User-visible actions
    - Any task requiring reliability

  Default: FALSE
**}
    property DropTaskOnThrottle: Boolean read FDropTaskOnThrottle write FDropTaskOnThrottle;
    property Stopped: Boolean read FStopped;
    property OnTaskFinished: TOnTaskFinished read FOnTaskFinished write FOnTaskFinished;
    property OnTaskExcept: TOnTaskExcept read FOnTaskExcept write FOnTaskExcept;
  end;

  TAnonymousThreadTask = class(TInterfacedObject, IThreadTask)
  private
    FSuccess: Boolean;
    FFunc: TFunc<Boolean>;
    FPriority: Byte;
    FKind: Integer;
    FKey: UIntPtr;
    FOwner: UIntPtr;
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
  System.Math;

// ---------------------------- UTILITIES ----------------------------

function BackoffMs(Retry: Integer): Integer;
begin
  Result := Min(5 + Retry * Retry * 10, 500);
end;

// ---------------------------- WORKER ----------------------------

constructor TCommonThreadPool.TWorker.Create(AOwner: TCommonThreadPool);
begin
  inherited Create(True);
  FreeOnTerminate := False;
  FOwner := AOwner;
  FState := wsIdle;
  FKind := wkBase;
end;

procedure TCommonThreadPool.TWorker.SetState(AState: TWorkerState);
begin
  if FState = AState then
    Exit;

  TLockDiagnostics.CriticalSectionEnter(FOwner.FWorkerCS, 'TCommonThreadPool.Worker');
  try
    case FState of
      wsIdle: begin
        Dec(FOwner.FWorkersIdle);
        if FOwner.FWorkersIdle < 0 then
          FOwner.FWorkersIdle := 0;
      end;
      wsBusy: begin
        Dec(FOwner.FWorkersBusy);
        if FOwner.FWorkersBusy < 0 then
          FOwner.FWorkersBusy := 0;
      end;
    end;

    case AState of
      wsIdle: Inc(FOwner.FWorkersIdle);
      wsBusy: Inc(FOwner.FWorkersBusy);
    end;

    FState := AState;
    if FState = wsIdle then begin
      if Self.FKind = wkQuota then begin
        Self.FKind := wkBase;
        Dec(FOwner.FQuotaCount);
        Inc(FOwner.FBaseCount);
      end;
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FOwner.FWorkerCS, 'TCommonThreadPool.Worker');
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
  LRetry: Integer;
  StartTick, Latency: UInt64;
  Tid: Cardinal;
  LResult: TTaskResult;
  Kind: Integer;
  LQuotaEntered: Boolean;
  LReservedKind: Integer;
begin
  Tid := TThread.CurrentThread.ThreadID;;
  SetState(wsBusy);

{$IFDEF DEBUG}
  Log(Format('Worker start [TID=%d Kind=%s]', [Tid, WorkerKindToStr(Self.FKind)]), etDebug);
{$ENDIF}

  while not Terminated do begin
    // Fast exit on pool stop
    if FOwner.FStopping then
      Break;

    Task := nil;
    LQuotaEntered := False;
    LReservedKind := -1;

    { STEP 1: Runnable task }
    if Self.FKind = wkBurst then begin
      Task := FOwner.DequeueRunnableTaskMinPriority(5);
      if Task = nil then begin
{$IFDEF DEBUG}
        Log(Format('Worker exit burst [TID=%d]', [Tid]), etDebug);
{$ENDIF}
        Break; // burst worker intentionally exits
      end;
    end
    else if Self.FKind = wkQuota then
      { High-priority control tasks must not wait behind quota work. }
      Task := FOwner.DequeueRunnableTaskMinPriority(5)
    else
      Task := FOwner.DequeueRunnableTask;

    { STEP 2: Quota task }
    if Task = nil then begin
      if Self.FKind = wkQuota then
        //        Task := FOwner.TryTakeQuotaTaskByKind(Kind)
        Task := FOwner.TryTakeQuotaTaskByKind(FTaskKind, LReservedKind)
      else
        Task := FOwner.TryTakeQuotaTaskAny(LReservedKind);

      if Task <> nil then begin
        LQuotaEntered := LReservedKind >= 0;
{$IFDEF THREADPOOL_VERBOSE_LOG}
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
          Self.FTaskKind := Task.Kind;
          Inc(FOwner.FQuotaCount);
          Dec(FOwner.FBaseCount);
        end;
      end
      else begin
{$IFDEF THREADPOOL_VERBOSE_LOG}
        if (Self.FKind = wkQuota) and (Task = nil) then begin
          var LM := FOwner.GetMetricsSummary;

          Log(Format('[QUOTA EMPTY] TID=%d TaskKind=%d RunnableDepth=%d', [Tid, FTaskKind, LM.QueueDepth]), etDebug);
        end;
{$ENDIF}

        if (Self.FKind = wkQuota) then begin
          Task := FOwner.TryTakeQuotaTaskAny(LReservedKind);

          if Task <> nil then begin
            LQuotaEntered := LReservedKind >= 0;
            Self.FTaskKind := Task.Kind;
          end
          else begin
            Task := FOwner.DequeueRunnableTask;
            //            Self.FKind := wkBase;
            //            Self.FTaskKind := -1;
            //            Dec(FOwner.FQuotaCount);
            //            Inc(FOwner.FBaseCount);
          end;
        end;
      end;
    end;

    { STEP 3: Wait }
    if Task = nil then begin
{$IFDEF THREADPOOL_VERBOSE_LOG}
      Log(Format('Worker idle wait [TID=%d]', [Tid]), etDebug);

      Log(Format('[SLEEP] TID=%d', [TThread.Current.ThreadID]), etDebug);
{$ENDIF}

      //      FLastActiveTick := GetTickCount64;
      SetState(wsIdle);
      if not FOwner.FStopping then begin
        FOwner.FQueueEvent.ResetEvent;
        if not FOwner.FStopping then
          FOwner.FQueueEvent.WaitFor(250);
      end;

{$IFDEF THREADPOOL_VERBOSE_LOG}
      Log(Format('[WAKE] TID=%d', [TThread.Current.ThreadID]), etDebug);
{$ENDIF}

      // Idle timeout exit logic (burst / dynamic workers)
      if FOwner.FStopping or Terminated then
        Break;
      SetState(wsBusy);
      Continue;
    end;

    { STEP 4: Cancelled }
    if Task.IsCancelled then begin
{$IFDEF THREADPOOL_VERBOSE_LOG}
      Log(Format('Task cancelled skip [TID=%d Kind=%d Key=%x]', [Tid, Task.Kind, Task.Key]), etDebug);
{$ENDIF}
      if LQuotaEntered then begin
        FOwner.LeaveKind(Task.Kind);
        LQuotaEntered := False;
      end;
      FOwner.DoTaskFinished(Task, TTaskResult.trCanceled, 0, 0);
      Continue;
    end;

    Kind := Task.Kind;

    { STEP 5: Deadline }
    if (Task.DeadlineTick > 0) and (GetTickCount64 > Task.DeadlineTick) then begin
{$IFDEF THREADPOOL_VERBOSE_LOG}
      Log(Format('Task expired [TID=%d Kind=%d Key=%x]', [Tid, Kind, Task.Key]), etDebug);
{$ENDIF}

      Task.Cancel;
      if LQuotaEntered then begin
        FOwner.LeaveKind(Kind);
        LQuotaEntered := False;
      end;
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
      if (not LQuotaEntered) and (not FOwner.TryEnterKind(Kind)) then begin
{$IFDEF THREADPOOL_VERBOSE_LOG}
        Log(Format('Quota denied → requeue [TID=%d Kind=%d Key=%x]', [Tid, Kind, Task.Key]), etInfo);
{$ENDIF}

        FOwner.IncMetric(Kind, FOwner.FMetrics[Kind].QuotaDenied);
        FOwner.EnqueueQuotaTask(Task);

        // Quota Full. Change from wkQuota to wkBase;
        if Self.FKind = wkQuota then begin
          Self.FKind := wkBase;
          Self.FTaskKind := -1;
          Dec(FOwner.FQuotaCount);
          Inc(FOwner.FBaseCount);
        end;

        Continue; // ? IMPORTANT: continue, never break
      end;

      LQuotaEntered := True;

      // Deal Quota, Change from wkBase to wkQuota;
      if Self.FKind = wkBase then begin
        Self.FKind := wkQuota;
        Self.FTaskKind := Kind;
        Inc(FOwner.FQuotaCount);
        Dec(FOwner.FBaseCount);
      end;
    end;

    { STEP 7: Execute }
{$IFDEF THREADPOOL_VERBOSE_LOG}
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
        try
          Task.Run;
        except
          on E: Exception do begin
            LResult := TTaskResult.trFailed;
            FOwner.DoTaskExcept(Task);
            break; // exit current task
          end;
        end;

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

      if LQuotaEntered then begin
        FOwner.LeaveKind(Kind);
        LQuotaEntered := False;
      end;

      if Self.FKind = wkQuota then begin
        if FOwner.FWorkersIdle <= 0 then begin
          // No idle workers available.
          // Return this worker to the global scheduler so that
          // high-priority runnable tasks are not starved by
          // quota-only processing.

          Self.FKind := wkBase;
          Self.FTaskKind := -1;
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
  if FStopping then
    Exit(False);
  if FAcceptingStopped then
    Exit(False);

  Result := True;
  TLockDiagnostics.CriticalSectionEnter(FRateCS, 'TCommonThreadPool.Rate');
  try
    if not FRateLimiters.TryGetValue(Kind, L) then
      Exit;

    if (L.Capacity <= 0) or (L.RefillPerSec <= 0) then
      Exit; // explicitly disabled

    Now := TThread.GetTickCount64;
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
    TLockDiagnostics.CriticalSectionExit(FRateCS, 'TCommonThreadPool.Rate');
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
    TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
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
      TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
    end;
  end;
end;

class procedure TCommonThreadPool.CancelByOwner(Owner: UIntPtr);
var
  LPool: TCommonThreadPool;
begin
  LPool := GetInstance;
  if LPool <> nil then
    LPool._CancelByOwner(Owner);
end;

procedure TCommonThreadPool.CheckScale;
begin
  UpdateCpuUsage;
  // Scale UP
  if ((FQueueDepth - FQueueQuota) > FBaseCount * 2)
      and (FWorkersTotal < FMaxWorkers)
      and CpuAllowsScaleUp(CPU_BASE_LIMIT) then
    SpawnWorker;

  // Scale DOWN
  if (FWorkersIdle > 0) and (FWorkersTotal > FMinWorkers) then
    RetireIdleWorker;
end;

function TCommonThreadPool.CpuAllowsScaleUp(Limit: Integer): Boolean;
begin
  Result := (not FUseCpuUsage) or (FCachedCpu < Limit);
end;

{ ================= Thread Pool ================= }

constructor TCommonThreadPool.Create(WorkerCount: Integer);
var
  I, P: Integer;
  CPU: Integer;
begin
  FStopping := false;
  FStopped := false;
  FAcceptingStopped := false;
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

  FUseCpuUsage := false;

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
    FWorkers[I].Start;
  end;
end;

function TCommonThreadPool.DequeueRunnableTask: IThreadTask;
var
  P: Integer;
begin
  Result := nil;

  for P := PRIORITY_MAX downto 0 do begin
    TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Dec(FQueueDepth);
        Exit;
      end;
    finally
      TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
    end;
  end;
end;

function TCommonThreadPool.DequeueRunnableTaskMinPriority(MinPriority: Integer): IThreadTask;
var
  P, Kind: Integer;
begin
  Result := nil;

  for P := PRIORITY_MAX downto MinPriority do begin
    TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Kind := GetKind(Result);
        IncMetric(Kind, FMetrics[Kind].QueueDepth, -1); // FIX
        Exit;
      end;
    finally
      TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
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
    TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
    try
      if FQueues[P].Count > 0 then begin
        Result := FQueues[P].Dequeue;
        Kind := GetKind(Result);
        IncMetric(Kind, FMetrics[Kind].QueueDepth, -1); // FIX
        Exit;
      end;
    finally
      TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
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
      // Do not invoke the task again after an exception.  Retrying here
      // bypasses the pool retry policy and can re-enter CEF or a one-shot task.
      raise;
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
  LCs: TPair<Integer, TCriticalSection>;
  LKeys: TArray<Integer>;
begin
  // 1. Stop workers
  _Stop(True); // ALWAYS wait in destructor
  FQueueEvent.SetEvent;

  for W in FWorkers do
    W.Free;
  SetLength(FWorkers, 0);

  if Length(FBurstWorkers) > 0 then begin
    for W in FBurstWorkers do
      W.Free;
    SetLength(FBurstWorkers, 0);
  end;

  // 2. Free runnable queues and locks
  for I := 0 to PRIORITY_MAX do begin
    FQueues[I].Free;
    FQueueLocks[I].Free;
  end;

  // 3. Free quota queues (MISSING ENTIRELY)
  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    for Q in FQuotaQueues.Values do
      Q.Free;
    FQuotaQueues.Free;
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;

  // 4. Free metrics
  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
  try
    for P in FMetrics.Values do
      Dispose(P);
    FMetrics.Free;
  finally
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
  end;
  FMetricsCS.Free;

  if FCPU <> nil then
    FCPU.Free;

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
  if FQuotaQueueCS.Count > 0 then begin
    LKeys := FQuotaQueueCS.Keys.ToArray;

    for I := Low(LKeys) to High(LKeys) do begin
      LCs := FQuotaQueueCS.ExtractPair(LKeys[I]);
      LCs.Value.Free;
    end;
  end;
  FQuotaQueueCS.Free;
end;

procedure TCommonThreadPool.DoTaskExcept(const ATask: IThreadTask);
var
  LTask: IThreadTask;
begin
  if FStopping then
    Exit;

  if Assigned(FOnTaskExcept) then begin
    LTask := ATask;

    TThread.Queue(
        nil,
        procedure
        begin
          if FStopping then
            Exit;
          FOnTaskExcept(Self, LTask);
        end
    );
  end;
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
  if FStopping then
    Exit;

  if Assigned(FOnTaskFinished) then begin
    LTask := ATask; // pin interface reference
    TThread.Queue(
        nil,
        procedure
        begin
          if FStopping then
            Exit;
          FOnTaskFinished(LTask, AResult, ARetryCount, AExecTimeUs);
        end
    );
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
  TaskKey: UIntPtr;
  PriorityValue: Byte;
  Metrics: PThreadPoolMetrics;
begin
  if FAcceptingStopped or FStopping or FStopped then
    Exit; // or raise exception, depending on your design

  if Task = nil then begin
{$IFDEF DEBUG}
    Log('ThreadPool.Enqueue skipped: Task=nil', etDebug);
{$ENDIF}
    Exit;
  end;

  Kind := GetKind(Task);
  TaskKey := Task.Key;
  PriorityValue := Task.Priority;

  if not IsKindRegistered(Kind) then begin
{$IFDEF DEBUG}
    Log(
        Format(
            'ThreadPool.Enqueue rejected: Kind not registered (Kind=%d Key=%x Priority=%d)',
            [Kind, TaskKey, PriorityValue]
        ),
        etError
    );
{$ENDIF}
    raise Exception.CreateFmt('Task kind %d is not registered', [Kind]);
  end;

  { Ensure metrics entry exists }
  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
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
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
  end;

  { Rate limiting }
  if not AllowByRate(Kind) then begin
    IncMetric(Kind, FMetrics[Kind].Throttled);

{$IFDEF DEBUG}
    Log(
        Format(
            'Rate limit hit (Kind=%d Key=%x Priority=%d Drop=%s)',
            [Kind, TaskKey, PriorityValue, BoolToStr(FDropTaskOnThrottle, True)]
        ),
        etDebug
    );
{$ENDIF}

    if FDropTaskOnThrottle then begin
      IncMetric(Kind, FMetrics[Kind].Dropped);
{$IFDEF DEBUG}
      Log(Format('TASK DROPPED due to throttle (Kind=%d Key=%x)', [Kind, TaskKey]), etWarning);
{$ENDIF}
      Exit;
    end;

    // Safe path: requeue / delay
    //    Enqueue(Task);
    //    Sleep(1);
    //    Exit;
  end;

  { Normalize priority }
  if PriorityValue > PRIORITY_MAX then
    P := PRIORITY_MAX
  else
    P := PriorityValue;

  { Enqueue task }
  Log('TP Task before enqueue=%p Kind=%d Key=%x Priority=%d',
    [Pointer(Task), Kind, TaskKey, PriorityValue], etWarning);
  TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
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
    TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
  end;
  Log('TP Task after enqueue=%p Kind=%d Key=%x Priority=%d',
    [Pointer(Task), Kind, TaskKey, PriorityValue], etWarning);

{$IFDEF DEBUG}
  Log(
      Format(
          'ThreadPool.Enqueue OK (Kind=%d Key=%x Priority=%d QueuePrio=%d QueueDepth=%d)',
          [Kind, TaskKey, PriorityValue, P, FMetrics[Kind].QueueDepth]
      ),
      etDebug
  );
{$ENDIF}

  { Burst worker }
  if not IsRequeue then begin
    try
      if (PriorityValue >= 5) and (FWorkersIdle = 0) and (FWorkersTotal < FMaxWorkers + FBurstLimit) then begin
{$IFDEF DEBUG}
        Log(
            Format(
                'ThreadPool.SpawnBurstWorker (Reason=HighPriority Kind=%d Key=%x Workers=%d Idle=%d)',
                [Kind, TaskKey, FWorkersTotal, FWorkersIdle]
            ),
            etDebug
        );
{$ENDIF}

        SpawnBurstWorker;
      end
      else begin
        CheckScale;
      end;
    except
      on E: Exception do
        Log('TCommonThreadPool.Enqueue %s' + sLineBreak + '%s', [E.Message, E.StackTrace], etException);
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
var
  LPool: TCommonThreadPool;
  LConfig: TTaskKindConfig;
  LEffectivePriority: Byte;
begin
  LEffectivePriority := APriority;
  if (APriority = 0) then begin
    LPool := GetInstance;
    if (LPool <> nil) and LPool.GetKindConfig(AKind, LConfig) then
      LEffectivePriority := LConfig.DefaultPriority;
  end;

  if LEffectivePriority > PRIORITY_MAX then
    LEffectivePriority := PRIORITY_MAX;

  EnqueueTask(
      TAnonymousThreadTask
          .Create(AFunc, LEffectivePriority, AKind, ATaskOwner, AKey, ADeadlineTick, ACanRetry, AContext)
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
  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    if not FQuotaQueues.TryGetValue(Kind, Q) then begin
      Q := TQueue<IThreadTask>.Create;
      FQuotaQueues.Add(Kind, Q);
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;

  // Enqueue the task into the quota queue
  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    Q.Enqueue(Task);
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
{$IFDEF THREADPOOL_VERBOSE_LOG}
  Log(Format('[Q-IN] Kind=%d Key=%x QCount=%d', [Task.Kind, Task.Key, Q.Count]), etInfo);
{$ENDIF}

  FQueueEvent.SetEvent;
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
  TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
  try
    FQueues[P].Enqueue(Task);
    Inc(FQueueDepth); // global runnable depth
  finally
    TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
  end;

  // Wake one sleeping worker
  FQueueEvent.SetEvent;
end;

class procedure TCommonThreadPool.EnqueueTask(const Task: IThreadTask);
var
  LPool: TCommonThreadPool;
begin
  LPool := GetInstance;
  if LPool <> nil then
    LPool.Enqueue(Task);
end;

procedure TCommonThreadPool.EnterKey(Key: UIntPtr);
var
  L: TLightweightMREW;
begin
  if FStopping then
    Exit;

  TLockDiagnostics.CriticalSectionEnter(FKeyLockCS, 'TCommonThreadPool.Key');
  try
    if not FKeyLocks.TryGetValue(Key, L) then begin
      L := Default(TLightweightMREW);
      FKeyLocks.Add(Key, L);
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FKeyLockCS, 'TCommonThreadPool.Key');
  end;
  TLockDiagnostics.LightweightBeginWrite(L, 'TCommonThreadPool.Key', Pointer(Key));
end;

class function TCommonThreadPool.GetInstance: TCommonThreadPool;
begin
  if TInterlocked.CompareExchange(FGlobalStopping, 0, 0) <> 0 then begin
    Result := nil;
    Exit;
  end;

  if not Assigned(TCommonThreadPool.FInstance) then
    TCommonThreadPool.FInstance := TCommonThreadPool.Create(4); // default 4 workers

  Result := FInstance;
end;

function TCommonThreadPool.GetKind(const Task: IThreadTask): Integer;
begin
  Result := Task.Kind
end;

function TCommonThreadPool.GetKindConfig(const AKind: Integer; out KindCfg: TTaskKindConfig): Boolean;
begin
  Result := Assigned(FTaskKinds) and FTaskKinds.TryGetValue(AKind, KindCfg);
end;

function TCommonThreadPool.GetMetricsByKind: TDictionary<Integer, TThreadPoolMetrics>;
var
  K: Integer;
  P: PThreadPoolMetrics;
begin
  Result := TDictionary<Integer, TThreadPoolMetrics>.Create;

  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
  try
    for K in FMetrics.Keys do begin
      P := FMetrics[K];
      Result.Add(K, P^); // copy
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
  end;
end;

function TCommonThreadPool.GetMetricsForKind(Kind: Integer; out Metrics: TThreadPoolMetrics): Boolean;
var
  P: PThreadPoolMetrics;
begin
  Result := False;
  FillChar(Metrics, SizeOf(Metrics), 0);

  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
  try
    if FMetrics.TryGetValue(Kind, P) then begin
      Metrics := P^;
      Result := True;
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
  end;
end;

function TCommonThreadPool.GetMetricsSummary: TThreadPoolMetrics;
var
  P: PThreadPoolMetrics;
  I: Integer;
  Pair: TPair<Integer, TQueue<IThreadTask>>;
begin
  FillChar(Result, SizeOf(Result), 0);

  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
  try
    // Aggregate per-kind execution metrics
    for P in FMetrics.Values do
      AddMetrics(Result, P^);
  finally
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
  end;

  // -----------------------------
  // Runnable queue depth
  // -----------------------------
  Result.RunnableQueue := 0;
  Result.QueueDepth := 0;

  for I := Low(FQueues) to High(FQueues) do begin
    Inc(Result.RunnableQueue, FQueues[I].Count);
    Inc(Result.QueueDepth, FQueues[I].Count);
  end;

  // -----------------------------
  // Quota queue depth
  // -----------------------------
  Result.QuotaQueue := 0;

  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    for Pair in FQuotaQueues do begin
      Inc(Result.QuotaQueue, Pair.Value.Count);
      Inc(Result.QueueDepth, Pair.Value.Count);
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
end;

class function TCommonThreadPool.GetTaskKindConfig(AKind: Integer; out Cfg: TTaskKindConfig): Boolean;
var
  LPool: TCommonThreadPool;
begin
  LPool := GetInstance;
  Result := (LPool <> nil) and LPool.GetKindConfig(AKind, Cfg);
end;

class function TCommonThreadPool.GetTaskKindName(AKind: Integer): string;
var
  Cfg: TTaskKindConfig;
begin
  if GetTaskKindConfig(AKind, Cfg) then
    Result := Cfg.Name
  else
    Result := Format('Kind(%d)', [AKind]);
end;

class function TCommonThreadPool.GetWorkerStats: TThreadPoolWorkerStats;
var
  LPool: TCommonThreadPool;
begin
  FillChar(Result, SizeOf(Result), 0);
  LPool := GetInstance;
  if LPool <> nil then
    Result := LPool._GetWorkerStats;
end;

function TCommonThreadPool.HasKindQuota(Kind: Integer): Boolean;
begin
  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    Result := FQuotaByKind.ContainsKey(Kind);
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
end;

procedure TCommonThreadPool.IncMetric(Kind: Integer; var Field: Int64; Delta: Int64);
begin
  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
  try
    if not FMetrics.ContainsKey(Kind) then
      FMetrics.Add(Kind, New(PThreadPoolMetrics));
    Inc(Field, Delta);
  finally
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
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
  TLockDiagnostics.CriticalSectionEnter(FKeyLockCS, 'TCommonThreadPool.Key');
  try
    if FKeyLocks.TryGetValue(Key, L) then
      TLockDiagnostics.LightweightEndWrite(L, 'TCommonThreadPool.Key', Pointer(Key));
  finally
    TLockDiagnostics.CriticalSectionExit(FKeyLockCS, 'TCommonThreadPool.Key');
  end;
end;

procedure TCommonThreadPool.LeaveKind(Kind: Integer);
var
  V: Integer;
  Q: TQueue<IThreadTask>;
  LShouldWake: Boolean;
begin
  LShouldWake := False;

  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    if not FActiveByKind.TryGetValue(Kind, V) then
      Exit;

    Dec(V);
    if V < 0 then
      V := 0;

    FActiveByKind[Kind] := V;
    LShouldWake := FQuotaQueues.TryGetValue(Kind, Q) and (Q.Count > 0);
{$IFDEF DEBUG}
    Log(Format('[LEAVE] Kind=%d Active=%d->%d', [Kind, V + 1, V]), etDebug);
{$ENDIF}
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;

  if LShouldWake and (not FStopping) then
    FQueueEvent.SetEvent;
end;

function TCommonThreadPool.MetricsSnapshot: TDictionary<Integer, PThreadPoolMetrics>;
begin
  TLockDiagnostics.CriticalSectionEnter(FMetricsCS, 'TCommonThreadPool.Metrics');
  try
    Result := TDictionary<Integer, PThreadPoolMetrics>.Create(FMetrics);
  finally
    TLockDiagnostics.CriticalSectionExit(FMetricsCS, 'TCommonThreadPool.Metrics');
  end;
end;

procedure TCommonThreadPool.OnBurstWorkerExit(Worker: TWorker);
var
  I, N: Integer;
begin
  TLockDiagnostics.CriticalSectionEnter(FWorkerCS, 'TCommonThreadPool.Worker');
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
    TLockDiagnostics.CriticalSectionExit(FWorkerCS, 'TCommonThreadPool.Worker');
  end;

  // Now safe to free (outside lock)
  Worker.Free;
end;

class procedure TCommonThreadPool.RegisterTaskKind(
    AKind: Integer;
    const AName: string;
    MaxWorkers: Integer;
    RateCapacity, RateRefillPerSec: Int64;
    DefaultPriority: Byte
);
var
  LPool: TCommonThreadPool;
begin
  LPool := GetInstance;
  if LPool <> nil then
    LPool._RegisterTaskKind(AKind, AName, MaxWorkers, RateCapacity, RateRefillPerSec, DefaultPriority);
end;

procedure TCommonThreadPool.RetireIdleWorker;
var
  I, J: Integer;
  Worker: TWorker;
  WorkerToRetire: TWorker;
  NowTick: UInt64;
  Len: Integer;
begin
  NowTick := TThread.GetTickCount64;
  WorkerToRetire := nil;

  TLockDiagnostics.CriticalSectionEnter(FWorkerCS, 'TCommonThreadPool.Worker');
  try
    if FWorkersTotal <= FMinWorkers then
      Exit;

    Len := Length(FWorkers);

    for I := Len - 1 downto 0 do begin
      Worker := FWorkers[I];

      if (Worker.ThreadID <> TThread.CurrentThread.ThreadID)
          and (Worker.State = wsIdle)
          and (NowTick - Worker.LastActiveTick >= FIdleTimeoutMs) then begin
        WorkerToRetire := Worker;
        WorkerToRetire.SetState(wsStopping);

        Dec(FWorkersTotal);
        Dec(FBaseCount);
        if FBaseCount < 0 then
          FBaseCount := 0;

        // Shift elements down
        for J := I to Len - 2 do
          FWorkers[J] := FWorkers[J + 1];

        SetLength(FWorkers, Len - 1);

        Break; // retire ONE at a time
      end
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FWorkerCS, 'TCommonThreadPool.Worker');
  end;

  if WorkerToRetire <> nil then begin
    // Terminate and free the worker outside lock to prevent deadlock
    WorkerToRetire.Terminate;
    FQueueEvent.SetEvent;
    WorkerToRetire.WaitFor;
    WorkerToRetire.Free;
  end;
end;

procedure TCommonThreadPool.SetKindQuota(Kind: Integer; MaxWorkers: Integer);
var
  Q: TQueue<IThreadTask>;
  CS: TCriticalSection;
  Task: IThreadTask;
begin
  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    if MaxWorkers <= 0 then begin
      // Disable quota
      FQuotaByKind.Remove(Kind);
      FActiveByKind.Remove(Kind);

      // Drain quota queue back to runnable queue
      if FQuotaQueues.TryGetValue(Kind, Q) and FQuotaQueueCS.TryGetValue(Kind, CS) then begin
        TLockDiagnostics.CriticalSectionEnter(CS, 'TCommonThreadPool.QuotaQueue');
        try
          while Q.Count > 0 do begin
            Task := Q.Dequeue;
            EnqueueRunnableTask(Task); // <-- must wake workers
          end;
        finally
          TLockDiagnostics.CriticalSectionExit(CS, 'TCommonThreadPool.QuotaQueue');
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
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
end;

procedure TCommonThreadPool.SetRateLimit(Kind: Integer; Capacity, RefillPerSec: Int64);
var
  L: TKindRateLimiter;
begin
  TLockDiagnostics.CriticalSectionEnter(FRateCS, 'TCommonThreadPool.Rate');
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
    L.LastTick := TThread.GetTickCount64;

    FRateLimiters.AddOrSetValue(Kind, L);
  finally
    TLockDiagnostics.CriticalSectionExit(FRateCS, 'TCommonThreadPool.Rate');
  end;
end;

procedure TCommonThreadPool.SetUseCpuUsage(const Value: Boolean);
begin
  FUseCpuUsage := Value;
  if FUseCpuUsage and (FCPU = nil) then
    FCPU := TCPUUsageMonitor.Create;
end;

procedure TCommonThreadPool.SpawnBurstWorker;
var
  W: TWorker;
begin
  UpdateCpuUsage;

  if not CpuAllowsScaleUp(CPU_BURST_LIMIT) then
    Exit;

  TLockDiagnostics.CriticalSectionEnter(FWorkerCS, 'TCommonThreadPool.Worker');
  try
    if FWorkersTotal >= FMaxWorkers + FBurstLimit then
      Exit;

    W := TWorker.Create(Self);
    W.FKind := wkBurst;
    W.FTaskKind := -1;
    W.FState := wsBusy;
    Inc(FWorkersTotal);
    Inc(FWorkersBurst);
    Inc(FWorkersBusy);

    SetLength(FBurstWorkers, Length(FBurstWorkers) + 1);
    FBurstWorkers[High(FBurstWorkers)] := W;
    W.Start;
  finally
    TLockDiagnostics.CriticalSectionExit(FWorkerCS, 'TCommonThreadPool.Worker');
  end;
end;

procedure TCommonThreadPool.SpawnWorker;
var
  Worker: TWorker;
begin
  TLockDiagnostics.CriticalSectionEnter(FWorkerCS, 'TCommonThreadPool.Worker');
  try
    if FWorkersTotal >= FMaxWorkers then
      Exit;

    Worker := TWorker.Create(Self);
    Inc(FWorkersTotal);
    Inc(FWorkersIdle);
    Inc(FBaseCount);

    Worker.FreeOnTerminate := False;

    SetLength(FWorkers, Length(FWorkers) + 1);
    FWorkers[High(FWorkers)] := Worker;

    Worker.Start;
  finally
    TLockDiagnostics.CriticalSectionExit(FWorkerCS, 'TCommonThreadPool.Worker');
  end;
end;

class procedure TCommonThreadPool.Stop(Wait: Boolean);
begin
  TInterlocked.Exchange(FGlobalStopping, 1);
  if Assigned(FInstance) then
    FInstance._Stop(Wait);
end;

class procedure TCommonThreadPool.StopAccepting;
begin
  TInterlocked.Exchange(FGlobalStopping, 1);
  if not Assigned(FInstance) then
    Exit;

  FInstance.FAcceptingStopped := True;
  FInstance.FQueueEvent.SetEvent;
end;

class function TCommonThreadPool.IsStopped: Boolean;
begin
  Result := (not Assigned(FInstance)) or FInstance.Stopped;
end;

function TCommonThreadPool.TryEnterKind(Kind: Integer): Boolean;
var
  Active, Quota: Integer;
begin
  if FStopping then
    Exit(false);
  if FAcceptingStopped then
    Exit(false);
  Result := True;
  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    if not FQuotaByKind.TryGetValue(Kind, Quota) then
      Exit;

    Active := FActiveByKind[Kind];
    if Active >= Quota then begin
{$IFDEF DEBUG}
      Log(Format('[ENTER  Active >= Quota false] Kind=%d Active=%d->%d', [Kind, Active, Active + 1]), etDebug);
{$ENDIF}
      Exit(False);
    end;

    FActiveByKind[Kind] := Active + 1;
{$IFDEF DEBUG}
    Log(Format('[ENTER true] Kind=%d Active=%d->%d', [Kind, Active, Active + 1]), etDebug);
{$ENDIF}
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
end;

function TCommonThreadPool.TryTakeQuotaTaskAny(out AReservedKind: Integer): IThreadTask;
var
  Pair: TPair<Integer, TQueue<IThreadTask>>;
  Kind, Active, Quota: Integer;
begin
{$IFDEF THREADPOOL_VERBOSE_LOG}
  Log(Format('[TRY-QUOTA] WorkersIdle=%d QuotaQueues=%d', [FWorkersIdle, FQuotaQueues.Count]), etDebug);
{$ENDIF}
  Result := nil;
  AReservedKind := -1;

  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    for Pair in FQuotaQueues do begin
      if Pair.Value.Count = 0 then
        Continue;

      Kind := Pair.Key;

      if not FQuotaByKind.TryGetValue(Kind, Quota) then
        Continue;

      if not FActiveByKind.TryGetValue(Kind, Active) then
        Active := 0;

      if Active >= Quota then
        Continue;

      Result := Pair.Value.Dequeue;
      FActiveByKind.AddOrSetValue(Kind, Active + 1);
      AReservedKind := Kind;
{$IFDEF THREADPOOL_VERBOSE_LOG}
      Log(
          Format(
              '[Q-OUT] Kind=%d Key=%x Active=%d Quota=%d Remain=%d',
              [Result.Kind, Result.Key, Active + 1, Quota, Pair.Value.Count]
          ),
          etInfo
      );
{$ENDIF}
      Exit;
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
end;

function TCommonThreadPool.TryTakeQuotaTaskByKind(Kind: Integer; out AReservedKind: Integer): IThreadTask;
var
  Q: TQueue<IThreadTask>;
  Active, Quota: Integer;
begin
  Result := nil;
  AReservedKind := -1;

  TLockDiagnostics.CriticalSectionEnter(FQuotaCS, 'TCommonThreadPool.Quota');
  try
    if not FQuotaQueues.TryGetValue(Kind, Q) then
      Exit;

    if Q.Count = 0 then
      Exit;

    if not FQuotaByKind.TryGetValue(Kind, Quota) then
      Exit;

    if not FActiveByKind.TryGetValue(Kind, Active) then
      Active := 0;

    if Active >= Quota then
      Exit;

    Result := Q.Dequeue;
    FActiveByKind.AddOrSetValue(Kind, Active + 1);
    AReservedKind := Kind;
{$IFDEF THREADPOOL_VERBOSE_LOG}
    Log(
        Format(
            '[Q-OUT] Kind=%d Key=%x Active=%d Quota=%d Remain=%d',
            [Result.Kind, Result.Key, Active + 1, Quota, Q.Count]
        ),
        etInfo
    );
{$ENDIF}
  finally
    TLockDiagnostics.CriticalSectionExit(FQuotaCS, 'TCommonThreadPool.Quota');
  end;
end;

procedure TCommonThreadPool.UpdateCpuUsage;
const
  CPU_SAMPLE_INTERVAL = 1000; // ms
begin
  if not FUseCpuUsage then
    Exit;

  if TThread.GetTickCount64 - FLastCpuSampleTick < CPU_SAMPLE_INTERVAL then
    Exit;

  FCPU.TrySample(FCachedCpu);

  FLastCpuSampleTick := TThread.GetTickCount64;
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
    TLockDiagnostics.CriticalSectionEnter(FQueueLocks[P], 'TCommonThreadPool.Queue');
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
      TLockDiagnostics.CriticalSectionExit(FQueueLocks[P], 'TCommonThreadPool.Queue');
    end;
  end;
end;

function TCommonThreadPool._GetWorkerStats: TThreadPoolWorkerStats;
var
  LStartTick: UInt64;
begin
  LStartTick := TThread.GetTickCount64;
  Log(
      Format('[NEWS_STATS] WORKER_LOCK WAIT Lock=%p Pool=%p Thread=%d Tick=%d',
        [Pointer(@FWorkerCS), Pointer(Self), TThread.CurrentThread.ThreadID, LStartTick]),
      etDebug
  );
  TLockDiagnostics.CriticalSectionEnter(FWorkerCS, 'TCommonThreadPool.Worker');
  Log(
      Format('[NEWS_STATS] WORKER_LOCK ACQUIRED Lock=%p Pool=%p Thread=%d WaitMs=%d',
        [Pointer(@FWorkerCS), Pointer(Self), TThread.CurrentThread.ThreadID, TThread.GetTickCount64 - LStartTick]),
      etDebug
  );
  try
    Result.MinWorkers := FMinWorkers;
    Result.MaxWorkers := FMaxWorkers;
    Result.TotalWorkers := FWorkersTotal;
    Result.BusyWorkers := Max(0, FWorkersBusy);
    Result.IdleWorkers := Max(0, FWorkersIdle);
    Result.BaseWorkers := FBaseCount;
    Result.QuotaWorkers := FQuotaCount;
    Result.DynamicWorkers := FDynamicCount;
    Result.BurstWorkers := FWorkersBurst;
  finally
    Log(
        Format('[NEWS_STATS] WORKER_LOCK RELEASING Lock=%p Pool=%p Thread=%d HeldMs=%d',
          [Pointer(@FWorkerCS), Pointer(Self), TThread.CurrentThread.ThreadID, TThread.GetTickCount64 - LStartTick]),
        etDebug
    );
    TLockDiagnostics.CriticalSectionExit(FWorkerCS, 'TCommonThreadPool.Worker');
    Log(
        Format('[NEWS_STATS] WORKER_LOCK RELEASED Lock=%p Pool=%p Thread=%d',
          [Pointer(@FWorkerCS), Pointer(Self), TThread.CurrentThread.ThreadID]),
        etDebug
    );
  end;
end;

procedure TCommonThreadPool._RegisterTaskKind(
    AKind: Integer;
    const AName: string;
    MaxWorkers: Integer;
    RateCapacity, RateRefillPerSec: Int64;
    DefaultPriority: Byte
);
var
  Cfg: TTaskKindConfig;
begin
  Cfg.Name := AName;
  Cfg.MaxWorkers := MaxWorkers;
  Cfg.RateCapacity := RateCapacity;
  Cfg.RateRefillPerSec := RateRefillPerSec;
  Cfg.DefaultPriority := EnsureRange(DefaultPriority, 0, PRIORITY_MAX);

  // store config
  FTaskKinds.AddOrSetValue(AKind, Cfg);

  // apply runtime controls
  SetKindQuota(AKind, MaxWorkers);
  SetRateLimit(AKind, RateCapacity, RateRefillPerSec);
end;

procedure TCommonThreadPool._Stop(Wait: Boolean);
var
  W: TWorker;
begin
  TInterlocked.Exchange(FGlobalStopping, 1);
  if FStopping or FStopped then
    Exit;

  FStopping := True;
  FAcceptingStopped := True;

  // 1. Wake ALL workers
  FQueueEvent.SetEvent;

  // 2. Ask workers to terminate
  TLockDiagnostics.CriticalSectionEnter(FWorkerCS, 'TCommonThreadPool.Worker');
  try
    for W in FWorkers do
      W.Terminate;
    if Length(FBurstWorkers) > 0 then begin
      for W in FBurstWorkers do
        W.Terminate;
    end;
  finally
    TLockDiagnostics.CriticalSectionExit(FWorkerCS, 'TCommonThreadPool.Worker');
  end;

  // 3. Optionally wait
  if Wait then begin
    for W in FWorkers do
      W.WaitFor;
    if Length(FBurstWorkers) > 0 then begin
      for W in FBurstWorkers do
        W.WaitFor;
    end;
  end;

  FStopped := True;
end;

initialization
  //  if not Assigned(TCommonThreadPool.FInstance) then
  //    TCommonThreadPool.FInstance := TCommonThreadPool.Create(4); // default 4 workers

finalization
  if Assigned(TCommonThreadPool.FInstance) then
    FreeAndNil(TCommonThreadPool.FInstance);

end.
