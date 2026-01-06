# MWQ_ThreadPool

**MWQ_ThreadPool** is a high-performance, production-oriented thread pool implementation for **Delphi / Object Pascal**, designed for **server-side**, **data-pipeline**, and **event-driven** workloads.

It focuses on **fair scheduling**, **quota control**, **rate limiting**, and **robust task lifecycle management**, making it suitable for systems such as:

---

## ✨ Key Features

### 🧵 Thread Pool Architecture
- Base workers + burst workers
- Dynamic worker scaling
- Idle / active worker tracking
- Safe shutdown support

### 🎯 Task Scheduling
- Multi-priority queues (0–PRIORITY_MAX)
- Runnable queue + quota queue separation
- High-priority burst execution
- Optional task requeue logic

### 🚦 Rate Limiting
- Per-Kind rate control
- Throttle metrics
- Optional **drop-on-throttle** behavior
- Safe requeue when throttled

### 📊 Quota Control (Per Kind)
- Per-Kind concurrency quotas
- Active task tracking
- Dedicated quota workers
- Automatic worker role switching (`wkBase` ⇄ `wkQuota`)

### 🔑 Keyed Execution
- Tasks can be grouped by **Key**
- Prevents concurrent execution of the same key
- Ideal for:
  - Per-symbol
  - Per-news
  - Per-entity serialization

### ⏱ Deadline & Retry
- Optional task deadlines
- Retry with exponential backoff
- Cancel / expire handling
- Full lifecycle hooks

### 📈 Metrics & Debugging
- Enqueued / Started / Completed / Failed
- Throttled / QuotaDenied / Expired
- Queue depth & execution time
- Rich `{$IFDEF DEBUG}` logging

---

## 🧩 Core Concepts

### Task Kind
Each task belongs to a **Kind** (integer):
```pascal
Task.Kind := TASK_KIND_TRANSLATE;
```

### Kinds

Kinds are used for:

- Rate limiting

- Quota enforcement

- Metrics aggregation

### Task Key

Each task has a Key (UIntPtr) used to serialize execution:
```pascal
Task.Key := UIntPtr();
```

Tasks with the same key:

Will not execute concurrently

Are safely queued and resumed

### Worker Kinds

- wkBase – normal runnable tasks

- wkQuota – quota-controlled tasks

- wkBurst – temporary high-priority workers

Workers may switch roles dynamically depending on workload.

## 🚀 Example Usage
```pascal
const
  { ================= Your Application Task Kinds ================= }
  TASK_KIND_NONE = 0;

  { Example Task Kinds }
  TASK_KIND_SAVECACHE = 1; // Save cache data
  TASK_KIND_TRANSLATE = 10; // Translate text or news

procedure InitThreadPool;

implementation

{ ================= Configure quotas & rate limits ================= }

/// Register once at startup
procedure InitThreadPool;
begin
  TMWQThreadPool.RegisterTaskKind(TASK_KIND_NONE, 'NONE', 0, 0, 0); // No rate limit, no quotas
  TMWQThreadPool.RegisterTaskKind(
      TASK_KIND_SAVECACHE, // AKind: Integer;
      'Save cache', //  const AName: string;
      2, // MaxWorkers: Integer;
      2, // RateCapacity: Int64;
      2 //  RateRefillPerSec: Int64
  );
  TMWQThreadPool.RegisterTaskKind(TASK_KIND_TRANSLATE, 'Translate', 4, 4, 2);
end;


procedure TExampleUse.EnqueueSaveCacheJob(const Job: TSaveCacheJob);
var
  Key: string;
  Owner: UIntPtr;
begin
  Key := MakeJobKey(Job);
  Owner := UIntPtr(Job.RequestOwner);

  FJobsLock.BeginWrite;
  try
    if FActiveJobs.ContainsKey(Key) then
      Exit;
    FActiveJobs.Add(Key, GetTickCount64);
    Inc(FJobsSubmitted);
  finally
    FJobsLock.EndWrite;
  end;

  TMWQThreadPool.EnqueueProc(
      function: Boolean
      begin
        Result := false;
        try
          try
            Inc(FJobsStarted);           
            Job.BeginWrite;
            try
              if TCacheSaver.Save(Job.Data) then begin
                //              Sleep(1000);
                //              if True then begin
                Inc(FJobsCompleted);
                if Assigned(FOnCacheSaved) then
                  DoOnCacheSaved(Job);
                Result := true;
              end
              else
                Inc(FJobsFailed);
            finally
              Job.EndWrite;
            end;

          except
            Inc(FJobsFailed);
            raise;
          end;
        finally
          FJobsLock.BeginWrite;
          try
            FActiveJobs.Remove(Key);
          finally
            FJobsLock.EndWrite;
          end;
        end;
      end,
      0, // APriority
      TASK_KIND_SAVECACHE, // AKind
      Owner, // ATaskOwner: UIntPtr
      UIntPtr(Key), // AKey: UIntPtr
      0, // ADeadlineTick: UInt64, 0 = never expires, Example: GetTickCount64 + 120000 → task must start within 2 minutes
      True, // ACanRetry
      nil // Context object for OnTaskFinished Event.
  );
end;

```

⚠️ Warning
- DropTaskOnThrottle
When enabled, tasks exceeding rate limits will be dropped permanently.

## 🧪 Debug Logging

## 🛠 Requirements

Delphi (tested with modern Delphi versions)

Windows (uses WinAPI threading primitives)


## 📂 Project Status

This project is:

Actively developed

Open for improvements and refactoring

## 🤝 Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

**Additional Resources:**

## 📜 License

This project is licensed under the MIT - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments
- All contributors to this project

## 📞 Contact

- **Author:** 
- **GitHub:** [https://github.com/tixset/ImageWriter](https://github.com/wqmeng/MWQ_ThreadPool)

## 🔗 Links

- 

---

**Made with ❤️ for the open-source community**
