unit MWQ.ThreadPool.TaskKinds.Template;

interface

uses
  System.SysUtils,
  MWQ.ThreadPool.Common;

{ ============================================================================
  TEMPLATE UNIT FOR DEFINING TASK KINDS

  Each application should create its own copy of this unit and define task kinds
  as constants.

  Guidelines:
  1. Use ALL_CAPS naming: TASK_KIND_<YourTaskName>
  2. Assign a stable integer value for each kind.
     Do NOT reuse values.
  3. Leave gaps for future kinds (e.g., 1, 2, 10, 20).
  4. These constants are used with:
     - TMWQThreadPool.EnqueueProc(..., AKind)
     - SetKindQuota(AKind, ...)
     - SetRateLimit(AKind, ...)
     - Metrics collection

  Example:
     TASK_KIND_SAVECACHE = 1;
     TASK_KIND_TRANSLATE   = 2;
============================================================================ }

const
  { ================= Your Application Task Kinds ================= }
  TASK_KIND_NONE = 0;

  { Example Task Kinds }
  TASK_KIND_SAVECACHE = 1; // Save cache data
  TASK_KIND_TRANSLATE = 10; // Translate text or news

{ Future examples }
// TASK_KIND_NEWS_FETCH   = 20;
// TASK_KIND_SEND_EMAILS  = 30;
// TASK_KIND_PERSISTENCE  = 40;

procedure InitThreadPool;

implementation

{ ================= Configure quotas & rate limits ================= }

/// Register once at startup
procedure InitThreadPool;
begin
  TMWQThreadPool.RegisterTaskKind(TASK_KIND_NONE, 'NONE', 0, 0, 0); // No rate limit, no quotas
  TMWQThreadPool.RegisterTaskKind(TASK_KIND_SAVECACHE, 'Save cache', 2, 2, 2);
  TMWQThreadPool.RegisterTaskKind(TASK_KIND_TRANSLATE, 'Translate', 4, 4, 2);
end;

end.
