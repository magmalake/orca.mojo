- **orca-stack: deploying an image from before orca 0.18 swaps its UI in.**
  The UI's health check wanted 200 on `/healthz`, which older UIs don't
  have, so their `ui-next` never counted as healthy: the apply failed after
  `HEALTH_TIMEOUT`, kept the old UI and left the pipelines paused. A 404
  there now counts as up too; a check's status can be a list (`200,404`).
