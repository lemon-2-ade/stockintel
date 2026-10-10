# ADR 0006: Explicit, audited model promotion with MLflow aliases and gates

**Status:** Accepted (Phase 8)

## Context
Phase 7 showed that the best model does not beat a constant base-rate
forecast. The platform still needs a served model to exercise the prediction,
monitoring and dashboard paths, and it must be impossible for a retraining job
to put a model in front of users silently.

## Decision
- One registered model per task (`stockintel-direction-h5`). Stages are MLflow
  **aliases** (`candidate`, `challenger`, `champion`); `archived` is a version
  tag. The inference service serves `models:/<name>@champion` and re-resolves
  the alias periodically, so a promotion is picked up without a redeploy.
- Training registers a version and moves it to `candidate`, nothing more.
  Every other transition is an explicit CLI command with a reason and an
  actor, written to the `model_deployments` table in the same step as the
  registry change.
- Transitions follow a fixed graph (archived versions are never revived).
  Promoting a new champion or challenger archives the previous holder.
- **Gates** are evaluated on every promotion: hard ones (feature-set
  compatibility) can never be overridden; soft ones (a recorded evaluation,
  a log-loss gain over the base-rate forecast of at least 0.001 on both the
  untuned walk-forward folds and the test set) can be overridden with
  `--override-gates`, and the override is stored in the audit row.

## Alternatives considered
- **MLflow stages (`Staging`/`Production`)**: deprecated in MLflow 2.9 in
  favour of aliases.
- **Auto-promote when metrics improve**: convenient, but a metric computed on
  a noisy week can replace a model nobody reviewed. Rejected.
- **Hard gates only**: honest, but no model of this project would ever be
  served, and the downstream system could not be built or tested. The soft
  gate plus a recorded override is the honest middle: the record says the
  model was deployed despite failing the quality gate.

## Consequences
- Deploying the current model needs `--override-gates`; the audit log and the
  version tags show it, and the dashboard (Phase 10) labels predictions as
  experimental.
- The audit row and the registry change commit together; the window where the
  registry has changed but the database commit fails is logged, not prevented
  (MLflow has no transactions to join).
- A gate threshold change is a code change, reviewed like any other.
