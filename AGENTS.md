# Repository Instructions

## Normative sources

Before implementing or reviewing any milestone, read these files in full:

1. `docs/clarifications.md`
2. `docs/requirements.md`
3. `docs/data_contracts.md`
4. `docs/expected_run_metrics.yml`

Explicit clarifications take precedence over every other project document.
Requirements take precedence over technical contracts and metrics. Contracts
and metrics must remain consistent with requirements and clarifications.

Read `README.md` as mandatory descriptive and operational context. It is not a
normative source and never takes precedence over the four documents above.

Stop and report any normative ambiguity or conflict before changing code. Never
infer new requirements from the existing implementation.

## Git safety

- Never work directly on main.
- Before modifying files, run git branch --show-current and git status --short.
- If the current branch is main, stop and ask for a dedicated branch.
- Preserve all pre-existing user changes.
- Never run git add, commit, push, merge, rebase, reset, checkout, restore, clean, or force operations unless explicitly requested.
- Never modify Git history.
- Never change the tracking or ignore status of this AGENTS.md file unless the
  user explicitly authorizes that exact operation.

## Scope control

- Read all normative sources listed above before making changes.
- Inspect the existing repository structure before creating files.
- Modify only files required by the current task.
- Do not implement features belonging to future milestones.
- Do not invent requirements when documentation is incomplete.
- If requirements or contracts materially contradict each other, stop and request clarification.
- Do not overwrite or remove unrelated files or changes.
- Avoid destructive commands and recursive deletion.

## Engineering standards

- Prefer simple, maintainable solutions over unnecessary abstractions.
- Do not add dependencies unless technically necessary and explicitly justified.
- Keep code, identifiers, commit proposals, and technical names in English.
- Keep repository documentation in Italian unless the existing file uses another language.
- Never hardcode personal paths, credentials, secrets, tokens, or machine-specific values.
- Use deterministic behavior whenever reproducibility is required.
- Keep generated files, caches, databases, logs, temporary artifacts, and credentials out of Git unless explicitly required.
- Never track generated CSV files, staging, backup, recovery or lock artifacts.
- Keep the repository anonymized and suitable for future public release.

## Verification

- Run all relevant tests and validations after changes.
- Before every commit, run the minimum pertinent checks for the milestone.
- Run git diff --check before finishing.
- Run git status --short before finishing.
- Inspect the complete diff, including newly created files.
- Remove only temporary artifacts created during the current task.
- Do not claim that a check passed unless it was actually executed successfully.
- If a required check cannot be executed, state the exact reason.

## Final response

Always report:
- what was implemented;
- files created or modified;
- tests and validations executed with their results;
- assumptions and unresolved issues;
- complete output of git status --short;
- one proposed Conventional Commits message in English;
- explicit confirmation that no commit or push was performed.

Do not commit or push unless the user explicitly authorizes it.
