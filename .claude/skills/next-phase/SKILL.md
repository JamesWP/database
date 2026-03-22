---
name: next-phase
description: Complete the next phase from the development roadmap
tags: [workflow, planning]
---

# Next Phase Skill

You are helping the user complete a phase from the database development roadmap.

## Available Phases

First, scan the `doc/plan/` directory to find available phase files (excluding the `completed/` subdirectory).

## User Selection

Present the available phases to the user with AskUserQuestion and ask them to select which phase to work on. Include:
- Phase name (extracted from filename)
- Current status (from doc/plan/README.md if available)
- Brief description (first line or summary from the phase file)

provide a recomended next step.

## Instructions

1. List available phase files from `doc/plan/` (exclude `completed/` folder)
2. Read each phase file to get a brief description
3. Use AskUserQuestion to present the options and let them select
4. Once selected, create a new git branch with an appropriate name
5. Begin implementing the selected phase following the template above
6. Follow TDD practices per CLAUDE.md
7. Make small, focused commits for each item in the phase
8. Run tests after each change to ensure nothing breaks

## Important Notes

- Always check `doc/plan/README.md` for the current status of phases
- Phases in `doc/plan/completed/` should not be suggested
- Follow the Git Workflow from CLAUDE.md
- Use TaskCreate to track progress through multi-step phases
- Run `cargo test` before and after changes

## Stub / Partial Implementation Tracking

Some items are intentionally implemented partially — e.g. a constraint is parsed but not enforced, a type is accepted but coercion is deferred, a feature is recognised but returns a placeholder. These are **stubs**.

At the end of the phase (not per-commit), for every stub introduced:

1. Add a `// TODO(phase-<id>): <description>` comment at the stub site in the code.
2. Add or update a **Stubbed Features** section in `doc/plan/README.md` listing every currently active stub. Use this format:

```markdown
## Stubbed Features

| Feature | Stub behaviour | Tracking |
|---------|---------------|----------|
| FOREIGN KEY constraints | Parsed and silently ignored | TODO phase-av |
| CHECK constraints | Parsed and silently ignored | TODO phase-av |
| NOT NULL on DEFAULT-less omitted columns | Treated as NULL (permissive) | TODO phase-aj |
```

Remove a row from the table when the stub is fully implemented. The table gives users and future Claude sessions a quick picture of current limitations.

## Completion Steps

After implementing all items in the phase:

1. Add/update the **Stubbed Features** table in `doc/plan/README.md` as described above.
2. Move the phase file to `doc/plan/completed/`
3. Update the phase row in `doc/plan/README.md` to mark it as Completed.
4. Commit: `git add doc/plan/ && git commit -m "Mark Phase <X> as completed"`
5. Push the branch: `git push -u origin <branch>`
6. Create a PR: `gh pr create --title "Phase <X>: <title>" --body "..."`
