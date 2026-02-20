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

## Completion Steps

After implementing all items in the phase:

1. Move the phase file to `doc/plan/completed/`
2. Update `doc/plan/README.md` to mark the phase as Completed
3. Commit: `git add doc/plan/ && git commit -m "Mark Phase <X> as completed"`
4. Push the branch: `git push -u origin <branch>`
5. Create a PR: `gh pr create --title "Phase <X>: <title>" --body "..."`
6. Discard any accidental formatting-only changes: `git restore <file>`

## Proptest Guidelines

When adding proptests, keep test suites fast:
- Use `#![proptest_config(ProptestConfig::with_cases(N))]` inside `proptest! {}` blocks to cap case counts
- Large/slow tests (e.g. 200+ B-tree inserts): max 5 cases
- Medium tests (compiler, parser): max 50 cases
- Put slow tests in their own `proptest! {}` block so the config only applies to them
- Verify the new tests complete in reasonable time before committing
