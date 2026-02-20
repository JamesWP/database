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
