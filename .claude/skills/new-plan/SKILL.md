---
name: new-plan
description: Add a new phase plan to the development roadmap
tags: [workflow, planning]
---

# New Plan Skill

You are helping the user design and document a new phase for the database development roadmap.

## Context to Gather First

Before writing anything, collect the context you need:

1. Read `doc/plan/README.md` to understand the existing phases, current phase naming convention (A, B, C … G, G2, G4, H, I, J, K …), and the next available phase letter/number.
2. Scan `doc/plan/` (excluding `completed/`) to see any in-progress or planned phases that the new phase might depend on or follow.
3. Read one or two recent phase files to internalize the expected document structure and level of detail.

## User Input

Use AskUserQuestion to ask the user:
- What feature or improvement should the new phase cover?
- Are there any specific implementation approaches or constraints they have in mind?

Keep the question open-ended — the user may have given a description already in their prompt; if so, skip asking and use that.

## Design Options During Planning

While exploring the codebase and drafting the plan, you may discover that a key implementation decision has multiple viable approaches (e.g. DSL syntax style, storage layout, API shape). When this happens:

**Stop and discuss with the user before writing the plan.**

Use AskUserQuestion to present the options concisely:
- Give each option a short label (e.g. "Option A — set with expressions")
- Include a 1–2 line code or pseudocode example as the option description
- Note the key trade-off for each (pro/con in one sentence)
- Mark your recommendation as "(Recommended)" if you have one

Only proceed to write the plan after the user has chosen an approach. This prevents writing a detailed plan around an approach the user dislikes. If the choice is clear-cut and there is only one reasonable approach, skip asking and document your reasoning in the plan's "Implementation Approach" section instead.

## Determine the Phase Identifier

Pick the next available phase identifier following the existing sequence visible in `doc/plan/README.md`. Use a letter (e.g. `L`) or a numbered suffix (e.g. `G5`) depending on the context of the phase.

## Write the Plan File

Create `doc/plan/phase-<id>-<short-slug>.md` following the structure used in existing phase files:

```markdown
# Phase <ID> — <Title>

One sentence describing the goal of this phase.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| N | X.Y   | Description | — or phase |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Why this phase exists. What problem it solves. How it fits with other phases.

---

## N. Item Title (Track X.Y)

### What Changes
### Background
### Implementation Approach
### Key Files
### Tests
### Implementation Steps (N commits)

#### Step N.1 — ...
**Commit:** ...

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
```

Match the depth and detail of the existing plan files. Include concrete code examples, SQL snippets, or file paths where they clarify the approach. Be specific enough that another developer (or Claude) could implement the phase from the document alone.

## Update the README

Add a row to the phase table in `doc/plan/README.md`:

```markdown
| **<ID>** | <Title> | <item count> | [phase-<id>-<slug>.md](phase-<id>-<slug>.md) | Planned |
```

Insert it after the last existing row in the table.

## Commit and PR

1. Stage and show the diff for review:
   ```bash
   git add doc/plan/
   git diff --cached --stat
   ```
2. Wait for user approval, then commit:
   ```bash
   git commit -m "Add Phase <ID> plan: <title>"
   ```
3. Push and open a PR:
   ```bash
   git push -u origin <branch>
   gh pr create --title "Plan: Phase <ID> — <title>" --body "..."
   ```

The PR body should summarise the phase goal, list the items, and note any dependencies on other phases.

## Important Notes

- Do not implement any code — this skill is for planning only.
- Follow the Git Workflow from CLAUDE.md (small focused commits, review before committing).
- The plan should be detailed enough to hand off to the `next-phase` skill for implementation.
- If the user already created a branch before invoking this skill, use that branch rather than creating a new one.
