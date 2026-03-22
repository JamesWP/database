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

### Extend vs. Create New

Whenever the plan requires adding capability to the codebase — a new plan node, VM instruction, register type, storage helper, submodule, etc. — consider whether to extend something existing or introduce something new. The right choice depends on the complexity vs. duplication trade-off:

**Prefer extending when:**
- The new behaviour shares the same core logic and only differs at one step (e.g. same scan loop, different yield).
- The variation is naturally expressed as a small enum (2–3 named variants) or a single field — not a boolean flag where `true` and `false` have unclear meaning at the call site.
- Extending requires only mechanical, low-risk changes to existing constructor sites.

**Prefer creating new when:**
- The structure is genuinely different: different children, fields, or lifecycle — not just a mode switch.
- Extending would require multiple co-dependent fields that create invalid-state combinations.
- The new case would need meaningfully different logic in most match arms anyway, making a new variant more explicit than a hidden branch inside an existing one.
- The existing type is already complex; adding more to it makes it harder to reason about.

This applies across the codebase at every level: plan nodes, VM instructions, register/scalar types, storage helpers, engine modules, submodules. In each case, ask: *is this genuinely the same thing with a mode, or is it a new thing that happens to share some code?*

When the answer is unclear, present both options to the user with a short before/after sketch and the key trade-off.

## Determine the Phase Identifier

Pick the next available phase identifier following the existing sequence visible in `doc/plan/README.md`. Use a letter (e.g. `L`) or a numbered suffix (e.g. `G5`) depending on the context of the phase.

## Stubs and Partial Implementations

When a planned item intentionally implements something only partially — e.g. a constraint is parsed but not enforced, a syntax is accepted but ignored, a feature returns a placeholder — call it out explicitly in the plan document.

Each phase file must include a **Stubs** section (between the Overview and the per-item sections) listing every stub the phase introduces. This makes stubs visible and reviewable at planning time, before any code is written.

```markdown
## Stubs

| Stub | Behaviour | TODO marker | Completed by |
|------|-----------|-------------|--------------|
| FOREIGN KEY | Parsed and silently ignored | `TODO(phase-aj): enforce FK` | Phase AV |
| NOT NULL + missing default | Treated as NULL (permissive) | `TODO(phase-aj): reject if NOT NULL` | Phase AJ (later item) |
```

When `next-phase` implements the plan, it:
1. Places the `// TODO(phase-<id>): ...` comment at the stub site in code.
2. Adds the stub row to the **Stubbed Features** table in `doc/plan/README.md`.

If the phase introduces no stubs, include `## Stubs\n\nNone.` to make the absence explicit.

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

## Stubs

| Stub | Behaviour | TODO marker | Completed by |
|------|-----------|-------------|--------------|
| Example | Parsed and ignored | `TODO(phase-<id>): ...` | Phase XX |

_(Remove this table and replace with "None." if the phase introduces no stubs.)_

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
