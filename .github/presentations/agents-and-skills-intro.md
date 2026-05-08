---
marp: true
theme: default
paginate: true
style: |
  section {
    font-size: 1.35rem;
    font-family: 'Segoe UI', sans-serif;
  }
  section.lead h1 {
    font-size: 2.6rem;
    color: #0066cc;
  }
  section.lead h2 {
    font-size: 1.5rem;
    color: #555;
    font-weight: normal;
  }
  h1 { color: #0066cc; border-bottom: 2px solid #0066cc; padding-bottom: 0.2em; }
  h2 { color: #333; }
  table { font-size: 1.1rem; }
  pre, code { font-size: 0.85rem; background: #f4f4f4; }
  .columns { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  blockquote { border-left: 4px solid #0066cc; color: #444; font-style: italic; }
  section.tip { background: #e8f4fd; }
  section.warn { background: #fff8e1; }
---

<!-- _class: lead -->

# AI Agents & Skills in VS Code
## From "chat" to autonomous coding partner

*Introduction for engineering teams*

> *Research → Plan → Implement — guided by tests and skills*

---

# What Changed?

## Chat assistants answer questions. **Agents take actions.**

| Chat assistant | Coding Agent |
|---|---|
| Generates code snippets | Reads your codebase, then edits files |
| You paste into your editor | Runs terminals, installs packages, runs tests |
| One-shot response | Multi-step loop until the task is done |
| You supply all context | Searches for context autonomously |

**Key insight**: an agent is a loop — it observes, reasons, acts, observes again.

<!--
Analogy: chat is like asking a question at a help desk.
An agent is like handing a task to a junior colleague: they go figure it out.
-->

---

# The Agent Loop

## Every agent task follows the same three-phase cycle

```
┌─────────────────────────────────────────────────┐
│                                                 │
│   RESEARCH  ──►  PLAN  ──►  IMPLEMENT           │
│       ▲                        │                │
│       └────────────────────────┘                │
│            (verify & iterate)                   │
│                                                 │
└─────────────────────────────────────────────────┘
```

- **Research**: gather enough context to act confidently
- **Plan**: break the work into verifiable steps
- **Implement**: make changes, run commands, validate

> *"Don't guess — search. Don't assume — read. Then act."*

---

# Phase 1: Research

## The agent explores before it touches anything

What a good agent does:
- Searches the codebase for relevant patterns (`semantic_search`, `grep_search`)
- Reads files to understand existing conventions (`read_file`)
- Checks error messages and test output before editing
- Asks clarifying questions only when genuinely blocked

What you should do:
- Point the agent at the right starting files or directories
- Mention relevant function names, class names, or patterns
- Load the right **skill** to give domain context immediately

<!--
The research phase is where most time is spent on complex tasks.
The agent needs to understand: what exists? what pattern is used? what would break?
Skills short-circuit this by packaging that knowledge upfront.
-->

---

# Phase 1: Research — Tips

## Help the agent research efficiently

```
❌ "Add error handling to the database layer."

✅ "Add error handling to `db/connection.py`.
   Look at how `db/query.py` currently handles SQLAlchemy errors
   and follow the same pattern."
```

**Be specific about**:
- Which files or modules are in scope
- Existing patterns to follow
- What *not* to change

> *The agent's research quality is directly proportional to the specificity of your prompt.*

---

# Phase 2: Plan

## Break complex work into verifiable steps

The agent uses a **todo list** to track progress:

```
1. [ ] Read existing parser tests to understand expected behaviour
2. [ ] Search for all callers of parse_input() in the codebase
3. [ ] Add new column type to _COLUMN_DTYPE_MAP
4. [ ] Update parse_input() to handle new column
5. [ ] Run tests to verify no regressions
6. [ ] Update docstring
```

**Why this matters for you**:
- You can see *what* the agent is doing and *why*
- You can interrupt and redirect at any step
- Plans make large refactors auditable

---

# Phase 2: Plan — Prompting for a Plan

## Ask the agent to plan before implementing

```
"Before making any changes, show me your plan as a numbered
 list of steps. Wait for my approval before proceeding."
```

Or use an iterative workflow:
1. Agent proposes plan in chat
2. You approve, adjust, or add constraints
3. Agent implements step-by-step, marking each done
4. You review after each milestone

> *Planning prevents wasted implementation — especially for changes that touch many files.*

---

# Phase 3: Implement

## The agent acts — and validates

A good implementation cycle:
1. **Edit** files (never guessing — based on prior research)
2. **Run tests** to verify the change
3. **Fix** any failures before moving on
4. **Check** for compile/lint errors
5. **Report** what changed and why

**The agent should stop if**:
- Tests are failing and it can't determine why
- It needs to delete files or do something irreversible
- The task scope is growing unexpectedly

---

# Phase 3: Implement — Staying in Control

## You always have the final word

| Situation | Agent behaviour |
|---|---|
| Edit a file | Does it freely — reversible |
| Run tests | Does it freely — read-only |
| Delete a file | **Asks for confirmation** |
| `git push` or `git reset --hard` | **Asks for confirmation** |
| Modify shared infrastructure | **Asks for confirmation** |

**Tip**: Review the diff in the VS Code editor before accepting — use `Ctrl+Z` or git to undo.

<!--
The agent follows "local reversible actions freely; hard-to-reverse or shared-system actions — ask first."
-->

---

# Context Quality is Everything

## The agent is only as good as the context it has

```
Low context  →  hallucination, wrong patterns, broken conventions
High context →  correct, idiomatic, consistent code
```

**Three ways to provide context**:

| Method | When to use |
|---|---|
| In your prompt | Small, specific, one-off facts |
| `copilot-instructions.md` | Project-wide conventions (always loaded) |
| **Skills** | Domain knowledge for *specific* tasks |

---

# Skills: Just-in-Time Context

## Package expert knowledge and load it on demand

A **skill** is a markdown file that:
- Contains domain-specific patterns, conventions, and examples
- Is **loaded only when the agent needs it** (based on its description)
- Can bundle code templates, checklists, or reference tables

```
.github/skills/
  calib-postpro/
    SKILL.md        ← the knowledge document
    templates/      ← optional supporting assets
```

> *Think of a skill as a specialist's cheat sheet — not always needed, but invaluable when it is.*

---

# Why Skills Beat Always-On Instructions

## Context windows have limits — use them wisely

```
copilot-instructions.md   → loaded on EVERY request
                            (burns context even when irrelevant)

skill SKILL.md            → loaded ONLY when the description matches
                            (targeted, efficient, powerful)
```

**Rule of thumb**:
- Applies to most work → `copilot-instructions.md`
- Applies to a specific workflow → **Skill**
- Applies to specific file types → `*.instructions.md` with `applyTo`

---

# Anatomy of a SKILL.md

## Structure of an effective skill file

```markdown
---
name: my-skill
description: "Use when working on X. Covers Y and Z patterns.
              DO NOT use for general Python questions."
---

# My Skill

## When to Apply This Skill
(trigger conditions, workflow context)

## Key Conventions
(the most important patterns — bullet points)

## Common Pitfalls
(what goes wrong without this skill)

## Examples
(short, concrete code snippets)
```

**The `description` field is the discovery surface** — the agent reads it to decide whether to load the skill.

---

# Skills vs Instructions vs Agents

## Choosing the right customization primitive

| Need | Use |
|---|---|
| Always-on project context | `copilot-instructions.md` |
| File-type-specific rules | `*.instructions.md` with `applyTo` |
| Specific-task knowledge | **Skill** (`SKILL.md`) |
| Focused task with inputs | Prompt (`.prompt.md`) |
| Context isolation / multi-stage | Custom Agent (`.agent.md`) |
| Enforce behaviour deterministically | Hook (`.json`) |

**Key test**: Does this apply to *most* work, or *specific* tasks?
- Most → Instructions
- Specific → Skill

---

# Good Tests Guide the Agent to Success

## Tests are the agent's compass

Without tests, the agent:
- Can't tell if its changes broke something
- Stops at "looks right" rather than "demonstrably correct"
- May silently regress existing behavior

With tests, the agent:
- Gets immediate feedback on each change
- Knows exactly what success looks like
- Can iterate confidently without your supervision

> *"Write tests first, then ask the agent to make them pass."*

---

# Tests as Specification

## A test tells the agent exactly what to build

```python
# BAD prompt:
# "Add a function that parses DSM2 date strings."

# BETTER: write the test first, then prompt:
def test_parse_military_date():
    assert parse_military_date("01JAN2020 0000") == pd.Timestamp("2020-01-01")
    assert parse_military_date("31DEC2019 2400") == pd.Timestamp("2020-01-01")
    assert parse_military_date("15JUL2021 1200") == pd.Timestamp("2021-07-15 12:00")
```

**Prompt**: *"Make this test pass. The function goes in `pydsm/analysis/dsm2study.py`."*

The test eliminates ambiguity — the agent knows exactly what the function must do.

---

# The TDD Loop with Agents

## Write test → Agent implements → Verify → Repeat

```
You write a failing test
        │
        ▼
Agent reads test, researches codebase, implements
        │
        ▼
Agent runs test → passes? ──YES──► next feature
        │
       NO
        │
        ▼
Agent diagnoses failure, fixes implementation
        │
        ▼
  (repeat until green)
```

**Tip**: keep tests small and focused — one behaviour per test makes failures easy to diagnose.

---

# What Makes a Good Test (for Agents)

## Properties that help the agent succeed

| Property | Why it matters for agents |
|---|---|
| **Descriptive name** | Agent understands intent without reading code |
| **One assertion per test** | Failure is immediately diagnosable |
| **Uses real data fixtures** | Agent sees realistic inputs, not just toy examples |
| **Tests behavior, not implementation** | Survives refactoring |
| **Covers edge cases explicitly** | Agent won't invent its own assumptions |

```python
# Good: behavior-focused, edge case explicit
def test_timewindow_split_handles_spaces_around_dash():
    start, end = split_timewindow("01JAN2020 0000 - 31DEC2020 2400")
    assert start == pd.Timestamp("2020-01-01")
    assert end == pd.Timestamp("2021-01-01")
```

---

# Practical Tips: Prompting

## What works — and what doesn't

```
❌ "Fix the bug."
✅ "Fix the KeyError in calib_run.py line 247. The error message is:
   KeyError: 'RSAC075'. Look at how other stations are looked up
   and apply the same defensive pattern."

❌ "Refactor the whole module to be cleaner."
✅ "Extract the metric calculation (lines 180–220) into a separate
   function `compute_metrics()`. Don't change any other behavior."

❌ "Add logging everywhere."
✅ "Add a single debug log line at the start of `run_study()` that
   shows the variation name and number of channel groups."
```

> *Specific scope + existing pattern reference + success criterion = best results.*

---

# Practical Tips: Working with the Agent

## Habits that improve outcomes

- **Start with context**: mention which files matter, what pattern to follow
- **Use skills**: if your domain has special conventions, put them in a `SKILL.md`
- **Keep commits small**: easier to review; agent can re-run tests per commit
- **Read the plan**: before a big change, ask for a plan first
- **Trust but verify**: always review diffs; don't auto-accept on large changes
- **Interrupt early**: if the agent goes off-track, redirect immediately — don't wait

**Anti-patterns to avoid**:
- Asking for huge multi-module refactors in one go
- Providing no context and expecting magic
- Accepting changes without running tests locally

---

# Putting It All Together

## A complete example workflow

**Task**: *"Add support for a new EC metric `kge2` to the calibrator."*

1. **Skill loaded**: `calib-postpro` → agent knows metric registry, YAML schema
2. **Research**: agent reads `calib_run.py` metric registry, existing `kge` implementation
3. **Plan**: agent proposes 4 steps (test → impl → YAML schema → docs)
4. **Test first**: agent writes `test_compute_kge2_metric()` (fails)
5. **Implement**: agent adds `kge2` to metric registry, matching existing pattern
6. **Verify**: runs tests → green
7. **Update**: adds `kge2` to YAML reference in `copilot-instructions.md`

Total human intervention: prompt + plan approval + diff review.

---

# Key Takeaways

## Three things to remember

1. **Research → Plan → Implement** — every agent task should follow this cycle.
   Don't skip research; don't skip validation.

2. **Skills = just-in-time context** — package domain knowledge in `SKILL.md` files.
   Loaded when needed, not always.

3. **Tests are the agent's compass** — write failing tests first.
   A green test suite is the clearest definition of "done".

> *The agent is a force multiplier — you still need to understand the problem,
> but the agent handles the mechanical work of implementation.*

---

<!-- _class: lead -->

# Resources & Next Steps

| Resource | Location |
|---|---|
| VS Code Copilot docs | docs.github.com/copilot |
| Agent customization guide | `.github/skills/agent-customization/` |
| Calib postpro skill | `.github/skills/calib-postpro/SKILL.md` |
| Marp for VS Code | Marketplace: `marp-team.marp-vscode` |

**Try it now**:
1. Open a failing test in your project
2. Ask the agent: *"Make this test pass."*
3. Watch the Research → Plan → Implement cycle in action

*Questions?*
