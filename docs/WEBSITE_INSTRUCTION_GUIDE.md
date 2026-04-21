# CKC Website Instruction Guide

This guide explains how to use the CKC website end-to-end, with practical prompt examples you can copy and adapt.

## 1) What CKC Website Is For

CKC helps you coordinate task work that may involve multiple agents and multiple stages. Think of the site as a mission dashboard:

- You describe a task.
- The workspace tracks progress and outcomes.
- The interface helps you move from idea -> request -> review -> completion.

This guide focuses on user workflow and prompt writing quality.

## 2) Interface Map

The website experience is centered around these areas:

- Home: quick task entry and launch point.
- Login/Register: account entry.
- Workspace: task tracking and settings.
- Task Detail: execution timeline and status feed.
- Agent Store: available agent options and discovery surface.
- Builder: guided flow for defining custom agent profile fields.

## 3) Before You Start

For the best experience, prepare:

- A clear objective (what result you need).
- A scope boundary (what is out of scope).
- A success definition (what “done” means).
- Any constraints (deadline, tone, format, budget, safety limits).

If you skip these four items, outputs are usually broad and less useful.

## 4) Recommended Task Formula

Use this structure in prompts:

1. Goal
2. Context
3. Constraints
4. Output format
5. Quality bar

Template:

```text
Goal: <what you need>
Context: <background and audience>
Constraints: <time, cost, policy, style, exclusions>
Output format: <bullet list, memo, table, JSON, etc.>
Quality bar: <what makes the result acceptable>
```

## 5) Homepage Prompting

On Home, write a concise first request. Avoid writing a full novel in the first pass.

Good first-pass prompt:

```text
Goal: Draft a one-page launch plan for a campus AI workshop.
Context: Audience is graduate students with mixed technical backgrounds.
Constraints: Keep budget under $300, no paid ads, launch in 2 weeks.
Output format: 3-phase plan + timeline + risk checklist.
Quality bar: Actionable steps, realistic timing, clear owners.
```

Weak first-pass prompt:

```text
Help me plan something for AI.
```

Why weak: no audience, no timeline, no constraints, no completion criteria.

## 6) Register/Login Flow

1. Create account in Register page.
2. Return through Login page for repeat sessions.
3. Confirm you can reach Workspace.

If your team is demoing live, test this flow before presentation day.

## 7) Workspace Basics

Workspace has two core tabs:

- Tasks: active task cards and task history.
- Settings: profile updates and account-level preferences.

Best practice:

- Keep task titles specific.
- Save notable tasks so they remain easy to find.
- Delete low-value test tasks to reduce clutter.

## 8) Writing Better Task Prompts

### A) Research prompt

```text
Goal: Compare three approaches for low-cost user research in education apps.
Context: Team is 2 people, launch window is 1 month.
Constraints: Must be remote-first and under 20 total participant hours.
Output format: Comparison table + recommendation + fallback option.
Quality bar: Practical execution details, not generic theory.
```

### B) Product spec prompt

```text
Goal: Draft a product requirements outline for a feature that tracks assignment progress.
Context: Users are instructors and students.
Constraints: Must support mobile and desktop; no dark patterns.
Output format: Problem, user stories, edge cases, acceptance criteria.
Quality bar: Concrete and testable requirements.
```

### C) Content strategy prompt

```text
Goal: Create a 30-day content plan for project updates.
Context: Audience is technical founders and grad students.
Constraints: 3 posts/week, no paid media, factual tone only.
Output format: Calendar + post angles + CTA ideas.
Quality bar: Consistent voice and measurable outcomes.
```

## 9) Task Detail Page: How to Read Progress

The status feed typically reflects:

- parsing / planning,
- candidate evaluation,
- execution,
- completion summary.

Use the feed to validate that requested constraints are being respected.

If the result drifts from your goal, do not restart blindly. Instead issue a correction prompt.

Correction prompt example:

```text
Revise with these corrections:
1) Keep only B2B use cases.
2) Remove assumptions requiring a paid marketing team.
3) Return the output as a numbered plan with owners and deadlines.
```

## 10) Agent Store Usage

Agent Store helps with discovery and selection context.

When reviewing options:

- prioritize capability fit over novelty,
- check risk/cost alignment,
- keep fallback choices ready.

Prompt to validate fit:

```text
Given this task, rank the top 3 agent profiles by expected fit.
For each, provide strengths, likely failure mode, and when to use a fallback.
```

## 11) Builder Workflow (Guided)

Builder is useful when you want a structured profile for repeated work patterns.

Suggested sequence:

1. Define role and scope.
2. Set risk posture and fallback behavior.
3. Confirm expected output style.
4. Review and publish.

Builder prompt support example:

```text
Create a collaboration profile for "Policy Brief Analyst".
Scope: summarize and compare policy options.
Do not perform legal advice.
Output style: neutral, evidence-based, source-aware.
Fallback: ask clarifying questions when evidence is insufficient.
```

## 12) Prompt Patterns by Use Case

### Planning

```text
Create a phased implementation plan with milestones for 2, 6, and 12 weeks.
Include resource assumptions and top risks.
```

### Decision support

```text
Evaluate options A/B/C with weighted criteria: cost 30%, speed 30%, reliability 40%.
Return ranking plus rationale and confidence level.
```

### Drafting

```text
Draft a concise executive memo (max 400 words) summarizing recommendation and tradeoffs.
```

### QA and critique

```text
Review this draft for logical gaps, unsupported claims, and missing edge cases.
Return edits in priority order.
```

## 13) Common Mistakes and Fixes

Mistake: prompt is too broad.

Fix:

```text
Narrow to one goal, one audience, one output format.
```

Mistake: no success criteria.

Fix:

```text
Add 3 measurable acceptance checks before execution.
```

Mistake: vague timeline.

Fix:

```text
Specify deliverable deadline and intermediate milestones.
```

## 14) Output Review Checklist

Before accepting a result, verify:

- Is the goal answered directly?
- Are constraints respected?
- Is the output format exactly what you requested?
- Are assumptions explicit?
- Is there a clear next action?

If any are missing, ask for a targeted revision rather than rerunning from scratch.

## 15) Prompt Library (Copy-Ready)

### Launch plan

```text
Design a launch plan for a student AI product.
Audience: graduate students and teaching staff.
Constraints: 4 weeks, no paid ads, one part-time operator.
Deliverable: week-by-week plan + KPI targets + risk mitigations.
```

### User interview synthesis

```text
Synthesize 12 user interview notes into themes.
Return: top 5 insights, supporting quotes, and confidence per insight.
Add a section called "Decisions we can make now".
```

### Technical explainer

```text
Explain the workflow to a mixed audience.
Requirements: plain language first, technical appendix second.
Format: 6 bullets + glossary.
```

### Academic assignment support

```text
Create a one-page assignment plan for an AI systems class.
Include milestones, division of labor for 3 team members, and submission checklist.
```

## 16) Team Collaboration Mode

When collaborating with teammates, standardize prompt metadata at the top:

```text
Owner: <name>
Task ID: <id>
Revision: <v1/v2/v3>
Decision deadline: <date>
```

This makes status tracking easier in shared demos and reviews.

## 17) Demo-Day Workflow

For smoother demonstrations:

1. Pre-create 2-3 realistic tasks.
2. Keep one short and one complex prompt ready.
3. Use a correction prompt live to show iterative control.
4. End with a concise summary output.

Demo closing prompt:

```text
Summarize the completed workflow in 5 bullets:
objective, selected approach, tradeoffs, final output, next step.
```

## 18) Safety and Practical Constraints

Use explicit boundaries for sensitive topics.

Boundary prompt pattern:

```text
Do not provide prohibited or unsafe instructions.
If the task is ambiguous or high risk, ask clarifying questions first.
```

## 19) Final Advice

High-quality outcomes usually come from:

- clear task framing,
- constraints written in plain language,
- iterative correction instead of random retries,
- and explicit acceptance criteria.

Use this guide as a reusable playbook, then adapt it to your domain.
