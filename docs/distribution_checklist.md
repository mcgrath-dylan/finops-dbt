# Blog Post #1 - Distribution Checklist

Post: `content/blog/2026-04-xx-snowflake-finops-30pct-savings.md`

## Pre-publish

- [ ] Swap TODO screenshots if real-data versions are ready
- [ ] Verify the "30%" number against current demo seed projection
- [ ] Proofread once cold (next day, not same day)
- [ ] Confirm all repo links resolve
- [ ] Confirm images load on raw.githubusercontent.com URLs

## Pre-launch - AI disclosure draft

Before launch, draft the "How this was built" paragraph for the blog post. Run in a fresh Claude Code session. Claude asks the five questions below, Dylan answers, Claude drafts 80-120 words, Dylan rewrites for voice.

**Interview questions:**

1. Roughly how long from first finops-dbt commit to v3.0.0 tag? Rough is fine.
2. Which AI tools did you use across the project lifetime and in roughly what split? Example: ChatGPT early, Codex mid, Claude Code + Codex for v3.0.0.
3. What's one thing AI accelerated that would have been tedious solo? Test scaffolding, CI wiring, docs, Mermaid diagram, or something else.
4. What's one modeling or methodology decision that was yours, not the AI's? Forecast approach, department showback shape, or a Snowflake-specific modeling call.
5. What's one gotcha you hit that the AI would not have predicted? ACCOUNT_USAGE lag, storage refresh cadence, warehouse billing edge case, or a CI failure mode.

**Target output:** 80-120 words, first person, matter-of-fact, no apology, no overclaiming. The point is to disclose up front, not to apologize for AI use. Visible AI-assisted velocity is a hiring signal, not a liability.

## Launch - dbt Slack + dev.to

Two channels only. Skip r/dataengineering entirely for post #1. Revisit for post #2 after #1 clears the day-14 gate and the hook has been iterated based on dbt Slack / dev.to reactions.

1. [ ] Publish on **dev.to** first with the data-engineering tag. Set `canonical_url` in the markdown front-matter to the dev.to URL.
2. [ ] Post one link in **dbt Slack #show-and-tell** with 1-2 sentences of framing. No thread spam, no DM follow-ups.
3. [ ] Update `cross_posted_to:` front-matter with the dev.to URL.
4. [ ] Log launch date, dev.to URL, and dbt Slack message link in a tracking note.

**Do not cross-post yet.** Medium, LinkedIn, r/dataengineering, and Snowflake Community are gated on the 14-day signal below.

## Day-14 signal gate

At day 14 from launch, evaluate:

- [ ] **Reads:** >=100 on the launch channel
- [ ] **Engagement:** >=1 substantive reaction in dbt Slack #show-and-tell OR >=1 non-trivial comment on the post itself

**If both clear, cross-post in this order:**

1. [ ] Medium - paste as-is, set `canonical_url` to the dev.to URL
2. [ ] LinkedIn - shorter version, first 400 words plus link back to canonical, hashtags `#snowflake #dbt #finops`
3. [ ] r/dataengineering - one post, only after hook iteration based on launch reactions
4. [ ] Snowflake Community forum - Data Engineering section
5. [ ] Update `cross_posted_to:` front-matter with all URLs

**If either fails, do not cross-post.** Rewrite the hook or reconsider the dev.to framing before posting #2 and #3 from the ROADMAP.

## Track

- [ ] Day 3, day 7, day 14, day 30, day 90: log views, stars, inbound messages
- [ ] Day 30: Green/Yellow/Red verdict per ROADMAP kill criteria
- [ ] Day 90: full ROADMAP decision, double down or archive as portfolio piece

## Reframed bet

Primary success metric: recruiter-visible analytics engineering portfolio signal, not consulting inbound. If consulting inquiries arrive organically, handle them, but do not optimize the post or cross-posts for consulting funnel conversion.
