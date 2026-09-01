# Example skill (copy me into graph/skills/)

A one-line description goes here: it shows under the skill's name in
the Ask picker.

A skill is YOUR briefing to the agent — how your team reads the data.
Copy this file to `<your graph dir>/skills/<name>.md`, rewrite it in
your own words, and it appears in Ask's ⊕ skills picker. Loading it
puts this text into the agent's context for that session.

Things a skill is good at saying:

- which metrics your team treats as the default ("when someone says
  spend without qualification, they mean the certified acquirer net
  spend")
- calendar habits ("quarters are fiscal, ending March/June/Sep/Dec")
- naming habits ("SMB means the small-business segment, never the
  bank")
- what to avoid ("we never report the mined variant of approval rate
  to leadership")

Things a skill can NOT do, by design: it cannot add tables, metrics,
or numbers to the world. The agent's tools serve only the compiled
build, and the verifier checks every claim against it — a skill
steers where the agent looks first, nothing more.
