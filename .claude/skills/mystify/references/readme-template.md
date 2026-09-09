# README template (Karpathy voice)

The target is the register of nanoGPT, llm.c, nanochat: a person who built the
thing explaining it to a peer, in the order the peer needs it. Short sentences.
No marketing adjectives. Every command has been run. Every claim about the code
is checkable by opening the file it names.

Fill the skeleton below. Delete any section that would be padding for this
repo; do not add sections that are not listed here unless the repo genuinely
needs them (a "Data" section for a repo that ships or downloads a dataset, a
"Hardware" section for something that needs a GPU).

---

# <repo name>

One or two sentences: what this is, in plain words, and what you get when you
run it. If there is a number that says how big or how fast, put it here.

<Optional: one image or a 10-line terminal transcript of the thing working.>

## quick start

The shortest path from clone to a visible result. One code block, no branching
prose. If a step needs credentials or data the reader does not have, say so on
the line before the command and show what the offline/demo path gives instead.

```bash
git clone <url>
cd <repo>
<install>
<one command that produces output>
```

Then two or three sentences on what just happened and what to look at.

## what this is not

Honest scope. The things a reader might assume this does and it does not.
The environments it has not been run in. The parts that are stubs or doors
rather than features. This section earns the reader's trust for the rest.

## how it works

The mental model in one paragraph, then the code walk. List the files in the
order a reader should open them, with one line each saying what the file owns
and why it exists. Aim for the five to ten files that carry the design; do not
list every module.

```
path/to/first.py     the entry point; parses args and calls <x>
path/to/second.py    <the core idea>; read this one slowly
...
```

Call out the one or two non-obvious decisions (a data layout, an invariant, a
place where the simple approach was tried and failed) and where in the code
they live.

## configuration

The knobs a reader will actually turn: env vars, flags, the config file. A
table with name, default, and one line on what it changes. Skip internal
tunables nobody outside the project needs.

## tests

The command, how long it takes, and what a failure usually means. If some
tests need credentials or data, say which and how they skip.

## layout

The directory tree, annotated, no deeper than two levels. This is the map for
the reader who wants to go somewhere the walkthrough did not.

## status / todos

What works, what is rough, what is next, as a short list. Dated if the repo
changes fast. A reader should be able to tell whether to build on this today.

## license

One line.

---

Voice checklist before you ship it:
- Would the author say this sentence out loud to a colleague? If not, rewrite.
- Every command block was copy-pasted from a fresh-clone run.
- No word like "powerful", "seamless", "robust", "leverage", "cutting-edge".
- No section is there because READMEs usually have one.
- A stranger can find the most important file in under a minute.
