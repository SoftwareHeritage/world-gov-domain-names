# AI policy

## Banned LLM assistance for communication

Interpersonal communication must be fully written by humans: issues, pull or merge requests, commit messages, reviews and emails.

If you want to add content written with the assistance of an LLM, add it as an attachment (e.g. in an email) or as additional information (e.g. in an issue).

Translation and rephrasing tools are allowed on what you have written, and you're encouraged to disclose help received when it has been significant.

An "agent" must not perform any action on the project by itself (opening issues, pushing commits, etc.).

## Allowed LLM assistance for writing for code and documentation

You can use an LLM to help you write patches or pull/merge request content.

If a contribution contains LLM-generated content, it must carry an `Assisted-by: <tool>:<model>` trailer. Do not use `Co-authored-by:` to mention the assistance of an LLM.

## You are responsible, not "AI"

You must own and be able to justify every change you propose to code and documentation and be careful to propose targeted commits that are easy for humans to review.
