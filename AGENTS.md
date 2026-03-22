Scope: these instructions apply to the entire repository.

Project intent:
Keep this repository minimal and easy to evolve. Prefer small, explicit changes that establish a clean foundation for future work.

Working style:
- Keep changes focused and easy to review.
- Prefer simple implementations over speculative abstractions.
- Preserve the existing project structure unless a change clearly justifies new files or directories.
- Update README.md when behavior or setup expectations change.

Files and conventions:
- Use ASCII by default unless a file already requires other characters.
- Match the surrounding style of the codebase.
- Add comments only when they clarify non-obvious logic.
- Avoid introducing new dependencies without a clear need.

Validation:
- Run the smallest relevant check for the change you make.
- If the project gains a formatter or test suite, use the existing tooling rather than adding new tools.

Communication:
- In summaries, lead with what changed, then note validation status and any follow-up worth considering.
