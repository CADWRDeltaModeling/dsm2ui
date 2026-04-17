---
description: "Use when running terminal commands in dsm2ui. Always use Command Prompt (cmd) and always start by activating the dsm2ui conda environment before executing any project commands."
name: "dsm2ui Terminal And Conda Activation"
---
# Terminal And Environment Rules

- Use Command Prompt (`cmd`) for terminal commands in this repository.
- Before any project command, run `conda activate dsm2ui` first.
- Do not run project commands before environment activation.
- If the environment is unavailable, stop and ask for the correct environment setup instead of continuing in a different shell.

## Standard Command Sequence

```bat
conda activate dsm2ui
<project command here>
```

## Examples

```bat
conda activate dsm2ui
pytest tests/
```

```bat
conda activate dsm2ui
python -m dsm2ui.cli calib --help
```
